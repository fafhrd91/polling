//! Bindings to kqueue (macOS, iOS, tvOS, watchOS, visionOS, FreeBSD, NetBSD, OpenBSD, DragonFly BSD).

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io;
use std::mem::MaybeUninit;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::time::Instant;

use rustix::buffer::spare_capacity;
use rustix::event::{kqueue, Timespec};
use rustix::io::{fcntl_setfd, Errno, FdFlags};

use crate::{Event, PollMode};

pub use self::notify::Waker;

/// Interface to kqueue.
#[derive(Debug)]
pub struct Poller {
    /// File descriptor for the kqueue instance, shared with the wakers.
    kqueue_fd: Arc<OwnedFd>,

    /// List of sources currently registered in this poller.
    ///
    /// This is used to make sure the same source is not registered twice.
    sources: RefCell<HashSet<SourceId>>,

    /// Notification pipe for waking up the poller.
    ///
    /// On platforms that support `EVFILT_USER`, this uses that to wake up the poller. Otherwise, it
    /// uses a pipe.
    notify: notify::Notify,

    /// Changes queued by `modify_deferred()`, submitted by the next `wait_deadline()`.
    pending: RefCell<PendingChanges>,

    /// Changes being submitted by `wait_deadline()`, kept to reuse the allocation.
    submit: RefCell<Changes>,
}

/// A list of changes for kqueue.
#[derive(Default)]
struct Changes(Vec<kqueue::Event>);

// SAFETY: `udata` of the events is only used as an integer key.
unsafe impl Send for Changes {}

impl std::fmt::Debug for Changes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Changes")
            .field("len", &self.0.len())
            .finish()
    }
}

#[derive(Debug, Default)]
struct PendingChanges {
    changes: Changes,
    /// Index of each fd's Read-then-Write pair in `changes`.
    index: HashMap<RawFd, usize>,
}

impl PendingChanges {
    fn remove(&mut self, fd: RawFd) {
        if let Some(slot) = self.index.remove(&fd) {
            // Removing Write first preserves the moved pair's Read-then-Write order.
            self.changes.0.swap_remove(slot + 1);
            self.changes.0.swap_remove(slot);
            if slot < self.changes.0.len() {
                let kqueue::EventFilter::Read(moved_fd) = self.changes.0[slot].filter() else {
                    unreachable!("pending changes must be Read-then-Write pairs");
                };
                self.index.insert(moved_fd, slot);
            }
        }
    }
}

/// Identifier for a source.
#[doc(hidden)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum SourceId {
    /// Registered file descriptor.
    Fd(RawFd),

    /// Signal.
    Signal(std::os::raw::c_int),

    /// Process ID.
    Pid(rustix::process::Pid),

    /// Timer ID.
    Timer(usize),
}

impl Poller {
    /// Creates a new poller.
    pub fn new() -> io::Result<Poller> {
        // Create a kqueue instance.
        let kqueue_fd = kqueue::kqueue()?;
        fcntl_setfd(&kqueue_fd, FdFlags::CLOEXEC)?;

        let poller = Poller {
            kqueue_fd: Arc::new(kqueue_fd),
            sources: RefCell::new(HashSet::new()),
            notify: notify::Notify::new()?,
            pending: RefCell::new(PendingChanges::default()),
            submit: RefCell::new(Changes::default()),
        };

        // Register the notification pipe.
        poller.notify.register(&poller)?;

        #[cfg(feature = "tracing")]
        tracing::trace!(
            kqueue_fd = ?poller.kqueue_fd.as_raw_fd(),
            "new"
        );
        Ok(poller)
    }

    /// Whether this poller supports level-triggered events.
    pub fn supports_level(&self) -> bool {
        true
    }

    /// Whether this poller supports edge-triggered events.
    pub fn supports_edge(&self) -> bool {
        true
    }

    /// Adds a new file descriptor.
    ///
    /// # Safety
    ///
    /// The file descriptor must be valid and it must last until it is deleted.
    pub unsafe fn add(&self, fd: RawFd, ev: Event, mode: PollMode) -> io::Result<()> {
        self.add_source(SourceId::Fd(fd))?;

        // File descriptors don't need to be added explicitly, so just modify the interest.
        self.modify(BorrowedFd::borrow_raw(fd), ev, mode)
            .map_err(|e| {
                let _ = self.remove_source(SourceId::Fd(fd));
                e
            })
    }

    /// Modifies an existing file descriptor.
    pub fn modify(&self, fd: BorrowedFd<'_>, ev: Event, mode: PollMode) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = if !self.notify.has_fd(fd) {
            let span = tracing::trace_span!(
                "add",
                kqueue_fd = ?self.kqueue_fd.as_raw_fd(),
                ?fd,
                ?ev,
            );
            Some(span)
        } else {
            None
        };
        #[cfg(feature = "tracing")]
        let _enter = span.as_ref().map(|s| s.enter());

        self.has_source(SourceId::Fd(fd.as_raw_fd()))?;

        // Queued changes must not be applied after this one.
        self.pending.borrow_mut().remove(fd.as_raw_fd());

        let mode_flags = mode_to_flags(mode);

        let read_flags = if ev.readable {
            kqueue::EventFlags::ADD | mode_flags
        } else {
            kqueue::EventFlags::DELETE
        };
        let write_flags = if ev.writable {
            kqueue::EventFlags::ADD | mode_flags
        } else {
            kqueue::EventFlags::DELETE
        };

        // A list of changes for kqueue.
        let changelist = [
            kqueue::Event::new(
                kqueue::EventFilter::Read(fd.as_raw_fd()),
                read_flags | kqueue::EventFlags::RECEIPT,
                ev.key as _,
            ),
            kqueue::Event::new(
                kqueue::EventFilter::Write(fd.as_raw_fd()),
                write_flags | kqueue::EventFlags::RECEIPT,
                ev.key as _,
            ),
        ];

        // Apply changes.
        self.submit_changes(changelist)
    }

    /// Queues a modification of an existing file descriptor, it is submitted by the next wait.
    pub fn modify_deferred(&self, fd: BorrowedFd<'_>, ev: Event, mode: PollMode) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "modify_deferred",
            kqueue_fd = ?self.kqueue_fd.as_raw_fd(),
            ?fd,
            ?ev,
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        self.has_source(SourceId::Fd(fd.as_raw_fd()))?;

        // Unwanted filters are disabled instead of deleted. Deleting a filter that does not
        // exist fails, and any failed change makes `kevent` return without waiting.
        let mode_flags = mode_to_flags(mode);
        let flags = |wanted: bool| {
            if wanted {
                kqueue::EventFlags::ADD | kqueue::EventFlags::ENABLE | mode_flags
            } else {
                kqueue::EventFlags::ADD | kqueue::EventFlags::DISABLE | mode_flags
            }
        };

        let changes = [
            kqueue::Event::new(
                kqueue::EventFilter::Read(fd.as_raw_fd()),
                flags(ev.readable),
                ev.key as _,
            ),
            kqueue::Event::new(
                kqueue::EventFilter::Write(fd.as_raw_fd()),
                flags(ev.writable),
                ev.key as _,
            ),
        ];
        let mut pending = self.pending.borrow_mut();
        if let Some(&slot) = pending.index.get(&fd.as_raw_fd()) {
            pending.changes.0[slot..slot + 2].copy_from_slice(&changes);
        } else {
            let slot = pending.changes.0.len();
            pending.changes.0.extend(changes);
            pending.index.insert(fd.as_raw_fd(), slot);
        }
        Ok(())
    }

    /// Submit one or more changes to the kernel queue and check to see if they succeeded.
    pub(crate) fn submit_changes<const N: usize>(
        &self,
        changelist: [kqueue::Event; N],
    ) -> io::Result<()> {
        submit_changes(&self.kqueue_fd, changelist)
    }

    /// Add a source to the sources set.
    #[inline]
    pub(crate) fn add_source(&self, source: SourceId) -> io::Result<()> {
        if self.sources.borrow_mut().insert(source) {
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::AlreadyExists))
        }
    }

    /// Tell if a source is currently inside the set.
    #[inline]
    pub(crate) fn has_source(&self, source: SourceId) -> io::Result<()> {
        if self.sources.borrow().contains(&source) {
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }

    /// Remove a source from the sources set.
    #[inline]
    pub(crate) fn remove_source(&self, source: SourceId) -> io::Result<()> {
        if self.sources.borrow_mut().remove(&source) {
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }

    /// Deletes a file descriptor.
    pub fn delete(&self, fd: BorrowedFd<'_>) -> io::Result<()> {
        // Simply delete interest in the file descriptor.
        self.modify(fd, Event::none(0), PollMode::Oneshot)?;

        self.remove_source(SourceId::Fd(fd.as_raw_fd()))
    }

    /// Waits for I/O events with an optional deadline.
    pub fn wait_deadline(&self, events: &mut Events, deadline: Option<Instant>) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "wait",
            kqueue_fd = ?self.kqueue_fd.as_raw_fd(),
            ?deadline,
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        let timeout = deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));

        // Timeout for kevent. In case of overflow, use no timeout.
        let timeout = match timeout {
            Some(t) => Timespec::try_from(t).ok(),
            None => None,
        };

        // Take the queued changes.
        let mut submit = self.submit.borrow_mut();
        {
            let mut pending = self.pending.borrow_mut();
            std::mem::swap(&mut pending.changes, &mut *submit);
            let used = pending.index.len();
            pending.index.clear();
            if pending.index.capacity() > 1024 && pending.index.capacity() / 4 > used {
                pending.index.shrink_to(used.max(1024));
            }
        }

        // A failed change is reported in the event list. If the list is full, `kevent` fails
        // and stops applying changes, so make room for all of them.
        events.reserve(submit.0.len());

        // Apply the changes and wait for I/O events.
        let res = unsafe {
            kqueue::kevent_timespec(
                &self.kqueue_fd,
                &submit.0,
                spare_capacity(&mut events.list),
                timeout.as_ref(),
            )
        };
        let used = submit.0.len();
        submit.0.clear();
        if submit.0.capacity() > 1024 && submit.0.capacity() / 4 > used {
            submit.0.shrink_to(used.max(1024));
        }
        drop(submit);
        let _res = res?;

        #[cfg(feature = "tracing")]
        tracing::trace!(
            kqueue_fd = ?self.kqueue_fd.as_raw_fd(),
            res = ?_res,
            "new events",
        );

        // Clear the notification (if received) and re-register interest in it.
        self.notify.reregister(self)?;

        Ok(())
    }

    /// Returns a waker that wakes up the current or next `wait()` call.
    pub fn waker(&self) -> Waker {
        self.notify.waker(self)
    }
}

/// Submit one or more changes to the kernel queue and check to see if they succeeded.
fn submit_changes<const N: usize>(
    kqueue_fd: &OwnedFd,
    changelist: [kqueue::Event; N],
) -> io::Result<()> {
    let mut eventlist = [MaybeUninit::<kqueue::Event>::uninit(); N];

    // Apply changes.
    let (eventlist, _) =
        unsafe { kqueue::kevent_timespec(kqueue_fd, &changelist, &mut eventlist, None)? };

    // Check for errors.
    for &ev in &*eventlist {
        let data = ev.data();

        // Explanation for ignoring EPIPE: https://github.com/tokio-rs/mio/issues/582
        if (ev.flags().contains(kqueue::EventFlags::ERROR))
            && data != 0
            && data != Errno::NOENT.raw_os_error() as _
            && data != Errno::PIPE.raw_os_error() as _
        {
            return Err(io::Error::from_raw_os_error(data as _));
        }
    }

    Ok(())
}

impl AsRawFd for Poller {
    fn as_raw_fd(&self) -> RawFd {
        self.kqueue_fd.as_raw_fd()
    }
}

impl AsFd for Poller {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.kqueue_fd.as_fd()
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "drop",
            kqueue_fd = ?self.kqueue_fd.as_raw_fd(),
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        let _ = self.notify.deregister(self);
    }
}

/// A list of reported I/O events.
pub struct Events {
    list: Vec<kqueue::Event>,
    /// Number of new events a single wait can receive.
    batch: usize,
}

unsafe impl Send for Events {}

impl Events {
    /// Creates an empty list.
    pub fn with_capacity(cap: usize) -> Events {
        Events {
            list: Vec::with_capacity(cap),
            batch: cap,
        }
    }

    /// Makes room for at least `additional` new events, events are appended to the list.
    fn reserve(&mut self, additional: usize) {
        let additional = additional.max(self.batch);
        let needed = self.list.len().saturating_add(additional).max(1024);
        // Reclaim room for an earlier change burst only when demand has subsided.
        if self.list.capacity() / 4 > needed {
            self.list.shrink_to(needed);
        }
        self.list.reserve(additional);
    }

    /// Iterates over I/O events.
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        // On some platforms, closing the read end of a pipe wakes up writers, but the
        // event is reported as EVFILT_READ with the EV_EOF flag.
        //
        // https://github.com/golang/go/commit/23aad448b1e3f7c3b4ba2af90120bde91ac865b4
        self.list.iter().filter_map(|ev| {
            // Failed deferred change, report it so the failure surfaces on the next I/O.
            if ev.flags().contains(kqueue::EventFlags::ERROR) {
                let data = ev.data();
                return if data == 0
                    || data == Errno::NOENT.raw_os_error() as _
                    || data == Errno::PIPE.raw_os_error() as _
                {
                    None
                } else {
                    Some(Event::all(ev.udata() as usize))
                };
            }

            Some(Event {
                key: ev.udata() as usize,
                readable: matches!(
                    ev.filter(),
                    kqueue::EventFilter::Read(..)
                        | kqueue::EventFilter::Vnode { .. }
                        | kqueue::EventFilter::Proc { .. }
                        | kqueue::EventFilter::Signal { .. }
                        | kqueue::EventFilter::Timer { .. }
                ),
                writable: matches!(ev.filter(), kqueue::EventFilter::Write(..))
                    || (matches!(ev.filter(), kqueue::EventFilter::Read(..))
                        && (ev.flags().intersects(kqueue::EventFlags::EOF))),
                extra: EventExtra,
            })
        })
    }

    /// Clears the list.
    pub fn clear(&mut self) {
        self.list.clear();
    }

    /// Get the capacity of the list.
    pub fn capacity(&self) -> usize {
        self.list.capacity()
    }
}

/// Extra information associated with an event.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct EventExtra;

impl EventExtra {
    /// Create a new, empty version of this struct.
    #[inline]
    pub const fn empty() -> EventExtra {
        EventExtra
    }

    /// Set the interrupt flag.
    #[inline]
    pub fn set_hup(&mut self, _value: bool) {
        // No-op.
    }

    /// Set the rd interrupt flag.
    #[inline]
    pub fn set_rd_hup(&mut self, _value: bool) {
        // No-op.
    }

    /// Set the priority flag.
    #[inline]
    pub fn set_pri(&mut self, _value: bool) {
        // No-op.
    }

    /// Is the interrupt flag set?
    #[inline]
    pub fn is_hup(&self) -> bool {
        false
    }

    /// Is the rd interrupt flag set?
    #[inline]
    pub fn is_rd_hup(&self) -> bool {
        false
    }

    /// Is the priority flag set?
    #[inline]
    pub fn is_pri(&self) -> bool {
        false
    }

    #[inline]
    pub fn is_connect_failed(&self) -> Option<bool> {
        None
    }

    #[inline]
    pub fn is_err(&self) -> Option<bool> {
        None
    }
}

pub(crate) fn mode_to_flags(mode: PollMode) -> kqueue::EventFlags {
    use kqueue::EventFlags as EV;

    match mode {
        PollMode::Oneshot => EV::ONESHOT,
        PollMode::Level => EV::empty(),
        PollMode::Edge => EV::CLEAR,
        PollMode::EdgeOneshot => EV::ONESHOT | EV::CLEAR,
    }
}

#[cfg(any(
    target_os = "freebsd",
    target_os = "dragonfly",
    target_vendor = "apple",
))]
mod notify {
    use super::Poller;
    use rustix::event::kqueue;
    use std::io;
    #[cfg(feature = "tracing")]
    use std::os::unix::io::BorrowedFd;
    use std::os::unix::io::OwnedFd;
    use std::sync::Arc;

    /// A notification pipe.
    ///
    /// This implementation uses `EVFILT_USER` to avoid allocating a pipe.
    #[derive(Debug)]
    pub(super) struct Notify;

    /// Wakes up the poller by triggering the `EVFILT_USER` event, can be used from any thread.
    #[derive(Debug, Clone)]
    pub struct Waker(Arc<OwnedFd>);

    impl Waker {
        /// Sends a notification to wake up the current or next `wait()` call.
        pub fn notify(&self) -> io::Result<()> {
            #[cfg(feature = "tracing")]
            let span = tracing::trace_span!("notify", kqueue_fd = ?self.0);
            #[cfg(feature = "tracing")]
            let _enter = span.enter();

            // Trigger the EVFILT_USER event.
            super::submit_changes(
                &self.0,
                [kqueue::Event::new(
                    kqueue::EventFilter::User {
                        ident: 0,
                        flags: kqueue::UserFlags::TRIGGER,
                        user_flags: kqueue::UserDefinedFlags::new(0),
                    },
                    kqueue::EventFlags::ADD | kqueue::EventFlags::RECEIPT,
                    crate::NOTIFY_KEY as _,
                )],
            )
            .ok();
            Ok(())
        }
    }

    impl Notify {
        /// Creates a new notification pipe.
        pub(super) fn new() -> io::Result<Self> {
            Ok(Self)
        }

        /// Registers this notification pipe in the `Poller`.
        pub(super) fn register(&self, poller: &Poller) -> io::Result<()> {
            // Register an EVFILT_USER event.
            poller.submit_changes([kqueue::Event::new(
                kqueue::EventFilter::User {
                    ident: 0,
                    flags: kqueue::UserFlags::empty(),
                    user_flags: kqueue::UserDefinedFlags::new(0),
                },
                kqueue::EventFlags::ADD | kqueue::EventFlags::RECEIPT | kqueue::EventFlags::CLEAR,
                crate::NOTIFY_KEY as _,
            )])
        }

        /// Reregister this notification pipe in the `Poller`.
        pub(super) fn reregister(&self, _poller: &Poller) -> io::Result<()> {
            // We don't need to do anything, it's already registered as EV_CLEAR.
            Ok(())
        }

        /// Returns a waker for the `Poller`.
        pub(super) fn waker(&self, poller: &Poller) -> Waker {
            Waker(poller.kqueue_fd.clone())
        }

        /// Deregisters this notification pipe from the `Poller`.
        pub(super) fn deregister(&self, poller: &Poller) -> io::Result<()> {
            // Deregister the EVFILT_USER event.
            poller.submit_changes([kqueue::Event::new(
                kqueue::EventFilter::User {
                    ident: 0,
                    flags: kqueue::UserFlags::empty(),
                    user_flags: kqueue::UserDefinedFlags::new(0),
                },
                kqueue::EventFlags::DELETE | kqueue::EventFlags::RECEIPT,
                crate::NOTIFY_KEY as _,
            )])
        }

        /// Whether this raw file descriptor is associated with this pipe.
        #[cfg(feature = "tracing")]
        pub(super) fn has_fd(&self, _fd: BorrowedFd<'_>) -> bool {
            false
        }
    }
}

#[cfg(not(any(
    target_os = "freebsd",
    target_os = "dragonfly",
    target_vendor = "apple",
)))]
mod notify {
    use super::Poller;
    use crate::{Event, PollMode, NOTIFY_KEY};
    use std::io::{self, prelude::*};
    #[cfg(feature = "tracing")]
    use std::os::unix::io::BorrowedFd;
    use std::os::unix::{
        io::{AsFd, AsRawFd},
        net::UnixStream,
    };
    use std::sync::Arc;

    /// A notification pipe.
    ///
    /// This implementation uses a pipe to send notifications.
    #[derive(Debug)]
    pub(super) struct Notify {
        /// The read end of the pipe.
        read_stream: UnixStream,

        /// The write end of the pipe, shared with the wakers.
        write_stream: Arc<UnixStream>,
    }

    /// Wakes up the poller by writing to the pipe, can be used from any thread.
    #[derive(Debug, Clone)]
    pub struct Waker(Arc<UnixStream>);

    impl Waker {
        /// Sends a notification to wake up the current or next `wait()` call.
        #[allow(clippy::unused_io_amount)]
        pub fn notify(&self) -> io::Result<()> {
            #[cfg(feature = "tracing")]
            let span = tracing::trace_span!("notify", write_stream = ?self.0);
            #[cfg(feature = "tracing")]
            let _enter = span.enter();

            // Write to the write end of the pipe
            (&*self.0).write(&[1]).ok();
            Ok(())
        }
    }

    impl Notify {
        /// Creates a new notification pipe.
        pub(super) fn new() -> io::Result<Self> {
            let (read_stream, write_stream) = UnixStream::pair()?;
            read_stream.set_nonblocking(true)?;
            write_stream.set_nonblocking(true)?;

            Ok(Self {
                read_stream,
                write_stream: Arc::new(write_stream),
            })
        }

        /// Registers this notification pipe in the `Poller`.
        pub(super) fn register(&self, poller: &Poller) -> io::Result<()> {
            // Register the read end of this pipe.
            unsafe {
                poller.add(
                    self.read_stream.as_raw_fd(),
                    Event::readable(NOTIFY_KEY),
                    PollMode::Oneshot,
                )
            }
        }

        /// Reregister this notification pipe in the `Poller`.
        pub(super) fn reregister(&self, poller: &Poller) -> io::Result<()> {
            // Clear out the notification.
            while (&self.read_stream).read(&mut [0; 64]).is_ok() {}

            // Reregister the read end of this pipe.
            poller.modify(
                self.read_stream.as_fd(),
                Event::readable(NOTIFY_KEY),
                PollMode::Oneshot,
            )
        }

        /// Returns a waker for the `Poller`.
        pub(super) fn waker(&self, _poller: &Poller) -> Waker {
            Waker(self.write_stream.clone())
        }

        /// Deregisters this notification pipe from the `Poller`.
        pub(super) fn deregister(&self, poller: &Poller) -> io::Result<()> {
            // Deregister the read end of the pipe.
            poller.delete(self.read_stream.as_fd())
        }

        /// Whether this raw file descriptor is associated with this pipe.
        #[cfg(feature = "tracing")]
        pub(super) fn has_fd(&self, fd: BorrowedFd<'_>) -> bool {
            self.read_stream.as_raw_fd() == fd.as_raw_fd()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::net::UnixStream;

    #[test]
    fn deferred_changes_coalesce_and_keep_last_interest() -> io::Result<()> {
        let poller = Poller::new()?;
        let (socket, mut peer) = UnixStream::pair()?;
        unsafe { poller.add(socket.as_raw_fd(), Event::none(0), PollMode::Oneshot)? };
        for key in 0..8192 {
            poller.modify_deferred(socket.as_fd(), Event::readable(key), PollMode::Level)?;
        }
        poller.modify_deferred(socket.as_fd(), Event::writable(8192), PollMode::Oneshot)?;
        assert_eq!(poller.pending.borrow().changes.0.len(), 2);
        assert_eq!(poller.pending.borrow().index.len(), 1);

        let (unregistered, _peer) = UnixStream::pair()?;
        for result in [
            poller.modify(unregistered.as_fd(), Event::none(0), PollMode::Oneshot),
            poller.modify_deferred(unregistered.as_fd(), Event::none(0), PollMode::Oneshot),
        ] {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
        }
        assert_eq!(poller.pending.borrow().changes.0.len(), 2);

        let mut events = Events::with_capacity(16);
        poller.wait_deadline(&mut events, Some(Instant::now()))?;
        assert_eq!(events.iter().collect::<Vec<_>>(), [Event::writable(8192)]);
        assert!(poller.pending.borrow().index.is_empty());
        assert!(poller.pending.borrow().changes.0.is_empty());

        events.clear();
        poller.modify_deferred(socket.as_fd(), Event::readable(8193), PollMode::Oneshot)?;
        peer.write_all(b"ready")?;
        poller.wait_deadline(&mut events, Some(Instant::now()))?;
        assert_eq!(events.iter().collect::<Vec<_>>(), [Event::readable(8193)]);
        poller.delete(socket.as_fd())?;
        Ok(())
    }

    #[test]
    fn deleting_deferred_changes_fixes_moved_indices() -> io::Result<()> {
        let poller = Poller::new()?;
        let mut sockets = (0..4)
            .map(|_| UnixStream::pair())
            .collect::<io::Result<Vec<_>>>()?;
        for (key, (socket, _)) in sockets.iter().enumerate() {
            unsafe { poller.add(socket.as_raw_fd(), Event::none(key), PollMode::Oneshot)? };
            poller.modify_deferred(socket.as_fd(), Event::readable(key), PollMode::Oneshot)?;
        }
        poller.delete(sockets[0].0.as_fd())?;
        assert_eq!(poller.pending.borrow().changes.0.len(), 6);
        poller.modify_deferred(sockets[3].0.as_fd(), Event::writable(33), PollMode::Oneshot)?;
        poller.modify(sockets[1].0.as_fd(), Event::none(1), PollMode::Oneshot)?;
        assert_eq!(poller.pending.borrow().changes.0.len(), 4);
        poller.modify_deferred(sockets[2].0.as_fd(), Event::readable(22), PollMode::Oneshot)?;
        poller.delete(sockets[1].0.as_fd())?;
        for (_, peer) in &mut sockets {
            peer.write_all(b"ready")?;
        }

        let mut events = Events::with_capacity(16);
        poller.wait_deadline(&mut events, Some(Instant::now()))?;
        let mut received = events.iter().collect::<Vec<_>>();
        received.sort_by_key(|event| event.key);
        assert_eq!(received, [Event::readable(22), Event::writable(33)]);
        poller.delete(sockets[2].0.as_fd())?;
        poller.delete(sockets[3].0.as_fd())?;
        Ok(())
    }

    #[test]
    fn deleted_fd_can_be_reused_with_deferred_changes() -> io::Result<()> {
        let poller = Poller::new()?;
        let (socket, _old_peer) = UnixStream::pair()?;
        let mut socket = OwnedFd::from(socket);
        let raw = socket.as_raw_fd();
        unsafe { poller.add(raw, Event::none(0), PollMode::Oneshot)? };
        poller.modify_deferred(socket.as_fd(), Event::readable(1), PollMode::Oneshot)?;
        poller.delete(socket.as_fd())?;
        assert!(poller.pending.borrow().changes.0.is_empty());
        assert!(poller.pending.borrow().index.is_empty());
        assert_eq!(
            poller
                .modify_deferred(socket.as_fd(), Event::none(2), PollMode::Oneshot)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );

        let (replacement, mut peer) = UnixStream::pair()?;
        // Deterministically reuse the descriptor without racing other tests' allocations.
        rustix::io::dup2(&replacement, &mut socket)?;
        assert_eq!(socket.as_raw_fd(), raw);
        unsafe { poller.add(raw, Event::none(2), PollMode::Oneshot)? };
        poller.modify_deferred(socket.as_fd(), Event::writable(3), PollMode::Oneshot)?;
        let mut events = Events::with_capacity(16);
        poller.wait_deadline(&mut events, Some(Instant::now()))?;
        assert_eq!(events.iter().collect::<Vec<_>>(), [Event::writable(3)]);

        events.clear();
        poller.modify_deferred(socket.as_fd(), Event::readable(4), PollMode::Oneshot)?;
        peer.write_all(b"ready")?;
        poller.wait_deadline(&mut events, Some(Instant::now()))?;
        assert_eq!(events.iter().collect::<Vec<_>>(), [Event::readable(4)]);
        poller.delete(socket.as_fd())?;
        Ok(())
    }

    #[test]
    fn deferred_burst_capacity_is_reclaimed_when_idle() -> io::Result<()> {
        let poller = Poller::new()?;
        let sockets = (0..4096)
            .map(|_| UnixStream::pair())
            .collect::<io::Result<Vec<_>>>()?;
        let mut events = Events::with_capacity(1024);
        for (key, (socket, _)) in sockets.iter().enumerate() {
            unsafe { poller.add(socket.as_raw_fd(), Event::none(key), PollMode::Oneshot)? };
            poller.modify_deferred(socket.as_fd(), Event::none(key), PollMode::Oneshot)?;
        }
        poller.wait_deadline(&mut events, Some(Instant::now()))?;
        assert!(events.iter().next().is_none());
        assert!(events.capacity() >= 8192);
        // Both alternating change buffers have been through a quiet wait.
        for _ in 0..2 {
            poller.wait_deadline(&mut events, Some(Instant::now()))?;
        }
        assert!(poller.pending.borrow().changes.0.capacity() <= 1024);
        assert!(poller.submit.borrow().0.capacity() <= 1024);
        assert!(poller.pending.borrow().index.capacity() <= 2048);
        assert_eq!(events.capacity(), 1024);
        for (socket, _) in &sockets {
            poller.delete(socket.as_fd())?;
        }
        Ok(())
    }
}
