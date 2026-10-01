//! Bindings to epoll (Linux, Android).

#[cfg(not(target_os = "redox"))]
use std::cell::Cell;
use std::io;
use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, RawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(not(target_os = "redox"))]
use rustix::event::{eventfd, EventfdFlags};
#[cfg(not(target_os = "redox"))]
use rustix::time::{
    timerfd_create, timerfd_settime, Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags,
};

use rustix::buffer::spare_capacity;
use rustix::event::{epoll, Timespec};
use rustix::fd::OwnedFd;
use rustix::fs::{fcntl_getfl, fcntl_setfl, OFlags};
use rustix::io::{fcntl_getfd, fcntl_setfd, read, write, FdFlags};
use rustix::pipe::{pipe, pipe_with, PipeFlags};

use crate::{Event, PollMode};

/// Interface to epoll.
#[derive(Debug)]
pub struct Poller {
    /// File descriptor for the epoll instance.
    epoll_fd: OwnedFd,

    /// Notifier used to wake up epoll, shared with the wakers.
    notifier: Arc<Notifier>,

    /// File descriptor for the timerfd that produces timeouts.
    ///
    /// Redox does not support timerfd.
    #[cfg(not(target_os = "redox"))]
    timer_fd: Option<OwnedFd>,

    /// Whether the timerfd may be armed by a previous wait.
    #[cfg(not(target_os = "redox"))]
    timer_armed: Cell<bool>,
}

/// Polling mode of the notifier.
///
/// An edge-triggered notifier stays armed, so a wait only reads it after it
/// fired and never re-arms it. Redox keeps the oneshot registration.
#[cfg(not(target_os = "redox"))]
const NOTIFIER_MODE: PollMode = PollMode::Edge;
#[cfg(target_os = "redox")]
const NOTIFIER_MODE: PollMode = PollMode::Oneshot;

impl Poller {
    /// Creates a new poller.
    pub fn new() -> io::Result<Poller> {
        // Create an epoll instance.
        //
        // Use `epoll_create1` with `EPOLL_CLOEXEC`.
        let epoll_fd = epoll::create(epoll::CreateFlags::CLOEXEC)?;

        // Set up notifier and timerfd.
        let notifier = Arc::new(Notifier::new()?);
        #[cfg(not(target_os = "redox"))]
        let timer_fd = timerfd_create(
            TimerfdClockId::Monotonic,
            TimerfdFlags::CLOEXEC | TimerfdFlags::NONBLOCK,
        )
        .ok();

        let poller = Poller {
            epoll_fd,
            notifier,
            #[cfg(not(target_os = "redox"))]
            timer_fd,
            #[cfg(not(target_os = "redox"))]
            timer_armed: Cell::new(false),
        };

        unsafe {
            // The timer is edge-triggered, so arming it with `timerfd_settime`
            // is enough and it never needs to be re-registered.
            #[cfg(not(target_os = "redox"))]
            if let Some(ref timer_fd) = poller.timer_fd {
                poller.add(
                    timer_fd.as_raw_fd(),
                    Event::readable(crate::NOTIFY_KEY),
                    PollMode::Edge,
                )?;
            }

            poller.add(
                poller.notifier.as_fd().as_raw_fd(),
                Event::readable(crate::NOTIFY_KEY),
                NOTIFIER_MODE,
            )?;
        }

        #[cfg(feature = "tracing")]
        tracing::trace!(
            epoll_fd = ?poller.epoll_fd.as_raw_fd(),
            notifier = ?poller.notifier,
            "new",
        );
        Ok(poller)
    }

    /// Whether this poller supports level-triggered events.
    pub fn supports_level(&self) -> bool {
        true
    }

    /// Whether the poller supports edge-triggered events.
    pub fn supports_edge(&self) -> bool {
        true
    }

    /// Adds a new file descriptor.
    ///
    /// # Safety
    ///
    /// The `fd` must be a valid file descriptor. The usual condition of remaining registered in
    /// the `Poller` doesn't apply to `epoll`.
    pub unsafe fn add(&self, fd: RawFd, ev: Event, mode: PollMode) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "add",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            ?fd,
            ?ev,
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        epoll::add(
            &self.epoll_fd,
            unsafe { rustix::fd::BorrowedFd::borrow_raw(fd) },
            epoll::EventData::new_u64(ev.key as u64),
            epoll_flags(&ev, mode) | ev.extra.flags,
        )?;

        Ok(())
    }

    /// Modifies an existing file descriptor.
    pub fn modify(&self, fd: BorrowedFd<'_>, ev: Event, mode: PollMode) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "modify",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            ?fd,
            ?ev,
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        epoll::modify(
            &self.epoll_fd,
            fd,
            epoll::EventData::new_u64(ev.key as u64),
            epoll_flags(&ev, mode) | ev.extra.flags,
        )?;

        Ok(())
    }

    /// Modifies an existing file descriptor, epoll applies it immediately.
    #[inline]
    pub fn modify_deferred(&self, fd: BorrowedFd<'_>, ev: Event, mode: PollMode) -> io::Result<()> {
        self.modify(fd, ev, mode)
    }

    /// Deletes a file descriptor.
    #[cfg_attr(not(feature = "tracing"), inline(always))]
    pub fn delete(&self, fd: BorrowedFd<'_>) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "delete",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            ?fd,
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        epoll::delete(&self.epoll_fd, fd)?;

        Ok(())
    }

    /// Waits for I/O events with an optional deadline.
    pub fn wait_deadline(&self, events: &mut Events, deadline: Option<Instant>) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "wait",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            ?deadline,
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        let timeout = deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));

        // Only a positive timeout needs the timer, a zero timeout is passed to
        // epoll directly.
        #[cfg(not(target_os = "redox"))]
        let timer_fd = match (&self.timer_fd, timeout) {
            (Some(timer_fd), Some(t)) if !t.is_zero() => {
                let mut it_value = TS_ZERO;
                it_value.tv_sec = t.as_secs() as _;
                it_value.tv_nsec = t.subsec_nanos() as _;
                set_timer(timer_fd, it_value)?;
                self.timer_armed.set(true);
                Some(timer_fd)
            }
            (Some(timer_fd), _) => {
                // A timer left armed by a previous wait would wake this one up.
                if self.timer_armed.replace(false) {
                    set_timer(timer_fd, TS_ZERO)?;
                }
                None
            }
            (None, _) => None,
        };

        #[cfg(target_os = "redox")]
        let timer_fd: Option<core::convert::Infallible> = None;

        // Timeout for epoll. In case of overflow, use no timeout.
        let timeout = match (timer_fd, timeout) {
            (_, Some(t)) if t == Duration::from_secs(0) => Some(Timespec::default()),
            (None, Some(t)) => Timespec::try_from(t).ok(),
            _ => None,
        };

        // Wait for I/O events.
        epoll::wait(
            &self.epoll_fd,
            spare_capacity(&mut events.list),
            timeout.as_ref(),
        )?;
        #[cfg(feature = "tracing")]
        tracing::trace!(
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            res = ?events.list.len(),
            "new events",
        );

        // Clear the notification, if received. The notifier is edge-triggered
        // and stays armed. Each notification is a new edge, so one arriving
        // after it is cleared is never lost.
        #[cfg(not(target_os = "redox"))]
        if events
            .list
            .iter()
            .any(|ev| ev.data.u64() == crate::NOTIFY_KEY as u64)
        {
            self.notifier.clear();
        }

        // Clear the notification (if received) and re-register interest in it.
        #[cfg(target_os = "redox")]
        {
            self.notifier.clear();
            self.modify(
                self.notifier.as_fd(),
                Event::readable(crate::NOTIFY_KEY),
                NOTIFIER_MODE,
            )?;
        }
        Ok(())
    }

    /// Returns a waker that wakes up the current or next `wait()` call.
    pub fn waker(&self) -> Waker {
        Waker(self.notifier.clone())
    }
}

/// Wakes up the poller, can be used from any thread.
#[derive(Debug, Clone)]
pub struct Waker(Arc<Notifier>);

impl Waker {
    /// Sends a notification to wake up the current or next `wait()` call.
    pub fn notify(&self) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!("notify", notifier = ?self.0);
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        self.0.notify();
        Ok(())
    }
}

impl AsRawFd for Poller {
    fn as_raw_fd(&self) -> RawFd {
        self.epoll_fd.as_raw_fd()
    }
}

impl AsFd for Poller {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.epoll_fd.as_fd()
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!(
            "drop",
            epoll_fd = ?self.epoll_fd.as_raw_fd(),
            notifier = ?self.notifier,
        );
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        #[cfg(not(target_os = "redox"))]
        if let Some(timer_fd) = self.timer_fd.take() {
            let _ = self.delete(timer_fd.as_fd());
        }
        let _ = self.delete(self.notifier.as_fd());
    }
}

/// `timespec` value that equals zero.
#[cfg(not(target_os = "redox"))]
const TS_ZERO: Timespec = unsafe { std::mem::transmute([0u8; std::mem::size_of::<Timespec>()]) };

/// Arms the timer to expire once after `it_value`, or disarms it if zero.
#[cfg(not(target_os = "redox"))]
#[allow(clippy::needless_update)]
fn set_timer(timer_fd: &OwnedFd, it_value: Timespec) -> io::Result<()> {
    let new_val = Itimerspec {
        it_interval: TS_ZERO,
        it_value,
        ..unsafe { std::mem::zeroed() }
    };
    timerfd_settime(timer_fd, TimerfdTimerFlags::empty(), &new_val)?;
    Ok(())
}

/// Get the EPOLL flags for the interest.
fn epoll_flags(interest: &Event, mode: PollMode) -> epoll::EventFlags {
    let mut flags = match mode {
        PollMode::Oneshot => epoll::EventFlags::ONESHOT,
        PollMode::Level => epoll::EventFlags::empty(),
        PollMode::Edge => epoll::EventFlags::ET,
        PollMode::EdgeOneshot => epoll::EventFlags::ET | epoll::EventFlags::ONESHOT,
    };
    if interest.readable {
        flags |= read_flags();
    }
    if interest.writable {
        flags |= write_flags();
    }
    flags
}

/// Epoll flags for all possible readability events.
fn read_flags() -> epoll::EventFlags {
    use epoll::EventFlags as Epoll;
    Epoll::IN | Epoll::HUP | Epoll::ERR | Epoll::PRI
}

/// Epoll flags for all possible writability events.
fn write_flags() -> epoll::EventFlags {
    use epoll::EventFlags as Epoll;
    Epoll::OUT | Epoll::HUP | Epoll::ERR
}

/// A list of reported I/O events.
pub struct Events {
    list: Vec<epoll::Event>,
}

unsafe impl Send for Events {}

impl Events {
    /// Creates an empty list.
    pub fn with_capacity(cap: usize) -> Events {
        Events {
            list: Vec::with_capacity(cap),
        }
    }

    /// Iterates over I/O events.
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.list.iter().map(|ev| {
            let flags = ev.flags;
            Event {
                key: ev.data.u64() as usize,
                readable: flags.intersects(read_flags()),
                writable: flags.intersects(write_flags()),
                extra: EventExtra { flags },
            }
        })
    }

    /// Clear the list.
    pub fn clear(&mut self) {
        self.list.clear();
    }

    /// Get the capacity of the list.
    pub fn capacity(&self) -> usize {
        self.list.capacity()
    }
}

/// Extra information about this event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventExtra {
    flags: epoll::EventFlags,
}

impl EventExtra {
    /// Create an empty version of the data.
    #[inline]
    pub const fn empty() -> EventExtra {
        EventExtra {
            flags: epoll::EventFlags::empty(),
        }
    }

    /// Add the interrupt flag to this event.
    #[inline]
    pub fn set_hup(&mut self, active: bool) {
        self.flags.set(epoll::EventFlags::HUP, active);
    }

    /// Set the rd interrupt flag.
    #[inline]
    pub fn set_rd_hup(&mut self, active: bool) {
        self.flags.set(epoll::EventFlags::RDHUP, active);
    }

    /// Add the priority flag to this event.
    #[inline]
    pub fn set_pri(&mut self, active: bool) {
        self.flags.set(epoll::EventFlags::PRI, active);
    }

    /// Tell if the interrupt flag is set.
    #[inline]
    pub fn is_hup(&self) -> bool {
        self.flags.intersects(epoll::EventFlags::HUP)
    }

    /// Tell if the rd interrupt flag is set.
    #[inline]
    pub fn is_rd_hup(&self) -> bool {
        self.flags.intersects(epoll::EventFlags::RDHUP)
    }

    /// Tell if the priority flag is set.
    #[inline]
    pub fn is_pri(&self) -> bool {
        self.flags.contains(epoll::EventFlags::PRI)
    }

    #[inline]
    pub fn is_connect_failed(&self) -> Option<bool> {
        Some(
            self.flags.contains(epoll::EventFlags::ERR)
                && self.flags.contains(epoll::EventFlags::HUP),
        )
    }

    #[inline]
    pub fn is_err(&self) -> Option<bool> {
        Some(self.flags.contains(epoll::EventFlags::ERR))
    }
}

/// The notifier for Linux.
///
/// Certain container runtimes do not expose eventfd to the client, as it relies on the host and
/// can be used to "escape" the container under certain conditions. Gramine is the prime example,
/// see [here](gramine). In this case, fall back to using a pipe.
///
/// [gramine]: https://gramine.readthedocs.io/en/stable/manifest-syntax.html#allowing-eventfd
#[derive(Debug)]
enum Notifier {
    /// The primary notifier, using eventfd.
    #[cfg(not(target_os = "redox"))]
    EventFd(OwnedFd),

    /// The fallback notifier, using a pipe.
    Pipe {
        /// The read end of the pipe.
        read_pipe: OwnedFd,

        /// The write end of the pipe.
        write_pipe: OwnedFd,
    },
}

impl Notifier {
    /// Create a new notifier.
    fn new() -> io::Result<Self> {
        // Skip eventfd for testing if necessary.
        #[cfg(not(target_os = "redox"))]
        {
            if !cfg!(polling_test_epoll_pipe) {
                // Try to create an eventfd.
                match eventfd(0, EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK) {
                    Ok(fd) => {
                        #[cfg(feature = "tracing")]
                        tracing::trace!("created eventfd for notifier");
                        return Ok(Notifier::EventFd(fd));
                    }

                    Err(_err) => {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            "eventfd() failed with error ({}), falling back to pipe",
                            _err
                        );
                    }
                }
            }
        }

        let (read, write) = pipe_with(PipeFlags::CLOEXEC).or_else(|_| {
            let (read, write) = pipe()?;
            fcntl_setfd(&read, fcntl_getfd(&read)? | FdFlags::CLOEXEC)?;
            fcntl_setfd(&write, fcntl_getfd(&write)? | FdFlags::CLOEXEC)?;
            io::Result::Ok((read, write))
        })?;

        fcntl_setfl(&read, fcntl_getfl(&read)? | OFlags::NONBLOCK)?;
        Ok(Notifier::Pipe {
            read_pipe: read,
            write_pipe: write,
        })
    }

    /// The file descriptor to register in the poller.
    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            #[cfg(not(target_os = "redox"))]
            Notifier::EventFd(fd) => fd.as_fd(),
            Notifier::Pipe {
                read_pipe: read, ..
            } => read.as_fd(),
        }
    }

    /// Notify the poller.
    fn notify(&self) {
        match self {
            #[cfg(not(target_os = "redox"))]
            Self::EventFd(fd) => {
                let buf: [u8; 8] = 1u64.to_ne_bytes();
                let _ = write(fd, &buf);
            }

            Self::Pipe { write_pipe, .. } => {
                write(write_pipe, &[0; 1]).ok();
            }
        }
    }

    /// Clear the notification.
    fn clear(&self) {
        match self {
            #[cfg(not(target_os = "redox"))]
            Self::EventFd(fd) => {
                let mut buf = [0u8; 8];
                let _ = read(fd, &mut buf);
            }

            Self::Pipe { read_pipe, .. } => while read(read_pipe, &mut [0u8; 1024]).is_ok() {},
        }
    }
}
