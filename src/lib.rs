//! Portable interface to epoll, kqueue, event ports, and IOCP.
//!
//! Supported platforms:
//! - [epoll](https://en.wikipedia.org/wiki/Epoll): Linux, Android, RedoxOS
//! - [kqueue](https://en.wikipedia.org/wiki/Kqueue): macOS, iOS, tvOS, watchOS, visionOS, FreeBSD, NetBSD, OpenBSD,
//!   DragonFly BSD
//! - [event ports](https://illumos.org/man/port_create): illumos, Solaris
//! - [poll](https://en.wikipedia.org/wiki/Poll_(Unix)): VxWorks, Fuchsia, HermitOS, other Unix systems
//! - [IOCP](https://learn.microsoft.com/en-us/windows/win32/fileio/i-o-completion-ports): Windows, Wine (version 7.13+)
//!
//! By default, polling is done in oneshot mode, which means interest in I/O events needs to
//! be re-enabled after an event is delivered if we're interested in the next event of the same
//! kind. However, level and edge triggered modes are also available for certain operating
//! systems. See the documentation of the [`PollMode`] type for more information.
//!
//! A [`Poller`] is [`Send`] but not [`Sync`], it is used by the thread that waits for I/O
//! events. Other threads can wake it up with a [`Notifier`].
//!
//! # Examples
//!
//! ```no_run
//! use ntex_polling::{Event, Events, Poller};
//! use std::net::TcpListener;
//!
//! // Create a TCP listener.
//! let socket = TcpListener::bind("127.0.0.1:8000")?;
//! socket.set_nonblocking(true)?;
//! let key = 7; // Arbitrary key identifying the socket.
//!
//! // Create a poller and register interest in readability on the socket.
//! let poller = Poller::new()?;
//! unsafe {
//!     poller.add(&socket, Event::readable(key))?;
//! }
//!
//! // The event loop.
//! let mut events = Events::new();
//! loop {
//!     // Wait for at least one I/O event.
//!     events.clear();
//!     poller.wait(&mut events, None)?;
//!
//!     for ev in events.iter() {
//!         if ev.key == key {
//!             // Perform a non-blocking accept operation.
//!             socket.accept()?;
//!             // Set interest in the next readability event.
//!             poller.modify(&socket, Event::readable(key))?;
//!         }
//!     }
//! }
//!
//! poller.delete(&socket)?;
//! # std::io::Result::Ok(())
//! ```

#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]
#![allow(clippy::useless_conversion, clippy::unnecessary_cast, unused_unsafe)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]

use std::cell::Cell;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cfg_if::cfg_if;

cfg_if! {
    // Note: This cfg is intended to make it easy for polling developers to test
    // the backend that uses poll, and is not a public API.
    if #[cfg(polling_test_poll_backend)] {
        mod poll;
        use poll as sys;
    } else if #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "redox"
    ))] {
        mod epoll;
        use epoll as sys;
    } else if #[cfg(any(
        target_os = "illumos",
        target_os = "solaris",
    ))] {
        mod port;
        use port as sys;
    } else if #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ))] {
        mod kqueue;
        use kqueue as sys;
    } else if #[cfg(any(
        target_os = "vxworks",
        target_os = "hermit",
        target_os = "fuchsia",
        target_os = "horizon",
        unix,
    ))] {
        mod poll;
        use poll as sys;
    } else if #[cfg(target_os = "windows")] {
        mod iocp;
        use iocp as sys;
    } else {
        compile_error!("polling does not support this target OS");
    }
}

pub mod os;

/// Key associated with notifications.
const NOTIFY_KEY: usize = usize::MAX;

/// Indicates that a file descriptor or socket can read or write without blocking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    /// Key identifying the file descriptor or socket.
    pub key: usize,
    /// Can it do a read operation without blocking?
    pub readable: bool,
    /// Can it do a write operation without blocking?
    pub writable: bool,
    /// System-specific event data.
    extra: sys::EventExtra,
}

/// The mode in which the poller waits for I/O events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum PollMode {
    /// Poll in oneshot mode.
    ///
    /// In this mode, the poller will only deliver one event per file descriptor or socket.
    /// Once an event has been delivered, interest in the event needs to be re-enabled
    /// by calling `Poller::modify` or `Poller::add`.
    ///
    /// This is the default mode.
    Oneshot,

    /// Poll in level-triggered mode.
    ///
    /// Once an event has been delivered, polling will continue to deliver that event
    /// until interest in the event is disabled by calling `Poller::modify` or `Poller::delete`.
    ///
    /// Not all operating system support this mode. Trying to register a file descriptor with
    /// this mode in an unsupported operating system will raise an error. You can check if
    /// the operating system supports this mode by calling `Poller::supports_level`.
    Level,

    /// Poll in edge-triggered mode.
    ///
    /// Once an event has been delivered, polling will not deliver that event again unless
    /// a new event occurs.
    ///
    /// Not all operating system support this mode. Trying to register a file descriptor with
    /// this mode in an unsupported operating system will raise an error. You can check if
    /// the operating system supports this mode by calling `Poller::supports_edge`.
    Edge,

    /// Poll in both edge-triggered and oneshot mode.
    ///
    /// This mode is similar to the `Oneshot` mode, but it will only deliver one event per new
    /// event.
    ///
    /// Not all operating system support this mode. Trying to register a file descriptor with
    /// this mode in an unsupported operating system will raise an error. You can check if
    /// the operating system supports this mode by calling `Poller::supports_edge`.
    EdgeOneshot,
}

impl Event {
    /// Create a new event.
    pub const fn new(key: usize, readable: bool, writable: bool) -> Event {
        Event {
            key,
            readable,
            writable,
            extra: sys::EventExtra::empty(),
        }
    }

    /// All kinds of events (readable and writable).
    ///
    /// Equivalent to: `Event::new(key, true, true)`
    #[inline]
    pub const fn all(key: usize) -> Event {
        Event::new(key, true, true)
    }

    /// Only the readable event.
    ///
    /// Equivalent to: `Event::new(key, true, false)`
    #[inline]
    pub const fn readable(key: usize) -> Event {
        Event::new(key, true, false)
    }

    /// Only the writable event.
    ///
    /// Equivalent to: `Event::new(key, false, true)`
    #[inline]
    pub const fn writable(key: usize) -> Event {
        Event::new(key, false, true)
    }

    /// No events.
    ///
    /// Equivalent to: `Event::new(key, false, false)`
    #[inline]
    pub const fn none(key: usize) -> Event {
        Event::new(key, false, false)
    }

    /// Add interruption events to this interest.
    ///
    /// This usually indicates that the file descriptor or socket has been closed. It corresponds
    /// to the `EPOLLHUP` and `POLLHUP` events.
    ///
    /// Interruption events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this function is a no-op.
    #[inline]
    pub fn set_interrupt(&mut self, active: bool) {
        self.extra.set_hup(active);
    }

    /// Add rd interruption events to this interest.
    ///
    /// This usually indicates that the file descriptor or socket has been closed. It corresponds
    /// to the `EPOLLRDHUP` and `POLLRDHUP` events.
    ///
    /// Interruption events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll` on Linux, Android, FreeBSD and illumos
    /// - IOCP
    /// - Event Ports on illumos
    ///
    /// On other platforms, this function is a no-op.
    #[inline]
    pub fn set_rd_interrupt(&mut self, active: bool) {
        self.extra.set_rd_hup(active);
    }

    /// Add interruption events to this interest.
    ///
    /// This usually indicates that the file descriptor or socket has been closed. It corresponds
    /// to the `EPOLLHUP` and `POLLHUP` events.
    ///
    /// Interruption events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this function is a no-op.
    #[inline]
    pub fn with_interrupt(mut self) -> Self {
        self.set_interrupt(true);
        self
    }

    /// Add rd interruption events to this interest.
    ///
    /// This usually indicates that the file descriptor or socket has been closed. It corresponds
    /// to the `EPOLLRDHUP` and `POLLRDHUP` events.
    ///
    /// Interruption events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll` on Linux, Android, FreeBSD and illumos
    /// - IOCP
    /// - Event Ports on illumos
    ///
    /// On other platforms, this function is a no-op.
    #[inline]
    pub fn with_rd_interrupt(mut self) -> Self {
        self.set_rd_interrupt(true);
        self
    }

    /// Add priority events to this interest.
    ///
    /// This indicates that there is urgent data to read. It corresponds to the `EPOLLPRI` and
    /// `POLLPRI` events.
    ///
    /// Priority events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this function is a no-op.
    #[inline]
    pub fn set_priority(&mut self, active: bool) {
        self.extra.set_pri(active);
    }

    /// Add priority events to this interest.
    ///
    /// This indicates that there is urgent data to read. It corresponds to the `EPOLLPRI` and
    /// `POLLPRI` events.
    ///
    /// Priority events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this function is a no-op.
    #[inline]
    pub fn with_priority(mut self) -> Self {
        self.set_priority(true);
        self
    }

    /// Tell if this event is the result of an interrupt notification.
    ///
    /// This usually indicates that the file descriptor or socket has been closed. It corresponds
    /// to the `EPOLLHUP` and `POLLHUP` events.
    ///
    /// Interruption events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this always returns `false`.
    #[inline]
    pub fn is_interrupt(&self) -> bool {
        self.extra.is_hup()
    }

    /// Tell if this event is the result of an rd interrupt notification.
    #[inline]
    pub fn is_rd_interrupt(&self) -> bool {
        self.extra.is_rd_hup()
    }

    /// Tell if this event is the result of a priority notification.
    ///
    /// This indicates that there is urgent data to read. It corresponds to the `EPOLLPRI` and
    /// `POLLPRI` events.
    ///
    /// Priority events are only supported on the following platforms:
    ///
    /// - `epoll`
    /// - `poll`
    /// - IOCP
    /// - Event Ports
    ///
    /// On other platforms, this always returns `false`.
    #[inline]
    pub fn is_priority(&self) -> bool {
        self.extra.is_pri()
    }

    /// Tells if this event is the result of a connection failure.
    ///
    /// This function checks if a TCP connection has failed. It corresponds to the `EPOLLERR`  or `EPOLLHUP` event in Linux
    /// and `CONNECT_FAILED` event in Windows IOCP.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{io, net};
    /// // Assuming polling and socket2 are included as dependencies in Cargo.toml
    /// use ntex_polling::Event;
    /// use socket2::Type;
    ///
    /// fn main() -> io::Result<()> {
    ///     let socket = socket2::Socket::new(socket2::Domain::IPV4, Type::STREAM, None)?;
    ///     let poller = ntex_polling::Poller::new()?;
    ///     unsafe {
    ///         poller.add(&socket, Event::new(0, true, true))?;
    ///     }
    ///     let addr = net::SocketAddr::new(net::Ipv4Addr::LOCALHOST.into(), 8080);
    ///     socket.set_nonblocking(true)?;
    ///     let _ = socket.connect(&addr.into());
    ///
    ///     let mut events = ntex_polling::Events::new();
    ///
    ///     events.clear();
    ///     poller.wait(&mut events, None)?;
    ///
    ///     let event = events.iter().next();
    ///
    ///     let event = match event {
    ///         Some(event) => event,
    ///         None => {
    ///             println!("no event");
    ///             return Ok(());
    ///         },
    ///     };
    ///
    ///     println!("event: {:?}", event);
    ///     if event
    ///         .is_connect_failed()
    ///         .unwrap_or_default()
    ///     {
    ///         println!("connect failed");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// Returns `Some(true)` if the connection has failed, `Some(false)` if the connection has not failed,
    /// or `None` if the platform does not support detecting this condition.
    #[inline]
    #[deprecated(
        since = "3.4.0",
        note = "use `is_err` in combination of is_hup instead, see documentation for `is_err`"
    )]
    pub fn is_connect_failed(&self) -> Option<bool> {
        self.extra.is_connect_failed()
    }

    /// Tells if this event is the result of a connection failure.
    ///
    /// This function checks if an error exist, particularly useful in detecting if TCP connection failed. It corresponds to the `EPOLLERR` event in Linux
    /// and `CONNECT_FAILED` event in Windows IOCP.
    ///
    /// ## Caveats
    ///
    /// In `epoll`, a TCP connection failure is indicated by `EPOLLERR` + `EPOLLHUP`, though just `EPOLLERR` is enough to indicate a connection failure.
    /// EPOLLHUP may happen when we haven't event called `connect` on the socket, but it is still a valid event to check for.
    ///
    /// Returns `Some(true)` if the connection has failed, `Some(false)` if there is no error,
    /// or `None` if the platform does not support detecting this condition.
    #[inline]
    pub fn is_err(&self) -> Option<bool> {
        self.extra.is_err()
    }

    /// Remove any extra information from this event.
    #[inline]
    pub fn clear_extra(&mut self) {
        self.extra = sys::EventExtra::empty();
    }

    /// Get a version of this event with no extra information.
    ///
    /// This is useful for comparing events with `==`.
    #[inline]
    pub fn with_no_extra(mut self) -> Self {
        self.clear_extra();
        self
    }
}

/// Waits for I/O events.
///
/// The poller is [`Send`] but not [`Sync`], it is used by the thread that waits for I/O
/// events. Use a [`Notifier`] to wake it up from other threads.
pub struct Poller {
    poller: sys::Poller,
    notifier: Notifier,

    /// The poller is used by a single thread, which lets the backends avoid locking.
    _not_sync: PhantomData<Cell<()>>,
}

impl Poller {
    /// Creates a new poller.
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::Poller;
    ///
    /// let poller = Poller::new()?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn new() -> io::Result<Poller> {
        let poller = sys::Poller::new()?;
        let notifier = Notifier(Arc::new(NotifierInner {
            notified: AtomicBool::new(false),
            waker: poller.waker(),
        }));

        Ok(Poller {
            poller,
            notifier,
            _not_sync: PhantomData,
        })
    }

    /// Tell whether or not this `Poller` supports level-triggered polling.
    pub fn supports_level(&self) -> bool {
        self.poller.supports_level()
    }

    /// Tell whether or not this `Poller` supports edge-triggered polling.
    pub fn supports_edge(&self) -> bool {
        self.poller.supports_edge()
    }

    /// Adds a file descriptor or socket to the poller.
    ///
    /// A file descriptor or socket is considered readable or writable when a read or write
    /// operation on it would not block. This doesn't mean the read or write operation will
    /// succeed, it only means the operation will return immediately.
    ///
    /// If interest is set in both readability and writability, the two kinds of events might be
    /// delivered either separately or together.
    ///
    /// For example, interest in `Event { key: 7, readable: true, writable: true }` might result in
    /// a single [`Event`] of the same form, or in two separate [`Event`]s:
    /// - `Event { key: 7, readable: true, writable: false }`
    /// - `Event { key: 7, readable: false, writable: true }`
    ///
    /// Note that interest in I/O events needs to be re-enabled using
    /// [`modify()`][`Poller::modify()`] again after an event is delivered if we're interested in
    /// the next event of the same kind.
    ///
    /// It is possible to register interest in the same file descriptor or socket using multiple
    /// separate [`Poller`] instances. When the event is delivered, one or more [`Poller`]s are
    /// notified with that event. The exact number of [`Poller`]s notified depends on the
    /// underlying platform. When registering multiple sources into one event, the user should
    /// be careful to accommodate for events lost to other pollers.
    ///
    /// One may also register one source into other, non-`polling` event loops, like GLib's
    /// context. While the plumbing will vary from platform to platform, in general the [`Poller`]
    /// will act as if the source was registered with another [`Poller`], with the same caveats
    /// as above.
    ///
    /// # Safety
    ///
    /// The source must be [`delete()`]d from this `Poller` before it is dropped.
    ///
    /// [`delete()`]: Poller::delete
    ///
    /// # Errors
    ///
    /// This method returns an error in the following situations:
    ///
    /// * If `key` equals `usize::MAX` because that key is reserved for internal use.
    /// * If an error is returned by the syscall.
    ///
    /// # Examples
    ///
    /// Set interest in all events:
    ///
    /// ```no_run
    /// use ntex_polling::{Event, Poller};
    ///
    /// let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// source.set_nonblocking(true)?;
    /// let key = 7;
    ///
    /// let poller = Poller::new()?;
    /// unsafe {
    ///     poller.add(&source, Event::all(key))?;
    /// }
    /// poller.delete(&source)?;
    /// # std::io::Result::Ok(())
    /// ```
    pub unsafe fn add(&self, source: impl AsRawSource, interest: Event) -> io::Result<()> {
        self.add_with_mode(source, interest, PollMode::Oneshot)
    }

    /// Adds a file descriptor or socket to the poller in the specified mode.
    ///
    /// This is identical to the `add()` function, but allows specifying the
    /// polling mode to use for this socket.
    ///
    /// # Safety
    ///
    /// The source must be [`delete()`]d from this `Poller` before it is dropped.
    ///
    /// [`delete()`]: Poller::delete
    ///
    /// # Errors
    ///
    /// If the operating system does not support the specified mode, this function
    /// will return an error.
    pub unsafe fn add_with_mode(
        &self,
        source: impl AsRawSource,
        interest: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        if interest.key == NOTIFY_KEY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the key is not allowed to be `usize::MAX`",
            ));
        }
        self.poller.add(source.raw(), interest, mode)
    }

    /// Modifies the interest in a file descriptor or socket.
    ///
    /// This method has the same behavior as [`add()`][`Poller::add()`] except it modifies the
    /// interest of a previously added file descriptor or socket.
    ///
    /// To use this method with a file descriptor or socket, you must first add it using
    /// [`add()`][`Poller::add()`].
    ///
    /// Note that interest in I/O events needs to be re-enabled using
    /// [`modify()`][`Poller::modify()`] again after an event is delivered if we're interested in
    /// the next event of the same kind.
    ///
    /// # Errors
    ///
    /// This method returns an error in the following situations:
    ///
    /// * If `key` equals `usize::MAX` because that key is reserved for internal use.
    /// * If an error is returned by the syscall.
    ///
    /// # Examples
    ///
    /// To enable interest in all events:
    ///
    /// ```no_run
    /// # use ntex_polling::{Event, Poller};
    /// # let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// # let key = 7;
    /// # let poller = Poller::new()?;
    /// # unsafe { poller.add(&source, Event::none(key))?; }
    /// poller.modify(&source, Event::all(key))?;
    /// # std::io::Result::Ok(())
    /// ```
    ///
    /// To enable interest in readable events and disable interest in writable events:
    ///
    /// ```no_run
    /// # use ntex_polling::{Event, Poller};
    /// # let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// # let key = 7;
    /// # let poller = Poller::new()?;
    /// # unsafe { poller.add(&source, Event::none(key))?; }
    /// poller.modify(&source, Event::readable(key))?;
    /// # poller.delete(&source)?;
    /// # std::io::Result::Ok(())
    /// ```
    ///
    /// To disable interest in readable events and enable interest in writable events:
    ///
    /// ```no_run
    /// # use ntex_polling::{Event, Poller};
    /// # let poller = Poller::new()?;
    /// # let key = 7;
    /// # let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// # unsafe { poller.add(&source, Event::none(key))? };
    /// poller.modify(&source, Event::writable(key))?;
    /// # poller.delete(&source)?;
    /// # std::io::Result::Ok(())
    /// ```
    ///
    /// To disable interest in all events:
    ///
    /// ```no_run
    /// # use ntex_polling::{Event, Poller};
    /// # let source = std::net::TcpListener::bind("127.0.0.1:0")?;
    /// # let key = 7;
    /// # let poller = Poller::new()?;
    /// # unsafe { poller.add(&source, Event::none(key))?; }
    /// poller.modify(&source, Event::none(key))?;
    /// # poller.delete(&source)?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn modify(&self, source: impl AsSource, interest: Event) -> io::Result<()> {
        self.modify_with_mode(source, interest, PollMode::Oneshot)
    }

    /// Modifies interest in a file descriptor or socket to the poller, but with the specified
    /// mode.
    ///
    /// This is identical to the `modify()` function, but allows specifying the polling mode
    /// to use for this socket.
    ///
    /// # Performance Notes
    ///
    /// This function can be used to change a source from one polling mode to another. However,
    /// on some platforms, this switch can cause delays in the delivery of events.
    ///
    /// # Errors
    ///
    /// If the operating system does not support the specified mode, this function will return
    /// an error.
    pub fn modify_with_mode(
        &self,
        source: impl AsSource,
        interest: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        if interest.key == NOTIFY_KEY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the key is not allowed to be `usize::MAX`",
            ));
        }
        self.poller.modify(source.source(), interest, mode)
    }

    /// Modifies interest in a file descriptor or socket, possibly deferring the change to the
    /// next wait.
    ///
    /// This is identical to [`modify()`][`Poller::modify()`], except that on kqueue the change
    /// is queued and submitted by the next [`wait()`][`Poller::wait()`] together with its
    /// `kevent` system call, rather than costing a system call of its own. Other platforms
    /// apply the change immediately.
    ///
    /// See [`modify_with_mode_deferred()`][`Poller::modify_with_mode_deferred()`] for details.
    pub fn modify_deferred(&self, source: impl AsSource, interest: Event) -> io::Result<()> {
        self.modify_with_mode_deferred(source, interest, PollMode::Oneshot)
    }

    /// Modifies interest in a file descriptor or socket with the specified mode, possibly
    /// deferring the change to the next wait.
    ///
    /// This is identical to [`modify_with_mode()`][`Poller::modify_with_mode()`], except that
    /// on kqueue the change is queued and submitted by the next [`wait()`][`Poller::wait()`]
    /// together with its `kevent` system call. Other platforms apply the change immediately.
    ///
    /// A queued change takes effect when the next wait starts.
    ///
    /// On kqueue a filter that is not wanted is disabled rather than deleted, and keeps the mode
    /// it was created with, so a source should keep using the same mode.
    ///
    /// # Errors
    ///
    /// Only errors detected before the change is queued are returned, such as an unknown
    /// source. If the kernel rejects a queued change, the source is reported as both readable
    /// and writable, so that the failure surfaces on its next read or write.
    pub fn modify_with_mode_deferred(
        &self,
        source: impl AsSource,
        interest: Event,
        mode: PollMode,
    ) -> io::Result<()> {
        if interest.key == NOTIFY_KEY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the key is not allowed to be `usize::MAX`",
            ));
        }
        self.poller.modify_deferred(source.source(), interest, mode)
    }

    /// Removes a file descriptor or socket from the poller.
    ///
    /// Unlike [`add()`][`Poller::add()`], this method only removes the file descriptor or
    /// socket from the poller without putting it back into blocking mode.
    ///
    /// If the given source is not known to the poller then this is guaranteed to fail
    /// with [`std::io::ErrorKind::NotFound`]. That particular error kind is never
    /// returned in any other situation.
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::{Event, Poller};
    /// use std::net::TcpListener;
    ///
    /// let socket = TcpListener::bind("127.0.0.1:0")?;
    /// socket.set_nonblocking(true)?;
    /// let key = 7;
    ///
    /// let poller = Poller::new()?;
    /// unsafe { poller.add(&socket, Event::all(key))?; }
    /// poller.delete(&socket)?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn delete(&self, source: impl AsSource) -> io::Result<()> {
        self.poller.delete(source.source())
    }

    /// Waits for at least one I/O event and returns the number of new events.
    ///
    /// New events will be appended to `events`, which grows if it has no room for them. If
    /// necessary, make sure to clear the [`Events`][Events::clear()] before calling
    /// [`wait()`][`Poller::wait()`]!
    ///
    /// This method will return with no new events if a notification is delivered by the
    /// [`notify()`] method or a [`Notifier`], or the timeout is reached. Sometimes it may even
    /// return with no events spuriously.
    ///
    /// If the operating system is ready to deliver a large number of events at once, this method
    /// may decide to deliver them in smaller batches.
    ///
    /// [`notify()`]: `Poller::notify()`
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::{Event, Events, Poller};
    /// use std::net::TcpListener;
    /// use std::time::Duration;
    ///
    /// let socket = TcpListener::bind("127.0.0.1:0")?;
    /// socket.set_nonblocking(true)?;
    /// let key = 7;
    ///
    /// let poller = Poller::new()?;
    /// unsafe {
    ///     poller.add(&socket, Event::all(key))?;
    /// }
    ///
    /// let mut events = Events::new();
    /// let n = poller.wait(&mut events, Some(Duration::from_secs(1)))?;
    /// poller.delete(&socket)?;
    /// # std::io::Result::Ok(())
    /// ```
    pub fn wait(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize> {
        self.wait_impl(
            events,
            timeout.and_then(|timeout| Instant::now().checked_add(timeout)),
        )
    }

    /// Waits for at least one I/O event and returns the number of new events, with a deadline.
    ///
    /// See [`wait()`][`Poller::wait()`] for more details.
    pub fn wait_deadline(&self, events: &mut Events, deadline: Instant) -> io::Result<usize> {
        self.wait_impl(events, Some(deadline))
    }

    fn wait_impl(&self, events: &mut Events, deadline: Option<Instant>) -> io::Result<usize> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!("Poller::wait", ?deadline);
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        let before = events.len();

        loop {
            // Wait for I/O events.
            if let Err(e) = self.poller.wait_deadline(&mut events.events, deadline) {
                // If the wait was interrupted by a signal, try again. A failed wait does not
                // append events, so events from earlier calls are kept.
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                } else {
                    return Err(e);
                }
            }

            // Clear the notification, if any.
            self.notifier.0.notified.swap(false, Ordering::SeqCst);

            // Indicate number of new events.
            return Ok(events.len() - before);
        }
    }

    /// Returns a [`Notifier`] that wakes up this poller from any thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::{Events, Poller};
    /// use std::thread;
    ///
    /// let poller = Poller::new()?;
    /// let notifier = poller.notifier();
    ///
    /// // Notify the poller from another thread.
    /// thread::spawn(move || notifier.notify().unwrap()).join().unwrap();
    ///
    /// let mut events = Events::new();
    /// poller.wait(&mut events, None)?; // wakes up immediately
    /// assert!(events.is_empty());
    /// # std::io::Result::Ok(())
    /// ```
    pub fn notifier(&self) -> Notifier {
        self.notifier.clone()
    }

    /// Wakes up the current or the following invocation of [`wait()`].
    ///
    /// If the poller is not waiting right now, this method will cause the following call
    /// to [`wait()`] to wake up immediately.
    ///
    /// To wake up the poller from another thread, use a [`Notifier`].
    ///
    /// [`wait()`]: `Poller::wait()`
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::{Events, Poller};
    ///
    /// let poller = Poller::new()?;
    ///
    /// // Notify the poller.
    /// poller.notify()?;
    ///
    /// let mut events = Events::new();
    /// poller.wait(&mut events, None)?; // wakes up immediately
    /// assert!(events.is_empty());
    /// # std::io::Result::Ok(())
    /// ```
    pub fn notify(&self) -> io::Result<()> {
        self.notifier.notify()
    }
}

/// Wakes up a [`Poller`], can be used from any thread.
///
/// A notifier is created by [`Poller::notifier()`], it can be cloned and sent to other threads.
/// It stays valid after the poller is dropped, notifications are ignored then.
#[derive(Clone)]
pub struct Notifier(Arc<NotifierInner>);

struct NotifierInner {
    /// Whether a notification is pending, avoids waking up the poller more than once.
    notified: AtomicBool,
    waker: sys::Waker,
}

impl Notifier {
    /// Wakes up the current or the following invocation of [`Poller::wait()`].
    ///
    /// If the poller is not waiting right now, this method will cause the following call
    /// to [`wait()`][`Poller::wait()`] to wake up immediately.
    pub fn notify(&self) -> io::Result<()> {
        #[cfg(feature = "tracing")]
        let span = tracing::trace_span!("Notifier::notify");
        #[cfg(feature = "tracing")]
        let _enter = span.enter();

        if self
            .0
            .notified
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.0.waker.notify()?;
        }
        Ok(())
    }
}

impl fmt::Debug for Notifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notifier")
            .field("notified", &self.0.notified)
            .field("waker", &self.0.waker)
            .finish()
    }
}

/// A container for I/O events.
pub struct Events {
    events: sys::Events,

    /// This is intended to be used from &mut, thread locally, so we should make it !Sync
    /// for consistency with the rest of the API.
    _not_sync: PhantomData<Cell<()>>,
}

impl Default for Events {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Events {
    /// Create a new container for events, using the default capacity.
    ///
    /// The default capacity is 1024.
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::Events;
    ///
    /// let events = Events::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        // ESP-IDF has a low amount of RAM, so we use a smaller default capacity.
        #[cfg(target_os = "espidf")]
        const DEFAULT_CAPACITY: usize = 32;

        #[cfg(not(target_os = "espidf"))]
        const DEFAULT_CAPACITY: usize = 1024;

        Self::with_capacity(NonZeroUsize::new(DEFAULT_CAPACITY).unwrap())
    }

    /// Create a new container with the provided capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::Events;
    /// use std::num::NonZeroUsize;
    ///
    /// let capacity = NonZeroUsize::new(1024).unwrap();
    /// let events = Events::with_capacity(capacity);
    /// ```
    #[inline]
    pub fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            events: sys::Events::with_capacity(capacity.get()),
            _not_sync: PhantomData,
        }
    }

    /// Create a new iterator over I/O events.
    ///
    /// This returns all of the events in the container, excluding the notification event.
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::{Event, Events, Poller};
    /// use std::time::Duration;
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let poller = Poller::new()?;
    /// let mut events = Events::new();
    ///
    /// poller.wait(&mut events, Some(Duration::from_secs(0)))?;
    /// assert!(events.iter().next().is_none());
    /// # Ok(()) }
    /// ```
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.events.iter().filter(|ev| ev.key != NOTIFY_KEY)
    }

    /// Delete all of the events in the container.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ntex_polling::{Event, Events, Poller};
    ///
    /// # fn main() -> std::io::Result<()> {
    /// let poller = Poller::new()?;
    /// let mut events = Events::new();
    ///
    /// /* register some sources */
    ///
    /// poller.wait(&mut events, None)?;
    ///
    /// events.clear();
    /// # Ok(()) }
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        self.events.clear();
    }

    /// Returns the number of events in the container.
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::Events;
    ///
    /// let events = Events::new();
    /// assert_eq!(events.len(), 0);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Returns `true` if the container contains no events.
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::Events;
    ///
    /// let events = Events::new();
    /// assert!(events.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the total capacity of the list.
    ///
    /// # Examples
    ///
    /// ```
    /// use ntex_polling::Events;
    /// use std::num::NonZeroUsize;
    ///
    /// let cap = NonZeroUsize::new(10).unwrap();
    /// let events = Events::with_capacity(std::num::NonZeroUsize::new(10).unwrap());
    /// assert_eq!(events.capacity(), cap);
    /// ```
    #[inline]
    pub fn capacity(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.events.capacity()).unwrap()
    }
}

impl fmt::Debug for Events {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Events { .. }")
    }
}

#[cfg(all(
    any(
        target_os = "linux",
        target_os = "android",
        target_os = "redox",
        target_os = "illumos",
        target_os = "solaris",
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ),
    not(polling_test_poll_backend),
))]
#[cfg_attr(
    docsrs,
    doc(cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "redox",
        target_os = "illumos",
        target_os = "solaris",
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    )))
)]
mod raw_fd_impl {
    use crate::Poller;
    use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, RawFd};

    impl AsRawFd for Poller {
        fn as_raw_fd(&self) -> RawFd {
            self.poller.as_raw_fd()
        }
    }

    impl AsFd for Poller {
        fn as_fd(&self) -> BorrowedFd<'_> {
            self.poller.as_fd()
        }
    }
}

#[cfg(windows)]
#[cfg_attr(docsrs, doc(cfg(windows)))]
mod raw_handle_impl {
    use crate::Poller;
    use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, RawHandle};

    impl AsRawHandle for Poller {
        fn as_raw_handle(&self) -> RawHandle {
            self.poller.as_raw_handle()
        }
    }

    impl AsHandle for Poller {
        fn as_handle(&self) -> BorrowedHandle<'_> {
            self.poller.as_handle()
        }
    }
}

impl fmt::Debug for Poller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.poller.fmt(f)
    }
}

cfg_if! {
    if #[cfg(any(unix, target_os = "hermit"))] {
        #[cfg(unix)]
        use std::os::unix::io::{AsRawFd, RawFd, AsFd, BorrowedFd};
        #[cfg(target_os = "hermit")]
        use std::os::hermit::io::{AsRawFd, RawFd, AsFd, BorrowedFd};

        /// A resource with a raw file descriptor.
        pub trait AsRawSource {
            /// Returns the raw file descriptor.
            fn raw(&self) -> RawFd;
        }

        impl<T: AsRawFd> AsRawSource for &T {
            fn raw(&self) -> RawFd {
                self.as_raw_fd()
            }
        }

        impl AsRawSource for RawFd {
            fn raw(&self) -> RawFd {
                *self
            }
        }

        /// A resource with a borrowed file descriptor.
        pub trait AsSource: AsFd {
            /// Returns the borrowed file descriptor.
            fn source(&self) -> BorrowedFd<'_> {
                self.as_fd()
            }
        }

        impl<T: AsFd> AsSource for T {}
    } else if #[cfg(windows)] {
        use std::os::windows::io::{AsRawSocket, RawSocket, AsSocket, BorrowedSocket};

        /// A resource with a raw socket.
        pub trait AsRawSource {
            /// Returns the raw socket.
            fn raw(&self) -> RawSocket;
        }

        impl<T: AsRawSocket> AsRawSource for &T {
            fn raw(&self) -> RawSocket {
                self.as_raw_socket()
            }
        }

        impl AsRawSource for RawSocket {
            fn raw(&self) -> RawSocket {
                *self
            }
        }

        /// A resource with a borrowed socket.
        pub trait AsSource: AsSocket {
            /// Returns the borrowed socket.
            fn source(&self) -> BorrowedSocket<'_> {
                self.as_socket()
            }
        }

        impl<T: AsSocket> AsSource for T {}
    }
}

#[allow(unused)]
fn unsupported_error(err: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, err.into())
}

fn _assert_send_and_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<Poller>();
    // Poller is !Sync

    assert_send::<Notifier>();
    assert_sync::<Notifier>();

    assert_send::<Event>();
    assert_sync::<Event>();

    assert_send::<Events>();
    // Events can be !Sync
}
