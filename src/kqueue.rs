//! Bindings to kqueue (macOS, iOS, FreeBSD, NetBSD, OpenBSD, DragonFly BSD).

use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::ptr;
use std::time::Duration;
use std::usize;

use crate::Event;

/// Interface to kqueue.
#[derive(Debug)]
pub struct Poller {
    /// File descriptor for the kqueue instance.
    kqueue_fd: RawFd,
    /// Read side of a pipe for consuming notifications.
    read_stream: UnixStream,
    /// Write side of a pipe for producing notifications.
    write_stream: UnixStream,
}

impl Poller {
    /// Creates a new poller.
    pub fn new() -> io::Result<Poller> {
        // Create a kqueue instance.
        let kqueue_fd = syscall!(kqueue())?;
        syscall!(fcntl(kqueue_fd, libc::F_SETFD, libc::FD_CLOEXEC))?;

        // Set up the notification pipe.
        let (read_stream, write_stream) = UnixStream::pair()?;
        read_stream.set_nonblocking(true)?;
        write_stream.set_nonblocking(true)?;
        let poller = Poller {
            kqueue_fd,
            read_stream,
            write_stream,
        };
        poller.interest(
            poller.read_stream.as_raw_fd(),
            Event {
                key: NOTIFY_KEY,
                readable: true,
                writable: false,
            },
        )?;

        Ok(poller)
    }

    /// Inserts a file descriptor.
    pub fn insert(&self, fd: RawFd) -> io::Result<()> {
        // Put the file descriptor in non-blocking mode.
        let flags = syscall!(fcntl(fd, libc::F_GETFL))?;
        syscall!(fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK))?;
        Ok(())
    }

    /// Sets interest in a read/write event on a file descriptor and associates a key with it.
    pub fn interest(&self, fd: RawFd, ev: Event) -> io::Result<()> {
        let mut read_flags = libc::EV_ONESHOT | libc::EV_RECEIPT;
        let mut write_flags = libc::EV_ONESHOT | libc::EV_RECEIPT;
        if ev.readable {
            read_flags |= libc::EV_ADD;
        } else {
            read_flags |= libc::EV_DELETE;
        }
        if ev.writable {
            write_flags |= libc::EV_ADD;
        } else {
            write_flags |= libc::EV_DELETE;
        }

        // A list of changes for kqueue.
        let changelist = [
            libc::kevent {
                ident: fd as _,
                filter: libc::EVFILT_READ,
                flags: read_flags,
                fflags: 0,
                data: 0,
                udata: ev.key as _,
            },
            libc::kevent {
                ident: fd as _,
                filter: libc::EVFILT_WRITE,
                flags: write_flags,
                fflags: 0,
                data: 0,
                udata: ev.key as _,
            },
        ];

        // Apply changes.
        let mut eventlist = changelist;
        syscall!(kevent(
            self.kqueue_fd,
            changelist.as_ptr() as *const libc::kevent,
            changelist.len() as _,
            eventlist.as_mut_ptr() as *mut libc::kevent,
            eventlist.len() as _,
            ptr::null(),
        ))?;

        // Check for errors.
        for ev in &eventlist {
            // Explanation for ignoring EPIPE: https://github.com/tokio-rs/mio/issues/582
            if (ev.flags & libc::EV_ERROR) != 0
                && ev.data != 0
                && ev.data != libc::ENOENT as _
                && ev.data != libc::EPIPE as _
            {
                return Err(io::Error::from_raw_os_error(ev.data as _));
            }
        }

        Ok(())
    }

    /// Removes a file descriptor.
    pub fn remove(&self, fd: RawFd) -> io::Result<()> {
        // A list of changes for kqueue.
        let changelist = [
            libc::kevent {
                ident: fd as _,
                filter: libc::EVFILT_READ,
                flags: libc::EV_DELETE | libc::EV_RECEIPT,
                fflags: 0,
                data: 0,
                udata: 0 as _,
            },
            libc::kevent {
                ident: fd as _,
                filter: libc::EVFILT_WRITE,
                flags: libc::EV_DELETE | libc::EV_RECEIPT,
                fflags: 0,
                data: 0,
                udata: 0 as _,
            },
        ];

        // Apply changes.
        let mut eventlist = changelist;
        syscall!(kevent(
            self.kqueue_fd,
            changelist.as_ptr() as *const libc::kevent,
            changelist.len() as _,
            eventlist.as_mut_ptr() as *mut libc::kevent,
            eventlist.len() as _,
            ptr::null(),
        ))?;

        // Check for errors.
        for ev in &eventlist {
            if (ev.flags & libc::EV_ERROR) != 0 && ev.data != 0 && ev.data != libc::ENOENT as _ {
                return Err(io::Error::from_raw_os_error(ev.data as _));
            }
        }

        Ok(())
    }

    /// Waits for I/O events with an optional timeout.
    ///
    /// Returns the number of processed I/O events.
    ///
    /// If a notification occurs, the notification event will be included in the `events` list
    /// identifiable by key `usize::MAX`.
    pub fn wait(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<usize> {
        // Convert the `Duration` to `libc::timespec`.
        let timeout = timeout.map(|t| libc::timespec {
            tv_sec: t.as_secs() as libc::time_t,
            tv_nsec: t.subsec_nanos() as libc::c_long,
        });

        // Wait for I/O events.
        let changelist = [];
        let eventlist = &mut events.list;
        let res = syscall!(kevent(
            self.kqueue_fd,
            changelist.as_ptr() as *const libc::kevent,
            changelist.len() as _,
            eventlist.as_mut_ptr() as *mut libc::kevent,
            eventlist.len() as _,
            match &timeout {
                None => ptr::null(),
                Some(t) => t,
            }
        ))?;
        events.len = res as usize;

        // Clear the notification (if received) and re-register interest in it.
        while (&self.read_stream).read(&mut [0; 64]).is_ok() {}
        self.interest(
            self.read_stream.as_raw_fd(),
            Event {
                key: NOTIFY_KEY,
                readable: true,
                writable: false,
            },
        )?;

        Ok(events.len)
    }

    /// Sends a notification to wake up the current or next `wait()` call.
    pub fn notify(&self) -> io::Result<()> {
        let _ = (&self.write_stream).write(&[1]);
        Ok(())
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        let _ = self.remove(self.read_stream.as_raw_fd());
        let _ = syscall!(close(self.kqueue_fd));
    }
}

/// Key associated with the pipe for producing notifications.
const NOTIFY_KEY: usize = usize::MAX;

/// A list of reported I/O events.
pub struct Events {
    list: Box<[libc::kevent]>,
    len: usize,
}

unsafe impl Send for Events {}

impl Events {
    /// Creates an empty list.
    pub fn new() -> Events {
        let ev = libc::kevent {
            ident: 0 as _,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: 0 as _,
        };
        let list = vec![ev; 1000].into_boxed_slice();
        let len = 0;
        Events { list, len }
    }

    /// Iterates over I/O events.
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        // On some platforms, closing the read end of a pipe wakes up writers, but the
        // event is reported as EVFILT_READ with the EV_EOF flag.
        //
        // https://github.com/golang/go/commit/23aad448b1e3f7c3b4ba2af90120bde91ac865b4
        self.list[..self.len].iter().map(|ev| Event {
            key: ev.udata as usize,
            readable: ev.filter == libc::EVFILT_READ,
            writable: ev.filter == libc::EVFILT_WRITE
                || (ev.filter == libc::EVFILT_READ && (ev.flags & libc::EV_EOF) != 0),
        })
    }
}
