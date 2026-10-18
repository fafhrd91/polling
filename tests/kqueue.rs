//! kqueue specific behavior.
#![cfg(all(
    any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly",
    ),
    not(polling_test_poll_backend)
))]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io;
use std::num::NonZeroI32;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::Duration;

use ntex_polling::os::kqueue::{PollerKqueueExt, Process, ProcessOps};
use ntex_polling::{Event, Events, PollMode, Poller};

struct Counting;

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.with(|c| c.set(c.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[test]
fn notify_and_modify_do_not_allocate() -> io::Result<()> {
    let poller = Poller::new()?;
    let notifier = poller.notifier();
    let (sock, _peer) = UnixStream::pair()?;
    unsafe { poller.add(&sock, Event::none(1))? };
    let mut events = Events::with_capacity(8.try_into().unwrap());

    let before = ALLOCS.with(Cell::get);
    for i in 0..100 {
        notifier.notify()?;
        poller.modify(&sock, Event::new(1, i % 2 == 0, true))?;
        poller.wait(&mut events, Some(Duration::ZERO))?;
        events.clear();
    }
    assert_eq!(ALLOCS.with(Cell::get) - before, 0);

    poller.delete(&sock)
}

#[test]
fn failed_add_is_not_registered() -> io::Result<()> {
    let poller = Poller::new()?;
    let other = Poller::new()?;

    // a kqueue can't be added to itself
    let fd = poller.as_raw_fd();
    let err = unsafe { poller.add(fd, Event::readable(1)) }.unwrap_err();
    assert_ne!(err.kind(), io::ErrorKind::AlreadyExists);
    let retry = unsafe { poller.add(fd, Event::readable(1)) }.unwrap_err();
    assert_eq!(retry.raw_os_error(), err.raw_os_error());

    // the fd is still registrable with another poller
    unsafe { other.add(fd, Event::readable(1))? };
    other.delete(&poller)
}

#[test]
fn failed_add_filter_is_not_registered() -> io::Result<()> {
    let poller = Poller::new()?;
    let pid = NonZeroI32::new(i32::MAX).unwrap();
    let process = unsafe { Process::from_pid(pid, ProcessOps::Exit) };

    let err = poller
        .add_filter(&process, 1, PollMode::Oneshot)
        .unwrap_err();
    assert_ne!(err.kind(), io::ErrorKind::AlreadyExists);
    let retry = poller
        .add_filter(&process, 1, PollMode::Oneshot)
        .unwrap_err();
    assert_eq!(retry.raw_os_error(), err.raw_os_error());

    let err = poller
        .modify_filter(&process, 1, PollMode::Oneshot)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
    Ok(())
}
