//! kqueue changes are submitted without heap allocations.
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
use std::os::unix::net::UnixStream;
use std::time::Duration;

use ntex_polling::{Event, Events, Poller};

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
