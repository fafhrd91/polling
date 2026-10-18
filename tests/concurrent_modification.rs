#![cfg(all(unix, not(target_os = "vita")))]

use std::io::{self, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use easy_parallel::Parallel;
use ntex_polling::{Event, Events, Poller};

#[test]
fn concurrent_interruption() -> io::Result<()> {
    struct MakeItSend<T>(T);
    unsafe impl<T> Send for MakeItSend<T> {}

    let (reader, _writer) = tcp_pair()?;
    let poller = Poller::new()?;
    unsafe {
        poller.add(&reader, Event::none(0))?;
    }

    let mut events = Events::new();
    let events_borrow = &mut events;
    let (sender, receiver) = std::sync::mpsc::channel();

    Parallel::new()
        .add(move || {
            // Register a signal handler so that the syscall is actually interrupted. A signal that
            // is ignored by default does not cause an interrupted syscall.
            signal_hook::flag::register(signal_hook::consts::signal::SIGURG, Default::default())?;

            // Signal to the other thread how to send a signal to us
            sender
                .send(MakeItSend(unsafe { libc::pthread_self() }))
                .unwrap();

            poller.wait(events_borrow, Some(Duration::from_secs(1)))?;
            Ok(())
        })
        .add(move || {
            let MakeItSend(target_thread) = receiver.recv().unwrap();
            thread::sleep(Duration::from_millis(100));
            assert_eq!(0, unsafe {
                libc::pthread_kill(target_thread, libc::SIGURG)
            });
            Ok(())
        })
        .run()
        .into_iter()
        .collect::<io::Result<()>>()?;

    assert_eq!(events.len(), 0);

    Ok(())
}

#[test]
fn interruption_keeps_earlier_events() -> io::Result<()> {
    struct MakeItSend<T>(T);
    unsafe impl<T> Send for MakeItSend<T> {}

    let (reader1, mut writer1) = tcp_pair()?;
    let (reader2, mut writer2) = tcp_pair()?;
    let poller = Poller::new()?;
    unsafe {
        poller.add(&reader1, Event::readable(1))?;
        poller.add(&reader2, Event::readable(2))?;
    }

    let mut events = Events::new();
    writer1.write_all(&[1])?;
    assert_eq!(poller.wait(&mut events, Some(Duration::from_secs(1)))?, 1);

    let events_borrow = &mut events;
    let (sender, receiver) = std::sync::mpsc::channel();

    Parallel::new()
        .add(move || {
            signal_hook::flag::register(signal_hook::consts::signal::SIGURG, Default::default())?;
            sender
                .send(MakeItSend(unsafe { libc::pthread_self() }))
                .unwrap();

            // The wait is interrupted first, the event of `reader1` must be kept.
            assert_eq!(poller.wait(events_borrow, Some(Duration::from_secs(5)))?, 1);
            Ok(())
        })
        .add(move || {
            let MakeItSend(target_thread) = receiver.recv().unwrap();
            thread::sleep(Duration::from_millis(100));
            assert_eq!(0, unsafe {
                libc::pthread_kill(target_thread, libc::SIGURG)
            });
            thread::sleep(Duration::from_millis(100));
            writer2.write_all(&[1])
        })
        .run()
        .into_iter()
        .collect::<io::Result<()>>()?;

    let mut keys: Vec<_> = events.iter().map(|ev| ev.key).collect();
    keys.sort_unstable();
    assert_eq!(keys, [1, 2]);

    Ok(())
}

fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let a = TcpStream::connect(listener.local_addr()?)?;
    let (b, _) = listener.accept()?;
    Ok((a, b))
}
