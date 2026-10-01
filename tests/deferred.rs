#![cfg(unix)]

use std::io::{self, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use ntex_polling::{Event, Events, PollMode, Poller};

fn pair() -> io::Result<(UnixStream, UnixStream)> {
    let (a, b) = UnixStream::pair()?;
    a.set_nonblocking(true)?;
    b.set_nonblocking(true)?;
    Ok((a, b))
}

fn wait(poller: &Poller, timeout: Duration) -> io::Result<Vec<Event>> {
    let mut events = Events::new();
    poller.wait(&mut events, Some(timeout))?;
    Ok(events.iter().collect())
}

#[test]
fn deferred_oneshot() -> io::Result<()> {
    let poller = Poller::new()?;
    let (reader, mut writer) = pair()?;
    unsafe { poller.add(&reader, Event::none(1))? };

    writer.write_all(&[1])?;
    assert!(wait(&poller, Duration::ZERO)?.is_empty());

    poller.modify_deferred(&reader, Event::readable(1))?;
    let events = wait(&poller, Duration::from_secs(1))?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key, 1);
    assert!(events[0].readable);

    // Oneshot interest is disarmed after the event.
    assert!(wait(&poller, Duration::ZERO)?.is_empty());

    poller.delete(&reader)?;
    Ok(())
}

#[test]
fn deferred_level() -> io::Result<()> {
    let poller = Poller::new()?;
    if !poller.supports_level() {
        return Ok(());
    }
    let (reader, mut writer) = pair()?;
    unsafe { poller.add_with_mode(&reader, Event::readable(2), PollMode::Level)? };

    writer.write_all(&[1])?;
    for _ in 0..2 {
        let events = wait(&poller, Duration::from_secs(1))?;
        assert_eq!(events.len(), 1);
        assert!(events[0].readable);
    }

    // Disabling interest stops reporting, re-enabling resumes it.
    poller.modify_with_mode_deferred(&reader, Event::none(2), PollMode::Level)?;
    assert!(wait(&poller, Duration::ZERO)?.is_empty());

    poller.modify_with_mode_deferred(&reader, Event::readable(3), PollMode::Level)?;
    let events = wait(&poller, Duration::from_secs(1))?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key, 3);
    assert!(events[0].readable);

    poller.delete(&reader)?;
    Ok(())
}

#[test]
fn deferred_then_delete() -> io::Result<()> {
    let poller = Poller::new()?;
    let (reader, mut writer) = pair()?;
    unsafe { poller.add(&reader, Event::none(4))? };

    writer.write_all(&[1])?;
    poller.modify_deferred(&reader, Event::readable(4))?;
    poller.delete(&reader)?;
    assert!(wait(&poller, Duration::ZERO)?.is_empty());

    // The source can be added again.
    unsafe { poller.add(&reader, Event::readable(5))? };
    let events = wait(&poller, Duration::from_secs(1))?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].key, 5);

    poller.delete(&reader)?;
    Ok(())
}

#[test]
fn deferred_errors() -> io::Result<()> {
    let poller = Poller::new()?;
    let (reader, _writer) = pair()?;

    let err = poller
        .modify_deferred(&reader, Event::readable(6))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    unsafe { poller.add(&reader, Event::none(6))? };
    let err = poller
        .modify_deferred(&reader, Event::readable(usize::MAX))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

    poller.delete(&reader)?;
    Ok(())
}

#[test]
fn notify_wakes_up() -> io::Result<()> {
    let poller = Poller::new()?;
    for _ in 0..3 {
        poller.notify()?;
        assert!(wait(&poller, Duration::from_secs(5))?.is_empty());
    }

    // A positive timeout expires without notification.
    let start = std::time::Instant::now();
    assert!(wait(&poller, Duration::from_millis(20))?.is_empty());
    assert!(start.elapsed() >= Duration::from_millis(20));

    // A zero timeout after a positive one does not block.
    let start = std::time::Instant::now();
    assert!(wait(&poller, Duration::ZERO)?.is_empty());
    assert!(start.elapsed() < Duration::from_secs(1));
    Ok(())
}
