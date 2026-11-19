use std::{
    io,
    sync::{Arc, Mutex},
};
use ticklog::{FanOut, Level, LogSink, LogSinkExt, try_info, try_log_bytes};

#[derive(Debug, PartialEq, Eq)]
enum Record {
    Line(Vec<u8>),
    Bytes(u16, Vec<u8>, Level),
}

struct Capture(Arc<Mutex<Vec<Record>>>);

impl LogSink for Capture {
    fn accept(&mut self, line: &[u8], _: Level) -> io::Result<()> {
        self.0.lock().unwrap().push(Record::Line(line.to_vec()));
        Ok(())
    }

    fn accept_bytes(&mut self, kind: u16, payload: &[u8], level: Level) -> io::Result<()> {
        self.0
            .lock()
            .unwrap()
            .push(Record::Bytes(kind, payload.to_vec(), level));
        Ok(())
    }
}

#[test]
fn tagged_bytes_and_text_share_fifo_through_shutdown() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let guard = ticklog::configure! {
        sink: FanOut::new().add(Capture(records.clone()).with_max_level(Level::Info)),
        format: "{message}",
    }
    .unwrap();
    ticklog::warm_up().unwrap();

    assert!(try_info!("before {}", 1u64));
    let mut first = [0xff, 0];
    assert!(try_log_bytes(Level::Warn, 0x1248, &[&first, b"\n"]));
    first.fill(b'x');
    assert!(try_log_bytes(Level::Debug, 2, &[b"filtered"]));
    let oversized = vec![0; u16::MAX as usize];
    assert!(!try_log_bytes(Level::Info, 3, &[&oversized]));
    assert!(try_info!("after"));
    assert!(try_log_bytes(Level::Info, 4, &[]));
    drop(guard);

    assert_eq!(
        records.lock().unwrap().as_slice(),
        [
            Record::Line(b"before 1".to_vec()),
            Record::Bytes(0x1248, vec![0xff, 0, b'\n'], Level::Warn),
            Record::Line(b"after".to_vec()),
            Record::Bytes(4, vec![], Level::Info),
        ]
    );
}
