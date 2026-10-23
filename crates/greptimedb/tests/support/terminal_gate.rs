//! Force a peer deschedule after terminal headers are visible to its client.

use grpc::core::transport::{MemoryPipe, SyncTransport};
use std::{
    io::{self, Read, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

pub struct TerminalGate {
    pipe: MemoryPipe,
    stopping: Arc<AtomicBool>,
    observed: Arc<AtomicBool>,
    enabled: bool,
}
impl TerminalGate {
    pub fn new(
        pipe: MemoryPipe,
        stopping: Arc<AtomicBool>,
        observed: Arc<AtomicBool>,
        enabled: bool,
    ) -> Self {
        Self {
            pipe,
            stopping,
            observed,
            enabled,
        }
    }
}
impl Read for TerminalGate {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.pipe.read(bytes)
    }
}
impl Write for TerminalGate {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // Controlled HTTP/2 fixture headers fit in one write. Type 1 with the
        // END_STREAM bit publishes trailers before server-side reset cleanup.
        let terminal = self.enabled && bytes.len() >= 9 && bytes[3] == 1 && bytes[4] & 1 != 0;
        if terminal {
            self.observed.store(true, Ordering::Release);
        }
        let result = self.pipe.write(bytes);
        if terminal && result.is_ok() {
            // The client can observe its exact structured failure while the
            // server has not yet written its final stream reset. Finish must
            // release this gate without first destroying the client endpoint.
            while !self.stopping.load(Ordering::Acquire) {
                thread::yield_now();
            }
        }
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        self.pipe.flush()
    }
}
impl SyncTransport for TerminalGate {
    fn set_nonblocking(&mut self, value: bool) -> io::Result<()> {
        self.pipe.set_nonblocking(value)
    }
    fn set_read_timeout(&mut self, value: Option<Duration>) -> io::Result<()> {
        self.pipe.set_read_timeout(value)
    }
    fn set_write_timeout(&mut self, value: Option<Duration>) -> io::Result<()> {
        self.pipe.set_write_timeout(value)
    }
    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        self.pipe.read_timeout()
    }
    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        self.pipe.write_timeout()
    }
    fn peer_endpoint(&self) -> io::Result<String> {
        self.pipe.peer_endpoint()
    }
}
