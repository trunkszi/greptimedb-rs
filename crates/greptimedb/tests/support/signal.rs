//! Byte notifications over the same completion-driven pipe as protocol fixtures.
#![allow(dead_code)]

use grpc::core::transport::{MemoryPipe, SyncTransport};
use std::{
    cell::RefCell,
    io::{self, Read, Write},
    time::Duration,
};

pub struct Sender(RefCell<MemoryPipe>);
pub struct Receiver(RefCell<MemoryPipe>);

pub fn channel() -> (Sender, Receiver) {
    let (sender, receiver) = MemoryPipe::pair();
    (
        Sender(RefCell::new(sender)),
        Receiver(RefCell::new(receiver)),
    )
}

impl Sender {
    /// Sends one byte notification.
    ///
    /// # Errors
    /// Returns pipe closure, timeout, or ring errors.
    pub fn send(&self, value: u8) -> io::Result<()> {
        let mut pipe = self.0.borrow_mut();
        pipe.set_write_timeout(Some(Duration::from_secs(5)))?;
        pipe.write_all(&[value])
    }
}

impl Receiver {
    /// Receives a notification within the supplied budget.
    ///
    /// # Errors
    /// Returns pipe closure, timeout, or ring errors.
    pub fn recv_timeout(&self, timeout: Duration) -> io::Result<u8> {
        let mut pipe = self.0.borrow_mut();
        pipe.set_read_timeout(Some(timeout))?;
        let mut byte = [0];
        pipe.read_exact(&mut byte)?;
        Ok(byte[0])
    }

    /// Receives a queued notification without waiting.
    ///
    /// # Errors
    /// Returns WouldBlock if no notification is queued, or a pipe failure.
    pub fn try_recv(&self) -> io::Result<u8> {
        let mut pipe = self.0.borrow_mut();
        pipe.set_nonblocking(true)?;
        let mut byte = [0];
        let result = pipe.read_exact(&mut byte).map(|()| byte[0]);
        pipe.set_nonblocking(false)?;
        result
    }
}
