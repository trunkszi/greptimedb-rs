//! Completion-driven dialing with owned operands and explicit retirement.

use grpc::core::transport::{TcpTransport, UnixTransport};
use liburing::{
    Completion, OperationId, Proactor, ProactorConfig, SubmissionContext, UringError,
    ops::{Timeout, net},
};
use socket2::{Domain, SockAddr, Socket, Type};
use std::{
    io,
    net::SocketAddr,
    os::fd::AsRawFd,
    path::Path,
    time::{Duration, Instant},
};

pub(super) const RING_ENTRIES: u32 = 64;

const CONNECT: OperationId = OperationId::from_raw((2 << 56) | 1);
const TIMER: OperationId = OperationId::from_raw((2 << 56) | 2);
const CANCEL: OperationId = OperationId::from_raw((2 << 56) | 3);

pub(super) fn error(error: UringError) -> io::Error {
    match error {
        UringError::RingInit(errno)
        | UringError::Submit(errno)
        | UringError::Wait(errno)
        | UringError::Syscall { errno, .. } => io::Error::from_raw_os_error(errno),
        UringError::SqThreadGone => io::Error::new(io::ErrorKind::BrokenPipe, error),
        UringError::OpcodeNotAdmitted { .. }
        | UringError::BlockingWaitUnavailable { .. }
        | UringError::CancellationUnavailable { .. } => {
            io::Error::new(io::ErrorKind::Unsupported, error)
        }
        other => io::Error::other(other),
    }
}

pub(super) fn ring() -> io::Result<Proactor> {
    Proactor::new(ProactorConfig {
        entries: RING_ENTRIES,
        staged_capacity: 256,
        max_cqe_batch: 64,
        ..ProactorConfig::default()
    })
    .map_err(error)
}

pub(super) fn remaining(deadline: Option<Instant>) -> io::Result<Option<Duration>> {
    deadline
        .map(|deadline| {
            let duration = deadline.saturating_duration_since(Instant::now());
            if duration.is_zero() {
                Err(io::ErrorKind::TimedOut.into())
            } else {
                Ok(duration)
            }
        })
        .transpose()
}

struct Attempt {
    ring: Option<Proactor>,
    socket: Option<Socket>,
    address: Option<Box<SockAddr>>,
    timer: Option<Timeout>,
    active: bool,
}

impl Drop for Attempt {
    fn drop(&mut self) {
        if self.active
            && self
                .ring
                .as_mut()
                .is_some_and(|ring| ring.shutdown().is_err())
        {
            // A failed retirement proof cannot release an address or descriptor
            // still referenced by the kernel. Quarantine the entire operation.
            if let Some(socket) = self.socket.take() {
                std::mem::forget(socket);
            }
            if let Some(address) = self.address.take() {
                std::mem::forget(address);
            }
            if let Some(timer) = self.timer.take() {
                std::mem::forget(timer);
            }
            if let Some(ring) = self.ring.take() {
                std::mem::forget(ring);
            }
        }
    }
}

fn connect(address: SockAddr, deadline: Option<Instant>) -> io::Result<Socket> {
    let mut attempt = Attempt {
        ring: Some(ring()?),
        socket: Some(Socket::new(address.domain(), Type::STREAM, None)?),
        address: Some(Box::new(address)),
        timer: remaining(deadline)?
            .map(Timeout::new)
            .transpose()
            .map_err(error)?,
        active: false,
    };
    let ring = attempt
        .ring
        .as_mut()
        .ok_or_else(|| io::Error::other("missing dial ring"))?;
    let socket = attempt
        .socket
        .as_ref()
        .ok_or_else(|| io::Error::other("missing dial socket"))?;
    let address = attempt
        .address
        .as_ref()
        .ok_or_else(|| io::Error::other("missing dial address"))?;
    attempt.active = true;
    ring.prepare_sqe_with(CONNECT, |sqe| {
        // SAFETY: The operation exclusively owns the socket and stable boxed
        // address until the connect CQE retires or ring shutdown proves retirement.
        unsafe {
            net::prep_connect(
                sqe,
                socket.as_raw_fd(),
                address.as_ptr().cast(),
                address.len(),
            )
        };
    })
    .map_err(error)?;
    if let Some(timer) = &attempt.timer {
        ring.prepare_sqe_with(TIMER, |sqe| {
            // SAFETY: Exclusive SQE; immutable timeout storage outlives its CQE.
            unsafe { timer.prepare(sqe) };
        })
        .map_err(error)?;
    }
    let mut connected = None;
    let mut timed = attempt.timer.is_none().then_some(0);
    let mut cancelling = false;
    let mut cancelled = true;
    while connected.is_none() || timed.is_none() || !cancelled {
        match ring.wait_completions(
            1,
            None,
            &mut |completion: Completion, _: &mut SubmissionContext<'_>| {
                match completion.operation_id {
                    CONNECT => connected = Some(completion.result),
                    TIMER => timed = Some(completion.result),
                    CANCEL => {
                        if completion.result < 0
                            && completion.result != -libc::ENOENT
                            && completion.result != -libc::EALREADY
                        {
                            return Err(UringError::syscalls(
                                "connect cancellation",
                                completion.result,
                            ));
                        }
                        cancelled = true;
                    }
                    other => return Err(UringError::InvalidPayload(other.raw())),
                }
                Ok(())
            },
        ) {
            Ok(_) => {}
            Err(UringError::Wait(libc::EINTR)) => continue,
            Err(failure) => return Err(error(failure)),
        }
        let target = if connected.is_some() && timed.is_none() {
            Some(TIMER)
        } else if connected.is_none() && timed.is_some() {
            Some(CONNECT)
        } else {
            None
        };
        if attempt.timer.is_some()
            && !cancelling
            && let Some(target) = target
        {
            ring.prepare_sqe_with(CANCEL, |sqe| {
                // SAFETY: Cancellation references only an operation key; all
                // target operands remain owned through their own final CQEs.
                unsafe { net::prep_cancel(sqe, target, 0) };
            })
            .map_err(error)?;
            cancelling = true;
            cancelled = false;
        }
    }
    attempt.active = false;
    let result = connected.ok_or_else(|| io::Error::other("missing connect completion"))?;
    if timed == Some(-libc::ETIME) {
        return Err(io::ErrorKind::TimedOut.into());
    }
    if let Some(timer_error) = timed.filter(|value| *value < 0 && *value != -libc::ECANCELED) {
        return Err(io::Error::from_raw_os_error(-timer_error));
    }
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }
    attempt
        .socket
        .take()
        .ok_or_else(|| io::Error::other("missing connected socket"))
}

pub(super) fn tcp(address: SocketAddr, deadline: Option<Instant>) -> io::Result<TcpTransport> {
    let socket = connect(address.into(), deadline)?;
    TcpTransport::from_std(socket.into())
}

pub(super) fn unix(path: &Path, deadline: Option<Instant>) -> io::Result<UnixTransport> {
    let address = SockAddr::unix(path)?;
    if address.domain() != Domain::UNIX {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let socket = connect(address, deadline)?;
    UnixTransport::from_std(socket.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_errors_map_io_kinds() {
        assert_eq!(
            error(UringError::SqThreadGone).kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            error(UringError::OpcodeNotAdmitted {
                opcode: 0,
                mode: "iopoll",
            })
            .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            error(UringError::BlockingWaitUnavailable { mode: "sqpoll" }).kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            error(UringError::CancellationUnavailable { mode: "sqpoll" }).kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(
            error(UringError::RingInit(libc::EINVAL)).raw_os_error(),
            Some(libc::EINVAL)
        );
        assert_eq!(
            error(UringError::CompletionsDropped).kind(),
            io::ErrorKind::Other
        );
    }

    #[test]
    fn lifecycle_unix_backlog_timeout_retires_connect_before_retry() -> io::Result<()> {
        let path = std::env::temp_dir().join(format!(
            "greptimedb-dial-{}-{:?}.sock",
            std::process::id(),
            std::thread::current().id()
        ));
        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        socket.bind(&SockAddr::unix(&path)?)?;
        socket.listen(0)?;
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                drop(std::fs::remove_file(&self.0));
            }
        }
        let _cleanup = Cleanup(path.clone());
        let listener: std::os::unix::net::UnixListener = socket.into();
        let first = unix(&path, Some(Instant::now() + Duration::from_secs(1)))?;
        let start = Instant::now();
        let error = match unix(&path, Some(start + Duration::from_millis(50))) {
            Ok(_) => return Err(io::Error::other("saturated backlog unexpectedly connected")),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(start.elapsed() < Duration::from_secs(1));
        let _accepted = listener.accept()?;
        let _fresh = unix(&path, Some(Instant::now() + Duration::from_secs(1)))?;
        drop(first);
        Ok(())
    }
}
