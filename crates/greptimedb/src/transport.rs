//! Connected byte transports for synchronous GreptimeDB channels.
//!
//! [`EndpointTransport`] unifies the streams provided by `grpc-core` so a
//! channel can be built from a TCP socket, a completed TLS session, a Unix
//! domain socket, or an in-memory pipe without naming the concrete type at the
//! call site.

use std::{
    io::{self, Read, Write},
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::path::Path;

#[cfg(feature = "tls")]
use grpc::core::tls::TlsTransport;
#[cfg(unix)]
use grpc::core::transport::UnixTransport;
use grpc::core::transport::{MemoryPipe, SyncTransport, TcpTransport};
mod dial;
mod dns;

/// A connected byte stream selected at construction time.
///
/// Every variant forwards [`Read`], [`Write`], and [`SyncTransport`], so
/// [`EndpointTransport::into_channel`] yields a channel independent of the
/// concrete socket type.
pub enum EndpointTransport {
    /// A plain TCP stream.
    Tcp(TcpTransport),
    /// A TLS session over TCP with HTTP/2 ALPN negotiation completed.
    #[cfg(feature = "tls")]
    Tls(Box<TlsTransport<rustls::ClientConnection, TcpTransport>>),
    /// A Unix domain stream.
    #[cfg(unix)]
    Unix(UnixTransport),
    /// A caller-provided in-memory pipe.
    Memory(MemoryPipe),
}

impl EndpointTransport {
    /// Connects a TCP transport.
    ///
    /// A connect timeout bounds DNS and all address attempts together. With
    /// `None`, only the resolver and operating system limits apply. Read and write timeouts are applied only when
    /// provided. `nodelay` is applied after connecting because the underlying
    /// transport enables `TCP_NODELAY` unconditionally.
    ///
    /// # Errors
    /// Returns an error if address resolution, connection, socket
    /// configuration, or timeout application fails.
    pub fn connect_tcp(
        addr: &str,
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
        nodelay: bool,
    ) -> io::Result<Self> {
        let transport =
            open_tcp_with_options(addr, connect_timeout, read_timeout, write_timeout, nodelay)?;
        Ok(Self::Tcp(transport))
    }

    /// Connects over TCP and completes a TLS handshake before returning.
    ///
    /// The SNI name is `tls.domain` when set and `server_name` otherwise. The
    /// TLS configuration and server name are resolved before any socket is
    /// opened, so invalid local input fails without dialing. When set,
    /// `connect_timeout` bounds the whole operation as one deadline shared by
    /// DNS, every connection attempt, and the TLS handshake.
    /// The read and write timeouts apply to the connected socket once the
    /// handshake has completed.
    ///
    /// # Errors
    /// Returns [`io::ErrorKind::InvalidInput`] when the TLS configuration,
    /// server name, or timeout is invalid, [`io::ErrorKind::TimedOut`] when the connect
    /// deadline elapses, and otherwise propagates socket or handshake
    /// failures.
    #[cfg(feature = "tls")]
    pub fn connect_tls(
        addr: &str,
        server_name: &str,
        tls: &crate::tls::TlsOptions,
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
        nodelay: bool,
    ) -> io::Result<Self> {
        let deadline = connect_deadline(connect_timeout)?;
        let config = tls.build_client_config()?;
        let name = tls.server_name(server_name)?;
        if [read_timeout, write_timeout]
            .into_iter()
            .flatten()
            .any(|timeout| timeout.is_zero())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O timeouts must be positive",
            ));
        }
        let socket = open_tcp_with_options_until(addr, deadline, None, None, nodelay)?;
        let mut transport = TlsTransport::client(socket, &config, name)?;
        let handshake = transport.handshake_until(deadline);
        dial::remaining(deadline)?;
        handshake?;
        apply_timeouts(&mut transport, read_timeout, write_timeout)?;
        Ok(Self::Tls(Box::new(transport)))
    }

    /// Connects a Unix domain transport.
    ///
    /// The connection attempt is bounded only by the operating system; the read
    /// and write timeouts below apply only after the socket is connected. Use
    /// [`Self::connect_unix_with_timeout`] to bound the connect itself.
    ///
    /// # Errors
    /// Returns an error if the connection or timeout application fails.
    #[cfg(unix)]
    pub fn connect_unix(
        path: impl AsRef<Path>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        Self::connect_unix_with_timeout(path, None, read_timeout, write_timeout)
    }

    /// Connects a Unix domain transport with an optional connect timeout.
    ///
    /// Connection and deadline operations run on a caller-owned Proactor.
    /// Cancellation retires the target completion before returning. The read
    /// and write timeouts are applied once connected.
    ///
    /// # Errors
    /// Returns [`io::ErrorKind::InvalidInput`] when `connect_timeout` is zero,
    /// [`io::ErrorKind::TimedOut`] when the connect does not complete in time,
    /// and otherwise propagates socket failures.
    #[cfg(unix)]
    pub fn connect_unix_with_timeout(
        path: impl AsRef<Path>,
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        let deadline = connect_deadline(connect_timeout)?;
        let mut transport = dial::unix(path.as_ref(), deadline)?;
        apply_timeouts(&mut transport, read_timeout, write_timeout)?;
        Ok(Self::Unix(transport))
    }

    /// Wraps an existing in-memory pipe.
    pub fn from_memory_pipe(pipe: MemoryPipe) -> Self {
        Self::Memory(pipe)
    }

    /// Builds a channel that owns this transport.
    ///
    /// The configuration is validated and the transport is switched to
    /// nonblocking mode before the channel is returned. The configuration is not
    /// rewritten: a TLS transport needs `scheme = "https"` (and usually the
    /// certificate's authority), so pass the matching [`grpc::ChannelConfig`].
    ///
    /// # Errors
    /// Returns an error if the configuration is invalid or the transport cannot
    /// be switched to nonblocking mode.
    pub fn into_channel(self, config: grpc::ChannelConfig) -> io::Result<grpc::SyncChannel<Self>> {
        grpc::SyncChannel::with_config(self, config).map_err(|status| {
            let kind = match status.code() {
                grpc::core::Code::InvalidArgument => io::ErrorKind::InvalidInput,
                _ => io::ErrorKind::Other,
            };
            io::Error::new(kind, status.message().to_owned())
        })
    }

    /// Reports whether this transport is an in-memory pipe.
    pub fn is_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
    }

    /// Reports whether this transport is a TLS session.
    #[cfg(feature = "tls")]
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Reports whether this transport is a TLS session.
    ///
    /// Always `false` when the crate is built without the `tls` feature.
    #[cfg(not(feature = "tls"))]
    pub fn is_tls(&self) -> bool {
        false
    }

    /// Reports whether this transport is a plain TCP stream.
    pub fn is_tcp(&self) -> bool {
        matches!(self, Self::Tcp(_))
    }

    /// Reports whether this transport is a Unix domain stream.
    #[cfg(unix)]
    pub fn is_unix(&self) -> bool {
        matches!(self, Self::Unix(_))
    }

    /// Reports whether this transport is a Unix domain stream.
    ///
    /// Always `false` on platforms without Unix domain sockets.
    #[cfg(not(unix))]
    pub fn is_unix(&self) -> bool {
        false
    }
}

impl Read for EndpointTransport {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(transport) => transport.read(buffer),
            #[cfg(feature = "tls")]
            Self::Tls(transport) => transport.read(buffer),
            #[cfg(unix)]
            Self::Unix(transport) => transport.read(buffer),
            Self::Memory(transport) => transport.read(buffer),
        }
    }
}

impl Write for EndpointTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(transport) => transport.write(buffer),
            #[cfg(feature = "tls")]
            Self::Tls(transport) => transport.write(buffer),
            #[cfg(unix)]
            Self::Unix(transport) => transport.write(buffer),
            Self::Memory(transport) => transport.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(transport) => transport.flush(),
            #[cfg(feature = "tls")]
            Self::Tls(transport) => transport.flush(),
            #[cfg(unix)]
            Self::Unix(transport) => transport.flush(),
            Self::Memory(transport) => transport.flush(),
        }
    }
}

impl SyncTransport for EndpointTransport {
    fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
        match self {
            Self::Tcp(transport) => transport.set_nonblocking(nonblocking),
            #[cfg(feature = "tls")]
            Self::Tls(transport) => transport.set_nonblocking(nonblocking),
            #[cfg(unix)]
            Self::Unix(transport) => transport.set_nonblocking(nonblocking),
            Self::Memory(transport) => transport.set_nonblocking(nonblocking),
        }
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Self::Tcp(transport) => transport.set_read_timeout(timeout),
            #[cfg(feature = "tls")]
            Self::Tls(transport) => transport.set_read_timeout(timeout),
            #[cfg(unix)]
            Self::Unix(transport) => transport.set_read_timeout(timeout),
            Self::Memory(transport) => transport.set_read_timeout(timeout),
        }
    }

    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        match self {
            Self::Tcp(transport) => transport.set_write_timeout(timeout),
            #[cfg(feature = "tls")]
            Self::Tls(transport) => transport.set_write_timeout(timeout),
            #[cfg(unix)]
            Self::Unix(transport) => transport.set_write_timeout(timeout),
            Self::Memory(transport) => transport.set_write_timeout(timeout),
        }
    }

    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        match self {
            Self::Tcp(transport) => transport.read_timeout(),
            #[cfg(feature = "tls")]
            Self::Tls(transport) => transport.read_timeout(),
            #[cfg(unix)]
            Self::Unix(transport) => transport.read_timeout(),
            Self::Memory(transport) => transport.read_timeout(),
        }
    }

    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        match self {
            Self::Tcp(transport) => transport.write_timeout(),
            #[cfg(feature = "tls")]
            Self::Tls(transport) => transport.write_timeout(),
            #[cfg(unix)]
            Self::Unix(transport) => transport.write_timeout(),
            Self::Memory(transport) => transport.write_timeout(),
        }
    }

    fn peer_endpoint(&self) -> io::Result<String> {
        match self {
            Self::Tcp(transport) => transport.peer_endpoint(),
            #[cfg(feature = "tls")]
            Self::Tls(transport) => transport.peer_endpoint(),
            #[cfg(unix)]
            Self::Unix(transport) => transport.peer_endpoint(),
            Self::Memory(transport) => transport.peer_endpoint(),
        }
    }
}

/// Computes the single deadline shared by connection attempts and handshakes.
///
/// A zero timeout cannot bound anything and an overflowing deadline is not
/// representable, so both are rejected as invalid input instead of being
/// silently clamped.
fn connect_deadline(connect_timeout: Option<Duration>) -> io::Result<Option<Instant>> {
    match connect_timeout {
        None => Ok(None),
        Some(timeout) if timeout.is_zero() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "connect timeout must be positive",
        )),
        Some(timeout) => Instant::now()
            .checked_add(timeout)
            .map(Some)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "connect timeout is too large")
            }),
    }
}

fn open_tcp_until(addr: &str, deadline: Option<Instant>) -> io::Result<TcpTransport> {
    let addresses = dns::resolve(addr, deadline, None)?;
    let mut last_error = None;
    for address in addresses {
        match dial::tcp(address, deadline) {
            Ok(transport) => return Ok(transport),
            Err(error) if error.kind() == io::ErrorKind::TimedOut => return Err(error),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "address resolved to no socket addresses",
        )
    }))
}

fn open_tcp_with_options_until(
    addr: &str,
    deadline: Option<Instant>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    nodelay: bool,
) -> io::Result<TcpTransport> {
    let mut transport = open_tcp_until(addr, deadline)?;
    // `TcpTransport` enables TCP_NODELAY unconditionally; disabling it here is a
    // deliberate override for callers that ask for Nagle's algorithm.
    if !nodelay {
        transport.get_ref().set_nodelay(false)?;
    }
    apply_timeouts(&mut transport, read_timeout, write_timeout)?;
    Ok(transport)
}

fn open_tcp_with_options(
    addr: &str,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    nodelay: bool,
) -> io::Result<TcpTransport> {
    let deadline = connect_deadline(connect_timeout)?;
    open_tcp_with_options_until(addr, deadline, read_timeout, write_timeout, nodelay)
}

fn apply_timeouts<T: SyncTransport>(
    transport: &mut T,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
) -> io::Result<()> {
    if let Some(timeout) = read_timeout {
        transport.set_read_timeout(Some(timeout))?;
    }
    if let Some(timeout) = write_timeout {
        transport.set_write_timeout(Some(timeout))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_pipe_reports_its_variant() -> io::Result<()> {
        let (pipe, _peer) = MemoryPipe::with_capacity(1024)?;
        let transport = EndpointTransport::from_memory_pipe(pipe);
        assert!(transport.is_memory());
        assert!(!transport.is_tcp());
        assert!(!transport.is_tls());
        #[cfg(unix)]
        assert!(!transport.is_unix());
        assert_eq!(transport.peer_endpoint()?, "memory://peer");
        Ok(())
    }

    #[test]
    fn memory_pipe_becomes_a_channel() -> io::Result<()> {
        let (pipe, _peer) = MemoryPipe::with_capacity(1024)?;
        let _channel = EndpointTransport::from_memory_pipe(pipe)
            .into_channel(grpc::ChannelConfig::default())?;
        Ok(())
    }

    #[test]
    fn nodelay_false_disables_the_option() -> io::Result<()> {
        let listener = grpc::core::transport::TcpTransportListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let worker = std::thread::spawn(move || listener.accept().map(|_| ()));
        let transport = EndpointTransport::connect_tcp(
            &addr.to_string(),
            Some(Duration::from_secs(5)),
            None,
            None,
            false,
        )?;
        let EndpointTransport::Tcp(tcp) = &transport else {
            panic!("connect_tcp must build a TCP transport");
        };
        assert!(!tcp.get_ref().nodelay()?, "nodelay=false must be honored");
        drop(transport);
        worker.join().expect("listener thread").expect("accept");
        Ok(())
    }
}
