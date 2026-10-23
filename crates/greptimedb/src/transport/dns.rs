//! c-ares lookups driven by the caller's completion ring and shared deadline.

use super::dial;
use cares::{AresStatus, ProactorResolver, Resolver, ResolverOptions};
use liburing::{Proactor, SubsystemId};
use std::{
    cell::{Cell, RefCell},
    io,
    net::{IpAddr, SocketAddr},
    rc::Rc,
    time::Instant,
};

struct Owner {
    owned: Option<(Proactor, ProactorResolver)>,
}

fn retire_or_quarantine<R, D>(
    (mut ring, driver): (R, D),
    pending: bool,
    shutdown: impl FnOnce(&mut R) -> io::Result<()>,
) {
    // Pending targets and cancellation controls retain the ring and all operands.
    // Failed shutdown cannot release either member of the resource bundle.
    if pending || shutdown(&mut ring).is_err() {
        std::mem::forget((ring, driver));
    } else {
        drop(ring);
        drop(driver);
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        if let Some((ring, driver)) = self.owned.take() {
            let pending = driver.has_pending_io();
            retire_or_quarantine((ring, driver), pending, |ring| {
                ring.shutdown().map_err(dial::error)
            });
        }
    }
}

impl Owner {
    fn new(ring: Proactor, driver: ProactorResolver) -> Self {
        Self {
            owned: Some((ring, driver)),
        }
    }

    fn parts(&mut self) -> io::Result<(&mut Proactor, &mut ProactorResolver)> {
        self.owned
            .as_mut()
            .map(|(ring, driver)| (ring, driver))
            .ok_or_else(|| io::Error::other("DNS resources already retired"))
    }

    fn finish(&mut self, deadline: Option<Instant>) -> io::Result<()> {
        let (ring, driver) = self.parts()?;
        ring.poll_completions(dial::RING_ENTRIES as usize, driver)
            .map_err(dial::error)?;
        if driver.has_pending_io() {
            dial::remaining(deadline)?;
        }
        driver.begin_shutdown(ring).map_err(io::Error::other)?;
        while driver.has_pending_io() {
            let budget = dial::remaining(deadline)?;
            driver.flush(ring).map_err(io::Error::other)?;
            ring.wait_completions(1, budget, driver)
                .map_err(dial::error)?;
        }
        Ok(())
    }
}

pub(super) fn resolve(
    addr: &str,
    deadline: Option<Instant>,
    servers: Option<&str>,
) -> io::Result<Vec<SocketAddr>> {
    dial::remaining(deadline)?;
    if let Ok(address) = addr.parse::<SocketAddr>() {
        return Ok(vec![address]);
    }
    let (host, port) = addr.rsplit_once(':').ok_or(io::ErrorKind::InvalidInput)?;
    let port: u16 = port.parse().map_err(|_| io::ErrorKind::InvalidInput)?;
    if host.is_empty() || host.contains([':', '[', ']']) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    let options = ResolverOptions::builder()
        .tries(1)
        .build()
        .map_err(io::Error::other)?;
    dial::remaining(deadline)?;
    let servers = match servers {
        Some(servers) => servers.to_owned(),
        None => Resolver::new(
            ResolverOptions::builder()
                .build()
                .map_err(io::Error::other)?,
        )
        .and_then(|resolver| resolver.servers_csv())
        .map_err(io::Error::other)?,
    };
    dial::remaining(deadline)?;
    let subsystem = SubsystemId::new(1).ok_or_else(|| io::Error::other("invalid DNS subsystem"))?;
    let mut driver =
        ProactorResolver::new(subsystem, options, &servers, 4).map_err(io::Error::other)?;
    let pending = Rc::new(Cell::new(2));
    let addresses = Rc::new(RefCell::new(Vec::new()));
    let status = Rc::new(Cell::new(AresStatus::NotFound));
    for family in [libc::AF_INET, libc::AF_INET6] {
        dial::remaining(deadline)?;
        let pending = Rc::clone(&pending);
        let addresses = Rc::clone(&addresses);
        let status = Rc::clone(&status);
        driver
            .resolver
            .gethostbyname(host, family, move |_, outcome| {
                if let Some(host) = outcome.host {
                    addresses.borrow_mut().extend(host.addresses);
                }
                if outcome.status != AresStatus::Success {
                    status.set(outcome.status);
                }
                pending.set(pending.get() - 1);
            })
            .map_err(io::Error::other)?;
    }
    dial::remaining(deadline)?;
    let mut owner = Owner::new(dial::ring()?, driver);
    {
        let (ring, driver) = owner.parts()?;
        driver.flush(ring).map_err(io::Error::other)?;
    }
    let mut failure = None;
    while pending.get() != 0 && addresses.borrow().is_empty() {
        let budget = match dial::remaining(deadline) {
            Ok(budget) => budget,
            Err(error) => {
                failure = Some(error);
                break;
            }
        };
        let (ring, driver) = owner.parts()?;
        match ring.wait_completions(1, budget, driver) {
            Ok(_) => {}
            Err(liburing::UringError::Wait(libc::EINTR)) => continue,
            Err(error) => {
                failure = Some(dial::error(error));
                break;
            }
        }
    }
    // The first usable family can dial immediately; its unanswered sibling must
    // not consume the entire connect budget. Final polling may add an answer.
    let retired = owner.finish(deadline);
    let values: Vec<_> = addresses
        .borrow()
        .iter()
        .copied()
        .map(|ip| SocketAddr::new(ip, port))
        .collect();
    if !values.is_empty() {
        return Ok(values);
    }
    if let Some(error) = failure {
        return Err(error);
    }
    retired?;
    if status.get() == AresStatus::Timeout {
        Err(io::ErrorKind::TimedOut.into())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("DNS lookup failed: {:?}", status.get()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        net::{TcpListener, UdpSocket},
        thread,
        time::Duration,
    };

    #[test]
    fn scenario_lost_dns_packets_use_the_shared_connect_deadline() -> io::Result<()> {
        let blackhole = UdpSocket::bind("127.0.0.1:0")?;
        let server = blackhole.local_addr()?.to_string();
        let budget = Duration::from_millis(60);
        let start = Instant::now();
        let error = resolve("lost.test.:4001", Some(start + budget), Some(&server))
            .expect_err("DNS deadline");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(start.elapsed() >= budget);
        assert!(start.elapsed() < budget + Duration::from_millis(30));
        blackhole.set_nonblocking(true)?;
        assert!(
            blackhole.recv_from(&mut [0; 512]).is_ok(),
            "lookup submitted a real UDP query"
        );
        Ok(())
    }

    #[test]
    fn scenario_fast_ipv4_dials_while_ipv6_is_silent() -> io::Result<()> {
        usable_family(true)
    }

    #[test]
    fn scenario_fast_ipv6_keeps_dial_budget_while_ipv4_is_silent() -> io::Result<()> {
        usable_family(false)
    }

    fn usable_family(ipv4: bool) -> io::Result<()> {
        let listener = if ipv4 {
            Some(TcpListener::bind("127.0.0.1:0")?)
        } else {
            None
        };
        let expected = match &listener {
            Some(listener) => listener.local_addr()?,
            None => SocketAddr::new(std::net::Ipv6Addr::LOCALHOST.into(), 4001),
        };
        let dns = UdpSocket::bind("127.0.0.1:0")?;
        dns.set_read_timeout(Some(Duration::from_secs(1)))?;
        let server = dns.local_addr()?.to_string();
        let worker = thread::spawn(move || -> io::Result<()> {
            for _ in 0..2 {
                let mut query = [0; 512];
                let (length, peer) = dns.recv_from(&mut query)?;
                let mut end = 12;
                while end < length && query[end] != 0 {
                    end += 1 + usize::from(query[end]);
                }
                let wanted = if ipv4 { 1u16 } else { 28u16 };
                if query[end + 1..end + 3] != wanted.to_be_bytes() {
                    continue;
                }
                let mut reply = query[..end + 5].to_vec();
                reply[2..4].copy_from_slice(&[0x81, 0x80]);
                reply[6..8].copy_from_slice(&[0, 1]);
                reply[10..12].copy_from_slice(&[0, 0]);
                reply.extend_from_slice(&[0xc0, 0x0c]);
                reply.extend_from_slice(&wanted.to_be_bytes());
                reply.extend_from_slice(&[0, 1, 0, 0, 0, 30, 0, if ipv4 { 4 } else { 16 }]);
                if ipv4 {
                    reply.extend_from_slice(&[127, 0, 0, 1]);
                } else {
                    reply.extend_from_slice(&std::net::Ipv6Addr::LOCALHOST.octets());
                }
                dns.send_to(&reply, peer)?;
            }
            Ok(())
        });
        let start = Instant::now();
        let budget = Duration::from_millis(250);
        let deadline = Some(start + budget);
        let addresses = resolve(
            &format!("one-family.test.:{}", expected.port()),
            deadline,
            Some(&server),
        )?;
        assert_eq!(addresses, [expected]);
        if let Some(listener) = listener {
            let _transport = dial::tcp(addresses[0], deadline)?;
            let _accepted = listener.accept()?;
        }
        assert!(dial::remaining(deadline)?.is_some());
        assert!(
            start.elapsed() < budget,
            "the answered family retained its dial budget"
        );
        worker.join().expect("DNS fixture")?;
        Ok(())
    }

    #[test]
    fn integration_dns_results_dial_over_the_same_remaining_budget() -> io::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let dns = UdpSocket::bind("127.0.0.1:0")?;
        dns.set_read_timeout(Some(Duration::from_secs(2)))?;
        let server = dns.local_addr()?.to_string();
        let worker = thread::spawn(move || -> io::Result<()> {
            for _ in 0..2 {
                let mut query = [0; 512];
                let (length, peer) = dns.recv_from(&mut query)?;
                let mut question = 12;
                while question < length && query[question] != 0 {
                    question += 1 + usize::from(query[question]);
                }
                let ipv4 = query[question + 1..question + 3] == [0, 1];
                let mut reply = query[..question + 5].to_vec();
                reply[10..12].copy_from_slice(&[0, 0]);
                reply[2..4].copy_from_slice(&[0x81, 0x80]);
                reply[6..8].copy_from_slice(&[0, u8::from(ipv4)]);
                if ipv4 {
                    reply.extend_from_slice(&[
                        0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 30, 0, 4, 127, 0, 0, 1,
                    ]);
                }
                dns.send_to(&reply, peer)?;
            }
            Ok(())
        });
        let deadline = Some(Instant::now() + Duration::from_secs(2));
        let addresses = resolve(
            &format!("dial.test.:{}", listener.local_addr()?.port()),
            deadline,
            Some(&server),
        )?;
        assert_eq!(addresses, [listener.local_addr()?]);
        let transport = dial::tcp(addresses[0], deadline)?;
        let (accepted, _) = listener.accept()?;
        assert_eq!(accepted.peer_addr()?, transport.get_ref().local_addr()?);
        worker.join().expect("DNS fixture")?;
        Ok(())
    }

    struct Probe {
        shutdowns: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
        fails: bool,
    }
    impl Probe {
        fn retire(&mut self) -> io::Result<()> {
            self.shutdowns.set(self.shutdowns.get() + 1);
            if self.fails {
                Err(io::ErrorKind::TimedOut.into())
            } else {
                Ok(())
            }
        }
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }
    struct BusinessState {
        drops: Rc<Cell<usize>>,
        pending: bool,
    }
    impl Drop for BusinessState {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    struct TestOwner {
        owned: Option<(Probe, BusinessState)>,
    }
    impl TestOwner {
        fn new(ring: Probe, driver: BusinessState) -> Self {
            Self {
                owned: Some((ring, driver)),
            }
        }
    }
    impl Drop for TestOwner {
        fn drop(&mut self) {
            if let Some((ring, driver)) = self.owned.take() {
                let pending = driver.pending;
                retire_or_quarantine((ring, driver), pending, Probe::retire);
            }
        }
    }

    #[test]
    fn lifecycle_failed_shutdown_never_closes_the_ring_on_either_result_path() {
        fn complete(_owner: TestOwner, success: bool) -> io::Result<()> {
            if success {
                Ok(())
            } else {
                Err(io::ErrorKind::TimedOut.into())
            }
        }
        for success in [true, false] {
            let shutdowns = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let states = Rc::new(Cell::new(0));
            let owner = TestOwner::new(
                Probe {
                    shutdowns: Rc::clone(&shutdowns),
                    drops: Rc::clone(&drops),
                    fails: true,
                },
                BusinessState {
                    drops: Rc::clone(&states),
                    pending: false,
                },
            );
            assert_eq!(complete(owner, success).is_ok(), success);
            assert_eq!(shutdowns.get(), 1);
            assert_eq!(drops.get(), 0);
            assert_eq!(
                states.get(),
                0,
                "caller-owned operands must be quarantined too"
            );
        }
    }

    #[test]
    fn lifecycle_pending_work_skips_blocking_shutdown_but_drained_work_closes() {
        for idle in [true, false] {
            let shutdowns = Rc::new(Cell::new(0));
            let drops = Rc::new(Cell::new(0));
            let states = Rc::new(Cell::new(0));
            let owner = TestOwner::new(
                Probe {
                    shutdowns: Rc::clone(&shutdowns),
                    drops: Rc::clone(&drops),
                    fails: false,
                },
                BusinessState {
                    drops: Rc::clone(&states),
                    pending: !idle,
                },
            );
            drop(owner);
            assert_eq!(shutdowns.get(), usize::from(idle));
            assert_eq!(drops.get(), usize::from(idle));
            assert_eq!(states.get(), usize::from(idle));
        }
    }
}
