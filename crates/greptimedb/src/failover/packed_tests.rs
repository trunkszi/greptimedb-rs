use super::state::CircuitWord;
use super::*;

/// Saturating failures must not wrap into a healthy circuit.
#[test]
fn packed_failures_saturate() {
    let now = Instant::now();
    let mut state = EndpointHealthState::fresh();
    state.word = state.word.patch(patch!(CircuitWord {
        FAILURES: 0xffff_ffff
    }));
    state.consecutive_failures = u32::MAX;
    assert!(state.fail(3, now));
    assert_eq!(state.consecutive_failures, u32::MAX);
    assert!(
        state
            .word
            .matches(pattern!(CircuitWord { OPEN & !HEALTHY & FAILURES == 0xffff_ffff }))
    );
    state.close();
    assert!(
        state
            .word
            .matches(pattern!(CircuitWord { HEALTHY & !OPEN & FAILURES == 0 }))
    );
}

/// Public health snapshots retain their independently editable phase inputs.
#[test]
fn health_snapshot_remains_editable() {
    let mut state = EndpointHealthState::fresh();
    state.consecutive_failures = 7;
    state.healthy = false;
    assert_eq!(
        state.phase(3, Duration::ZERO, Instant::now()),
        CircuitPhase::ClosedDegraded
    );
}

/// Non-power-of-two rotations preserve order through exclusions and cloning.
#[test]
fn rotation_exclusions_keep_order() {
    let now = Instant::now();
    for count in [1, 3, 7, 12] {
        let mut policy = FailoverPolicy::new((0..count).map(|n| n.to_string()));
        for index in 0..count * 4 {
            assert_eq!(
                policy.select_endpoint_excluding(now, &[count]).unwrap(),
                index % count
            );
        }
        let mut cloned = policy.clone();
        for _ in 0..count * 2 {
            assert_eq!(
                policy.select_endpoint(now).unwrap(),
                cloned.select_endpoint(now).unwrap()
            );
        }
    }
}

/// Connect success retains degradation until a completed probe closes the circuit.
#[test]
fn packed_transitions_keep_probe_rules() {
    let now = Instant::now();
    let cooldown = Duration::from_secs(3);
    let mut policy = FailoverPolicy::new(["one"]).with_circuit_breaker(2, cooldown);
    policy.record_failure(0, now);
    policy.record_connect_success(0);
    assert_eq!(
        policy.endpoint_phase(0, now),
        Some(CircuitPhase::ClosedDegraded)
    );
    policy.record_failure(0, now);
    let ready = now + cooldown;
    assert_eq!(policy.select_endpoint(ready).unwrap(), 0);
    assert!(policy.select_endpoint(ready).is_err());
    policy.release_probe(0);
    assert_eq!(policy.select_endpoint(ready).unwrap(), 0);
    policy.record_probe_success(0);
    assert_eq!(
        policy.endpoint_phase(0, ready),
        Some(CircuitPhase::ClosedHealthy)
    );
    assert_eq!(policy.health(0).unwrap().consecutive_failures, 0);
}

/// Random selection keeps its seeded mapping when the eligible window changes.
#[test]
fn random_windows_match_modulo() {
    let now = Instant::now();
    let mut policy = FailoverPolicy::new((0..7).map(|n| n.to_string()))
        .with_selection(EndpointSelection::Random)
        .with_random_seed(73);
    let mut oracle = policy.clone();
    for excluded in [&[][..], &[0, 3, 6][..], &[1, 2, 3, 4, 5, 6][..]] {
        let available: Vec<_> = (0..7).filter(|n| !excluded.contains(n)).collect();
        for _ in 0..20 {
            let draw = oracle.next_random();
            let expected = available[(draw % available.len() as u64) as usize];
            assert_eq!(
                policy.select_endpoint_excluding(now, excluded).unwrap(),
                expected
            );
            // Restore the common origin; only the random draw should determine the slot.
            policy.cursor = 0;
        }
    }
}
