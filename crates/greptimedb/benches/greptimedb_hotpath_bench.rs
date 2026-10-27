//! Hot-path latency for reusable ingestion buffers, Flight record batches, and failover.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use greptimedb::bulk::{FlightCompression, FlightIpcEncoder, batch_to_record_batch};
use greptimedb::column_buf::NullMask;
use greptimedb::failover::{CircuitPhase, CircuitSuperstate, EndpointSelection, FailoverPolicy};
use greptimedb::{
    BufferPolicy, ColumnDataType, MemoryBuffer, Row, SemanticType, TypedColumnBuf, ValueRef,
};

const ROWS: usize = 64;

thread_local! {
    static COUNT_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static ALLOC_COUNT: Cell<u64> = const { Cell::new(0) };
}

struct CountingAllocator;

// SAFETY: every allocation is forwarded to the system allocator. The counter only
// updates thread-local cells and does not create or free pointers itself.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_alloc();
        // SAFETY: `layout` is a valid non-zero layout supplied by the language allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from this allocator with the same `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_alloc();
        // SAFETY: `layout` is a valid non-zero layout supplied by the language allocator.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_alloc();
        // SAFETY: `ptr` came from this allocator with `layout`, and `new_size` is non-zero.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn note_alloc() {
    let Ok(active) = COUNT_ACTIVE.try_with(Cell::get) else {
        return;
    };
    if !active {
        return;
    }
    let Ok(()) = ALLOC_COUNT.try_with(|count| count.set(count.get().saturating_add(1))) else {
        return;
    };
}

fn prime_counter() {
    COUNT_ACTIVE.with(|_| {});
    ALLOC_COUNT.with(|_| {});
}

fn allocations_during(body: impl FnOnce()) -> u64 {
    prime_counter();
    ALLOC_COUNT.with(|count| count.set(0));
    COUNT_ACTIVE.with(|active| active.set(true));
    body();
    COUNT_ACTIVE.with(|active| active.set(false));
    ALLOC_COUNT.with(Cell::get)
}

#[derive(Row)]
#[row(table = "hotpath_ticks")]
struct HotTick {
    #[greptime(tag)]
    symbol: String,
    #[greptime(timestamp, unit = "ns")]
    ts: i64,
    #[greptime(field)]
    price: f64,
    #[greptime(field)]
    size: Option<u64>,
    #[greptime(field)]
    note: String,
}

fn sample_row(index: usize) -> HotTickRef<'static> {
    HotTickRef {
        symbol: "BTCUSDT",
        ts: index as i64,
        price: 100.25,
        size: if index.is_multiple_of(8) {
            None
        } else {
            Some(3)
        },
        note: "bid",
    }
}

fn warm_row_batch() -> HotTickBatch {
    let mut batch = HotTick::new_batch_with_policy(ROWS, BufferPolicy::unbounded());
    for index in 0..ROWS {
        batch.push_ref(&sample_row(index));
    }
    batch.clear();
    batch
}

fn fill_row_batch(batch: &mut HotTickBatch) {
    for index in 0..ROWS {
        batch.push_ref(&sample_row(index));
    }
}

fn row_batch_push_and_clear_reuse(batch: &mut HotTickBatch) {
    fill_row_batch(batch);
    batch.clear();
}

fn column_buf_fixed_and_null_bitmap(column: &mut TypedColumnBuf, mask: &mut NullMask) {
    fill_column(column);
    for index in 0..ROWS {
        mask.push(index.is_multiple_of(8));
    }
    column.clear_unbounded();
    mask.clear_unbounded();
}

fn warm_column(data_type: ColumnDataType) -> TypedColumnBuf {
    let mut column = TypedColumnBuf::with_capacity("value", SemanticType::Field, data_type, ROWS);
    fill_column(&mut column);
    column.clear_with_policy(BufferPolicy::unbounded());
    column
}

fn fill_column(column: &mut TypedColumnBuf) {
    for index in 0..ROWS {
        let value = match column.data_type() {
            ColumnDataType::Int64 => ValueRef::Int64(index as i64),
            ColumnDataType::TimestampNanosecond => {
                ValueRef::TimestampNanosecond(1_700_000_000 + index as i64)
            }
            ColumnDataType::String => ValueRef::String("BTCUSDT"),
            ColumnDataType::Binary => ValueRef::Binary(b"payload"),
            other => panic!("unexpected bench column {other:?}"),
        };
        let null = column.data_type() == ColumnDataType::String && index.is_multiple_of(8);
        let value = if null { ValueRef::Null } else { value };
        match column.push(value) {
            Ok(()) => {}
            Err(error) => panic!("column push failed: {error}"),
        }
    }
}

fn warm_mask() -> NullMask {
    let mut mask = NullMask::with_capacity(ROWS);
    for index in 0..ROWS {
        mask.push(index.is_multiple_of(8));
    }
    mask.clear_unbounded();
    mask
}

fn assert_zero(label: &str, count: u64) {
    assert!(
        count == 0,
        "{label} allocated {count} times on the reused path"
    );
}

fn warm_selection(selection: EndpointSelection) -> FailoverPolicy {
    let mut policy = FailoverPolicy::new(["primary", "secondary", "tertiary"])
        .with_selection(selection)
        .with_random_seed(7);
    policy.record_success(1, Duration::from_micros(40));
    policy.record_success(2, Duration::from_micros(80));
    let now = Instant::now();
    black_box(select_once(&mut policy, now));
    policy
}

fn assert_selection_zero(selection: EndpointSelection, label: &str) {
    let now = Instant::now();
    let mut policy = warm_selection(selection);
    assert_zero(
        label,
        allocations_during(|| {
            black_box(select_once(&mut policy, now));
        }),
    );
}

fn preflight() {
    prime_counter();
    let mut rows = warm_row_batch();
    assert_zero(
        "row_batch_push_and_clear_reuse",
        allocations_during(|| row_batch_push_and_clear_reuse(&mut rows)),
    );
    black_box(rows.len());

    let mut primitive = warm_column(ColumnDataType::Int64);
    assert_zero(
        "primitive column",
        allocations_during(|| fill_column(&mut primitive)),
    );
    let mut timestamp = TypedColumnBuf::with_capacity(
        "ts",
        SemanticType::Timestamp,
        ColumnDataType::TimestampNanosecond,
        ROWS,
    );
    fill_column(&mut timestamp);
    timestamp.clear_with_policy(BufferPolicy::unbounded());
    assert_zero(
        "timestamp column",
        allocations_during(|| fill_column(&mut timestamp)),
    );
    let mut text = warm_column(ColumnDataType::String);
    assert_zero(
        "string column",
        allocations_during(|| fill_column(&mut text)),
    );
    let mut binary = warm_column(ColumnDataType::Binary);
    assert_zero(
        "binary column",
        allocations_during(|| fill_column(&mut binary)),
    );
    let mut mask = warm_mask();
    assert_zero(
        "null mask",
        allocations_during(|| {
            for index in 0..ROWS {
                mask.push(index.is_multiple_of(8));
            }
            mask.clear_unbounded();
        }),
    );
    let mut fixed = warm_column(ColumnDataType::Int64);
    let mut fixed_mask = warm_mask();
    assert_zero(
        "column_buf_fixed_and_null_bitmap",
        allocations_during(|| column_buf_fixed_and_null_bitmap(&mut fixed, &mut fixed_mask)),
    );

    let opened = Instant::now();
    let mut policy = FailoverPolicy::new(["primary", "secondary"])
        .with_selection(EndpointSelection::Sticky)
        .with_circuit_breaker(2, Duration::from_millis(5));
    match policy.select_endpoint(opened) {
        Ok(index) => assert_eq!(index, 0),
        Err(error) => panic!("sticky selection failed: {error}"),
    }
    assert_zero(
        "sticky selection",
        allocations_during(|| {
            let index = match policy.select_endpoint(opened) {
                Ok(index) => index,
                Err(error) => panic!("sticky reselection failed: {error}"),
            };
            black_box(index);
        }),
    );
    assert_eq!(
        policy.endpoint_phase(0, opened),
        Some(CircuitPhase::ClosedHealthy)
    );
    assert_eq!(
        policy
            .endpoint_phase(0, opened)
            .map(CircuitPhase::superstate),
        Some(CircuitSuperstate::Eligible)
    );
    policy.record_failure(0, opened);
    assert_eq!(
        policy.endpoint_phase(0, opened),
        Some(CircuitPhase::ClosedDegraded)
    );
    let health = match policy.health(0) {
        Some(health) => health,
        None => panic!("missing endpoint health"),
    };
    assert_eq!(health.consecutive_failures, 1);
    assert!(health.circuit_opened_at.is_none());
    assert!(!health.healthy);
    policy.record_failure(0, opened);
    assert_eq!(
        policy.endpoint_phase(0, opened),
        Some(CircuitPhase::OpenCooldown)
    );
    assert_eq!(
        policy
            .endpoint_phase(0, opened)
            .map(CircuitPhase::superstate),
        Some(CircuitSuperstate::Ineligible)
    );
    let probe_at = opened + Duration::from_millis(5);
    assert_eq!(
        policy.endpoint_phase(0, probe_at),
        Some(CircuitPhase::HalfOpenProbe)
    );
    assert!(policy.needs_probe(0, probe_at));
    policy.record_probe_success(0);
    assert_eq!(
        policy.endpoint_phase(0, probe_at),
        Some(CircuitPhase::ClosedHealthy)
    );
    assert_selection_zero(EndpointSelection::RoundRobin, "round-robin selection");
    assert_selection_zero(EndpointSelection::Random, "random selection");
    assert_selection_zero(EndpointSelection::LatencyAware, "latency-aware selection");
    let closed = match policy.health(0) {
        Some(health) => health,
        None => panic!("missing endpoint health after probe"),
    };
    assert_eq!(closed.consecutive_failures, 0);
    assert!(closed.circuit_opened_at.is_none());
    assert!(closed.healthy);

    let mut batch = warm_row_batch();
    fill_row_batch(&mut batch);
    // Arrow IPC encoding allocates the header and body by design.
    encode_flight(&batch);
}

fn encode_flight(batch: &HotTickBatch) {
    let record = match batch_to_record_batch(batch) {
        Ok(record) => record,
        Err(error) => panic!("record batch build failed: {error}"),
    };
    let mut encoder = match FlightIpcEncoder::new(FlightCompression::None) {
        Ok(encoder) => encoder,
        Err(error) => panic!("flight encoder failed: {error}"),
    };
    match encoder.encode_batch(&record) {
        Ok(frame) => {
            assert!(!frame.data_header.is_empty());
        }
        Err(error) => panic!("flight encode failed: {error}"),
    }
}

fn flight_frame(batch: &HotTickBatch) -> usize {
    let record = match batch_to_record_batch(batch) {
        Ok(record) => record,
        Err(error) => panic!("record batch build failed: {error}"),
    };
    let mut encoder = match FlightIpcEncoder::new(FlightCompression::None) {
        Ok(encoder) => encoder,
        Err(error) => panic!("flight encoder failed: {error}"),
    };
    match encoder.encode_batch(&record) {
        Ok(frame) => frame.data_body.len() + frame.data_header.len(),
        Err(error) => panic!("flight encode failed: {error}"),
    }
}

fn select_once(policy: &mut FailoverPolicy, now: Instant) -> usize {
    match policy.select_endpoint(now) {
        Ok(index) => index,
        Err(error) => panic!("endpoint selection failed: {error}"),
    }
}

fn hotpath(c: &mut Criterion) {
    preflight();

    c.bench_function("row_batch_push_ref_clear", |b| {
        let mut batch = warm_row_batch();
        b.iter(|| row_batch_push_and_clear_reuse(&mut batch));
    });

    c.bench_function("column_primitive_push", |b| {
        let mut column = warm_column(ColumnDataType::Int64);
        b.iter(|| {
            fill_column(&mut column);
            column.clear_unbounded();
        });
    });

    c.bench_function("column_timestamp_push", |b| {
        let mut column = TypedColumnBuf::with_capacity(
            "ts",
            SemanticType::Timestamp,
            ColumnDataType::TimestampNanosecond,
            ROWS,
        );
        fill_column(&mut column);
        column.clear_unbounded();
        b.iter(|| {
            fill_column(&mut column);
            column.clear_unbounded();
        });
    });

    c.bench_function("column_string_push", |b| {
        let mut column = warm_column(ColumnDataType::String);
        b.iter(|| {
            fill_column(&mut column);
            column.clear_unbounded();
        });
    });

    c.bench_function("column_binary_push", |b| {
        let mut column = warm_column(ColumnDataType::Binary);
        b.iter(|| {
            fill_column(&mut column);
            column.clear_unbounded();
        });
    });

    c.bench_function("null_mask_pack", |b| {
        let mut mask = warm_mask();
        b.iter(|| {
            for index in 0..ROWS {
                mask.push(index.is_multiple_of(8));
            }
            mask.clear_unbounded();
            black_box(mask.null_count());
        });
    });

    c.bench_function("flight_doput_record_batch", |b| {
        let mut batch = warm_row_batch();
        fill_row_batch(&mut batch);
        b.iter(|| black_box(flight_frame(&batch)));
    });

    let now = Instant::now();
    c.bench_function("failover_sticky_select", |b| {
        b.iter_batched(
            || {
                let mut policy = FailoverPolicy::new(["primary", "secondary", "tertiary"])
                    .with_selection(EndpointSelection::Sticky);
                black_box(select_once(&mut policy, now));
                policy
            },
            |mut policy| black_box(select_once(&mut policy, now)),
            BatchSize::SmallInput,
        );
    });

    for (name, selection) in [
        ("failover_round_robin", EndpointSelection::RoundRobin),
        ("failover_random", EndpointSelection::Random),
        ("failover_latency", EndpointSelection::LatencyAware),
    ] {
        c.bench_function(name, |b| {
            let mut policy = FailoverPolicy::new(["primary", "secondary", "tertiary"])
                .with_selection(selection)
                .with_random_seed(7);
            policy.record_success(1, Duration::from_micros(40));
            policy.record_success(2, Duration::from_micros(80));
            b.iter(|| black_box(select_once(&mut policy, now)));
        });
    }

    c.bench_function("failover_circuit_transition", |b| {
        b.iter_batched(
            || {
                FailoverPolicy::new(["primary", "secondary"])
                    .with_circuit_breaker(1, Duration::from_millis(1))
            },
            |mut policy| {
                policy.record_failure(0, now);
                let later = now + Duration::from_millis(1);
                let phase = policy.endpoint_phase(0, later);
                policy.record_probe_success(0);
                black_box(phase);
            },
            BatchSize::SmallInput,
        );
    });

    black_box(now);
}

criterion_group!(hotpath_group, hotpath);
criterion_main!(hotpath_group);
