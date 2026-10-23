//! Column and batch memory accounting, reuse, and allocation-free capacity release.

use anyhow::Result;
use greptimedb::buffer_policy::{DEFAULT_MAX_RETAINED_BYTES, DEFAULT_MAX_RETAINED_ROWS};
use greptimedb::{
    BatchEncode, BufferPolicy, ColumnDataType, ColumnarBatch, Decimal128, MemoryBuffer, RowBatch,
    SemanticType, TimestampUnit, TypedColumnBuf, ValueRef,
};

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountingAllocator;

fn record_allocation() {
    ALLOCATIONS
        .try_with(|count| {
            if let Some(value) = count.get() {
                count.set(Some(value + 1));
            }
        })
        .unwrap_or_default();
}

// SAFETY: Every allocation operation delegates to System with the original
// pointer/layout. Thread-local accounting does not allocate or change ownership.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: GlobalAlloc's caller supplies a valid allocation layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        // SAFETY: GlobalAlloc's caller supplies a valid allocation layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: The allocation originated from System with this same layout.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation();
        // SAFETY: The caller supplies a live System allocation and valid size.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn count_allocations(operation: impl FnOnce()) -> usize {
    struct DisableOnDrop;
    impl Drop for DisableOnDrop {
        fn drop(&mut self) {
            ALLOCATIONS.with(|count| count.set(None));
        }
    }
    ALLOCATIONS.with(|count| assert!(count.replace(Some(0)).is_none()));
    let guard = DisableOnDrop;
    operation();
    let result = ALLOCATIONS.with(|count| count.get().unwrap_or_default());
    drop(guard);
    result
}

fn column(data_type: ColumnDataType) -> TypedColumnBuf {
    TypedColumnBuf::new("value", SemanticType::Field, data_type)
}

fn exercise_buffer_reuse(buffer: &mut impl MemoryBuffer) {
    let bytes = buffer.allocated_bytes();
    let capacity = buffer.row_capacity();
    assert!(bytes > 0);
    assert!(capacity >= 16);
    buffer.clear_unbounded();
    assert_eq!(buffer.allocated_bytes(), bytes);
    assert_eq!(buffer.row_capacity(), capacity);
    buffer.clear_with_policy(BufferPolicy::new(usize::MAX, 0));
    assert_eq!(buffer.allocated_bytes(), 0);
    assert_eq!(buffer.row_capacity(), 0);
}

#[test]
fn policy_watermarks_are_independent_inclusive_and_overflow_free() {
    let defaults = BufferPolicy::default();
    assert_eq!(defaults.max_retained_rows, DEFAULT_MAX_RETAINED_ROWS);
    assert_eq!(defaults.max_retained_bytes, DEFAULT_MAX_RETAINED_BYTES);
    assert_eq!(DEFAULT_MAX_RETAINED_ROWS, 65_536);
    assert_eq!(DEFAULT_MAX_RETAINED_BYTES, 8 * 1024 * 1024);
    let policy = BufferPolicy::new(8, 64);
    assert!(!policy.should_shrink(8, 64));
    assert!(policy.should_shrink(9, 0));
    assert!(policy.should_shrink(0, 65));
    assert!(!BufferPolicy::unbounded().should_shrink(usize::MAX, usize::MAX));
    assert!(!BufferPolicy::zero().should_shrink(0, 0));
    assert!(BufferPolicy::zero().should_shrink(1, 0));
    assert!(BufferPolicy::zero().should_shrink(0, 1));
}

#[test]
fn cleared_fixed_width_columns_retain_or_drop_capacity_by_watermark() {
    let mut buf =
        TypedColumnBuf::with_capacity("n", SemanticType::Field, ColumnDataType::Int64, 1024);
    for value in 0..10 {
        buf.push(ValueRef::Int64(value)).expect("integer");
    }
    let before = buf.to_proto_column();
    let large = buf.allocated_bytes();
    buf.shrink_to_fit();
    assert_eq!(buf.to_proto_column(), before);
    assert!(buf.allocated_bytes() < large);
    let bytes = buf.allocated_bytes();
    let rows = buf.row_capacity();
    buf.clear_with_policy(BufferPolicy::new(rows, bytes));
    assert_eq!(buf.row_count(), 0);
    assert_eq!(buf.non_null_len(), 0);
    assert_eq!(buf.allocated_bytes(), bytes);
    assert_eq!(buf.row_capacity(), rows);
    buf.push(ValueRef::Int64(99)).expect("reuse");
    assert_eq!(
        buf.to_proto_column().values.expect("values").i64_values,
        vec![99]
    );
    buf.clear_with_policy(BufferPolicy::new(usize::MAX, bytes - 1));
    assert_eq!(buf.allocated_bytes(), 0);
    assert_eq!(buf.row_capacity(), 0);
    buf.push(ValueRef::Null).expect("reuse after release");
    buf.clear_with_policy(BufferPolicy::new(0, usize::MAX));
    assert_eq!(buf.allocated_bytes(), 0);
}

#[test]
fn nested_string_binary_and_json_capacity_is_budgeted_and_reused() {
    let large = "x".repeat(16 * 1024);
    for data_type in [
        ColumnDataType::String,
        ColumnDataType::Binary,
        ColumnDataType::Json,
    ] {
        let mut buf = column(data_type);
        for _ in 0..3 {
            let value = match data_type {
                ColumnDataType::String => ValueRef::String(&large),
                ColumnDataType::Binary => ValueRef::Binary(large.as_bytes()),
                _ => ValueRef::Json(&large),
            };
            buf.push(value).expect("variable-length value");
        }
        assert!(buf.allocated_bytes() >= 3 * large.len());
        buf.clear_unbounded();
        let warm = buf.allocated_bytes();
        assert!(warm >= 3 * large.len());
        assert_eq!(buf.row_count(), 0);
        assert_eq!(buf.non_null_len(), 0);
        for _ in 0..3 {
            let value = match data_type {
                ColumnDataType::String => ValueRef::String("small"),
                ColumnDataType::Binary => ValueRef::Binary(b"small"),
                _ => ValueRef::Json("small"),
            };
            buf.push(value).expect("reuse nested allocation");
        }
        assert_eq!(buf.allocated_bytes(), warm);
        let wire = buf.to_proto_column();
        buf.shrink_to_fit();
        assert_eq!(buf.to_proto_column(), wire);
        assert!(buf.allocated_bytes() < warm);
        buf.clear_with_policy(BufferPolicy::zero());
        assert_eq!(buf.allocated_bytes(), 0);
        assert_eq!(buf.row_capacity(), 0);
    }
}

#[test]
fn clone_and_clear_are_independent_and_default_releases_burst_capacity() {
    let mut buf = column(ColumnDataType::String);
    buf.push(ValueRef::String("retained")).expect("string");
    let cloned = buf.clone();
    buf.clear();
    assert_eq!(buf.row_count(), 0);
    assert_eq!(cloned.row_count(), 1);
    assert_eq!(
        cloned
            .to_proto_column()
            .values
            .expect("values")
            .string_values,
        vec!["retained"]
    );
    let mut burst = TypedColumnBuf::with_capacity(
        "burst",
        SemanticType::Field,
        ColumnDataType::Int8,
        DEFAULT_MAX_RETAINED_ROWS + 1,
    );
    assert!(burst.row_capacity() > DEFAULT_MAX_RETAINED_ROWS);
    burst.clear();
    assert_eq!(burst.row_capacity(), 0);
    assert_eq!(burst.allocated_bytes(), 0);
}

#[test]
fn memory_accounting_and_retention_cover_every_storage_family() {
    for (data_type, value) in [
        (ColumnDataType::String, ValueRef::String("payload")),
        (ColumnDataType::Binary, ValueRef::Binary(b"payload")),
        (ColumnDataType::Json, ValueRef::Json("{\"k\":1}")),
        (ColumnDataType::Int64, ValueRef::Int64(42)),
        (ColumnDataType::Float64, ValueRef::Float64(1.5)),
        (
            ColumnDataType::TimestampMillisecond,
            ValueRef::TimestampMillisecond(42),
        ),
        (
            ColumnDataType::Decimal128 {
                precision: 38,
                scale: 9,
            },
            ValueRef::Decimal128(Decimal128::new(42, 38, 9)),
        ),
    ] {
        let mut buf = column(data_type);
        for _ in 0..3 {
            let mut previous = buf.allocated_bytes();
            for _ in 0..17 {
                buf.push(value.clone()).expect("matching value");
                assert!(buf.allocated_bytes() >= previous);
                previous = buf.allocated_bytes();
            }
            buf.clear_unbounded();
            let bytes = buf.allocated_bytes();
            let rows = buf.row_capacity();
            assert!(bytes > 0);
            buf.clear_with_policy(BufferPolicy::new(rows, bytes));
            assert_eq!(buf.allocated_bytes(), bytes);
            assert_eq!(buf.row_capacity(), rows);
        }
        let bytes = buf.allocated_bytes();
        buf.clear_with_policy(BufferPolicy::new(usize::MAX, bytes - 1));
        assert_eq!((buf.allocated_bytes(), buf.row_capacity()), (0, 0));
        buf.push(value.clone()).expect("reuse after byte limit");
        let rows = buf.row_capacity();
        buf.clear_with_policy(BufferPolicy::new(rows - 1, usize::MAX));
        assert_eq!((buf.allocated_bytes(), buf.row_capacity()), (0, 0));
        buf.push(value).expect("reuse after row limit");
        buf.clear_with_policy(BufferPolicy::zero());
        assert_eq!((buf.allocated_bytes(), buf.row_capacity()), (0, 0));
    }
}

#[test]
fn unit_lifecycle_reuse_retains_schema_and_enforces_aggregate_byte_limit() {
    let mut columns = ColumnarBatch::with_capacity("ticks", 16);
    columns
        .add_timestamp_column("time", TimestampUnit::Millisecond)
        .unwrap();
    let mut rows = RowBatch::with_capacity("ticks", 16);
    rows.add_column_schema(
        "time",
        SemanticType::Timestamp,
        ColumnDataType::TimestampMillisecond,
    )
    .unwrap();
    columns
        .push_row(&[ValueRef::TimestampMillisecond(1)])
        .unwrap();
    rows.push_row(&[ValueRef::TimestampMillisecond(1)]).unwrap();
    exercise_buffer_reuse(&mut columns);
    exercise_buffer_reuse(&mut rows);
    assert_eq!(columns.row_count(), 0);
    assert_eq!(rows.row_count(), 0);
    columns
        .push_row(&[ValueRef::TimestampMillisecond(2)])
        .unwrap();
    rows.push_row(&[ValueRef::TimestampMillisecond(2)]).unwrap();
    assert_eq!(columns.encode_insert_request().unwrap().row_count, 1);
    assert_eq!(
        rows.encode_row_insert_request()
            .unwrap()
            .rows
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn unit_lifecycle_configured_clear_policy_and_explicit_override() {
    let mut columns = ColumnarBatch::with_policy("ticks", 16, BufferPolicy::zero());
    columns
        .add_timestamp_column("time", TimestampUnit::Second)
        .unwrap();
    columns.push_row(&[ValueRef::TimestampSecond(1)]).unwrap();
    columns.clear_unbounded();
    assert!(columns.allocated_bytes() > 0);
    columns.push_row(&[ValueRef::TimestampSecond(2)]).unwrap();
    columns.clear();
    assert_eq!(columns.allocated_bytes(), 0);
    columns.push_row(&[ValueRef::TimestampSecond(3)]).unwrap();
    assert_eq!(columns.encode_insert_request().unwrap().row_count, 1);
}

#[test]
fn unit_lifecycle_retained_byte_budget_applies_to_whole_batch() {
    let mut columns = ColumnarBatch::with_capacity("ticks", 16);
    columns
        .add_timestamp_column("time", TimestampUnit::Second)
        .unwrap();
    columns
        .add_field_column("price", ColumnDataType::Float64)
        .unwrap();
    let one_column_bytes = columns.column(0).unwrap().allocated_bytes();
    assert!(one_column_bytes < columns.allocated_bytes());
    columns
        .push_row(&[ValueRef::TimestampSecond(1), ValueRef::Float64(42.5)])
        .unwrap();
    columns.clear_with_policy(BufferPolicy::new(usize::MAX, one_column_bytes));
    assert!(columns.allocated_bytes() <= one_column_bytes);
    columns
        .push_row(&[ValueRef::TimestampSecond(2), ValueRef::Float64(43.0)])
        .unwrap();
    assert_eq!(columns.encode_insert_request().unwrap().row_count, 1);
    columns.clear_with_policy(BufferPolicy::new(0, usize::MAX));
    assert_eq!(columns.row_capacity(), 0);

    let mut rows = RowBatch::with_capacity("ticks", 16);
    rows.add_column_schema(
        "time",
        SemanticType::Timestamp,
        ColumnDataType::TimestampSecond,
    )
    .unwrap();
    rows.push_row(&[ValueRef::TimestampSecond(1)]).unwrap();
    rows.clear_with_policy(BufferPolicy::new(0, usize::MAX));
    assert_eq!(rows.allocated_bytes(), 0);
    assert_eq!(rows.row_capacity(), 0);
}

#[test]
fn over_limit_variable_columns_release_without_allocating_descriptor_pools() -> Result<()> {
    for data_type in [ColumnDataType::String, ColumnDataType::Binary] {
        for policy in [
            BufferPolicy::zero(),
            BufferPolicy::new(0, usize::MAX),
            BufferPolicy::new(usize::MAX, 0),
            BufferPolicy::new(1, usize::MAX),
            BufferPolicy::new(usize::MAX, 1),
        ] {
            let mut column =
                TypedColumnBuf::with_capacity("payload", SemanticType::Field, data_type, 1024);
            for _ in 0..1024 {
                column.push(match data_type {
                    ColumnDataType::String => ValueRef::String("retained payload"),
                    _ => ValueRef::Binary(b"retained payload"),
                })?;
            }
            let allocations = count_allocations(|| column.clear_with_policy(policy));
            assert_eq!(
                allocations, 0,
                "clear allocated for {data_type:?} and {policy:?}"
            );
            assert_eq!(column.row_count(), 0);
            assert_eq!(column.row_capacity(), 0);
            assert_eq!(column.allocated_bytes(), 0);
        }
    }
    Ok(())
}

#[test]
fn empty_retained_variable_columns_release_without_allocating() {
    for data_type in [ColumnDataType::String, ColumnDataType::Binary] {
        let mut column =
            TypedColumnBuf::with_capacity("payload", SemanticType::Field, data_type, 1024);
        assert_eq!(column.row_count(), 0);
        assert!(column.row_capacity() >= 1024);
        let allocations = count_allocations(|| {
            column.clear_with_policy(BufferPolicy::new(64, usize::MAX));
        });
        assert_eq!(allocations, 0);
        assert_eq!(column.row_capacity(), 0);
        assert_eq!(column.allocated_bytes(), 0);
    }
}

#[test]
fn batch_retained_capacity_reports_the_largest_column() -> Result<()> {
    let mut batch = ColumnarBatch::new("metrics");
    batch.add_field_column("small", ColumnDataType::Int64)?;
    batch.add_field_column("large", ColumnDataType::String)?;
    batch
        .column_mut(0)
        .expect("small")
        .push(ValueRef::Int64(1))?;
    for _ in 0..1024 {
        batch
            .column_mut(1)
            .expect("large")
            .push(ValueRef::String("payload"))?;
    }
    let capacities: Vec<_> = batch
        .columns()
        .iter()
        .map(MemoryBuffer::row_capacity)
        .collect();
    assert!(capacities[0] < capacities[1]);
    assert_eq!(batch.row_capacity(), capacities[1]);
    Ok(())
}
