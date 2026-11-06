//! Padding-free logical column descriptors with full-width decimal metadata.
use super::ColumnDataType;
use uarch::prelude::*;

bitword! {
    #[derive(Debug)]
    struct DecimalMeta(u64) {
        [0..=31] PRECISION: u32,
        [32..=63] SCALE: u32,
    }
}

#[register_pass]
#[derive(Clone, Copy, Debug)]
pub(crate) struct ColumnDataTypeWrapper {
    kind: ColumnKind,
    decimal: DecimalMeta,
}
assert_layout!(ColumnDataTypeWrapper, size = 16, align = align_of::<u64>());

macro_rules! column_kinds {
    ($($kind:ident),* $(,)?) => {
        #[repr(u64)]
        #[derive(Clone, Copy, Debug)]
        enum ColumnKind { $($kind,)* Decimal128 }
        // SAFETY: The repr(u64) discriminant initializes every byte without padding.
        unsafe impl NoPadding for ColumnKind {}
        // SAFETY: A repr(u64) discriminant is one scalar ABI leaf.
        unsafe impl RegisterLeaf for ColumnKind { const LEAVES: u8 = 1; }
        impl From<ColumnDataType> for ColumnDataTypeWrapper {
            #[hot_inline]
            fn from(data: ColumnDataType) -> Self {
                let (kind, decimal) = match data {
                    $(ColumnDataType::$kind => (ColumnKind::$kind, DecimalMeta::default()),)*
                    ColumnDataType::Decimal128 { precision, scale } => (
                        ColumnKind::Decimal128,
                        DecimalMeta::default().patch(BitPatch::EMPTY
                            .with_field(DecimalMeta::PRECISION, u64::from(precision))
                            .with_field(DecimalMeta::SCALE, u64::from(scale))),
                    ),
                };
                Self { kind, decimal }
            }
        }
        impl ColumnDataTypeWrapper {
            #[hot_inline]
            pub(crate) fn logical(self) -> ColumnDataType {
                match self.kind {
                    $(ColumnKind::$kind => ColumnDataType::$kind,)*
                    ColumnKind::Decimal128 => ColumnDataType::Decimal128 {
                        precision: self.decimal.get(DecimalMeta::PRECISION),
                        scale: self.decimal.get(DecimalMeta::SCALE),
                    },
                }
            }
        }
    };
}
column_kinds!(
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float32,
    Float64,
    Binary,
    String,
    Date,
    DateTime,
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
    Json
);
