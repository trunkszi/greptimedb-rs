//! Shared little-endian 64-row validity windows.
use arrow_array::Array;
use uarch::prelude::*;

#[hot_inline]
pub(crate) fn bit_window(bytes: &[u8], offset: usize, count: usize) -> Bitmap64 {
    if count == 0 {
        return Bitmap64::EMPTY;
    }
    let base = offset / 8;
    let shift = offset & 7;
    let low = match bytes
        .get(base..base + 8)
        .and_then(|window| window.try_into().ok())
    {
        Some(window) => u64::from_le_bytes(window),
        None => bytes
            .get(base..)
            .unwrap_or_default()
            .iter()
            .take(8)
            .enumerate()
            .fold(0, |word, (index, byte)| {
                word | (u64::from(*byte) << (index * 8))
            }),
    };
    let high = if shift == 0 {
        0
    } else {
        bytes
            .get(base + 8)
            .map_or(0, |byte| u64::from(*byte) << (64 - shift))
    };
    Bitmap64::new(((low >> shift) | high) & Bitmap64::low_mask(count as u32))
}

#[hot_inline]
pub(crate) fn arrow_validity(array: &dyn Array, start: usize, count: usize) -> Bitmap64 {
    if count == 0 {
        return Bitmap64::EMPTY;
    }
    match array.nulls() {
        Some(nulls) => {
            if let Some(next) = nulls.validity().get((nulls.offset() + start) / 8 + 8) {
                prefetch_read::<L1, _>(next);
            }
            bit_window(nulls.validity(), nulls.offset() + start, count)
        }
        None => Bitmap64::new(Bitmap64::low_mask(count as u32)),
    }
}

pub(crate) fn wire_rows(mask: &[u8], rows: usize) -> impl Iterator<Item = bool> + '_ {
    (0..rows).step_by(64).flat_map(move |start| {
        let count = (rows - start).min(64);
        let bitmap = bit_window(mask, start, count);
        (0..count).map(move |bit| bitmap.test(bit as u32))
    })
}
