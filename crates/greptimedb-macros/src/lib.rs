//! Derive typed GreptimeDB schemas, column buffers, and borrowed row projections.

mod generate;
mod model;

use proc_macro::TokenStream;

/// Generates a schema, reusable `<Name>Batch`, `<Name>Ref`, and `<Name>View`.
///
/// Container options are `#[row(table = "...", rename_all = "...", crate_path = "...")]`.
/// Field options are `#[greptime(tag | timestamp | field, column = "...", unit = "...",
/// decimal(precision = ..., scale = ...), json)]`. Each schema must declare exactly
/// one non-nullable timestamp column. Unannotated fields have the `Field` semantic type.
/// An `i64` timestamp without an explicit unit uses nanoseconds.
///
/// A time index is required and cannot be nullable.
/// ```compile_fail
/// #[derive(greptimedb_macros::Row)]
/// struct MissingTime { value: f64 }
/// ```
/// ```compile_fail
/// #[derive(greptimedb_macros::Row)]
/// struct NullableTime { #[greptime(timestamp)] time: Option<i64> }
/// ```
/// ```compile_fail
/// #[derive(greptimedb_macros::Row)]
/// struct DuplicateTime {
///     #[greptime(timestamp)] first: i64,
///     #[greptime(timestamp)] second: i64,
/// }
/// ```
#[proc_macro_derive(Row, attributes(row, greptime))]
pub fn derive_row(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    model::Model::parse(&input)
        .map(generate::expand)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
