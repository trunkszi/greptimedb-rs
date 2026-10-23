//! Row schema validation and generation through the parser used by the derive.

#[path = "../src/generate.rs"]
mod generate;
#[path = "../src/model.rs"]
mod model;

use model::{Kind, Model, SemanticType, TimeUnit};
use syn::Result;

fn rename(value: &str, style: Option<&str>, span: proc_macro2::Span) -> Result<String> {
    let ident: syn::Ident = syn::parse_str(value)?;
    let style = style.map(|style| {
        let style = syn::LitStr::new(style, span);
        quote::quote!(#[row(rename_all = #style)])
    });
    let mut model = Model::parse(&syn::parse_quote! {
        #style
        struct Schema { #[greptime(timestamp)] ts: i64, #ident: u8 }
    })?;
    Ok(model.fields.swap_remove(1).column)
}

fn parse(source: &str) -> Result<Model> {
    Model::parse(&syn::parse_str(source)?)
}
fn field(source: &str) -> Result<Model> {
    parse(&format!(
        "struct S {{ #[greptime(timestamp)] ts: i64, {source} }}"
    ))
}
fn rejects(source: &str, message: &str) {
    match parse(source) {
        Ok(_) => panic!("schema must be rejected: {message}"),
        Err(error) => assert!(error.to_string().contains(message), "{error}"),
    }
}
#[test]
fn unit_requires_one_non_nullable_temporal_index() {
    rejects("struct Missing { value: u64 }", "exactly one");
    rejects(
        "struct Twice { #[greptime(timestamp)] a: i64, #[greptime(timestamp)] b: TimestampSecond }",
        "exactly one",
    );
    for ty in [
        "Option<i64>",
        "Option<TimestampNanosecond>",
        "String",
        "u64",
    ] {
        rejects(
            &format!("struct Wrong {{ #[greptime(timestamp)] ts: {ty} }}"),
            "non-nullable i64",
        );
    }
    let Ok(parsed) =
        parse("struct S { #[greptime(timestamp)] ts: i64, observed: TimestampSecond }")
    else {
        panic!("valid temporal fields must parse");
    };
    assert_eq!(parsed.fields[0].unit, Some(TimeUnit::Nanosecond));
    assert_eq!(parsed.fields[1].semantic, SemanticType::Field);
    assert_eq!(parsed.fields[1].unit, Some(TimeUnit::Second));
}
#[test]
fn unit_rejects_unsupported_structure_and_field_shapes() {
    for item in [
        "enum E { A }",
        "struct Tuple(i64);",
        "struct Unit;",
        "struct Empty {}",
        "struct G<T> { x: T }",
        "struct G<const N: usize> { x: [u8; N] }",
    ] {
        assert!(parse(item).is_err(), "{item}");
    }
    for ty in [
        "usize",
        "u128",
        "i128",
        "Vec<i64>",
        "Vec<u8, Allocator>",
        "Option<Option<u8>>",
        "Option<u8, u16>",
        "Unknown",
        "[u8; 8]",
        "&mut str",
        "&u64",
        "&'static str",
        "&'static [u8]",
        "Option<&'static str>",
        "Option<&'static [u8]>",
        "T<u8>::Vec<u8>",
        "T<u8>::Option<u8>",
    ] {
        assert!(field(&format!("value: {ty}")).is_err(), "{ty}");
    }
}
#[test]
fn unit_rejects_unknown_duplicate_and_wrong_scope_attributes() {
    for attr in [
        r##"#[row(unknown = "x")]"##,
        "#[row(table = 42)]",
        r##"#[row(table = "a", table = "b")]"##,
        "#[greptime(tag)]",
        r##"#[row(crate_path = "runtime::<u8>")]"##,
    ] {
        assert!(
            parse(&format!(
                "{attr} struct S {{ #[greptime(timestamp)] ts: i64 }}"
            ))
            .is_err(),
            "{attr}"
        );
    }
    for attr in [
        "#[greptime(unknown)]",
        "#[greptime(tag, field)]",
        "#[greptime(tag, tag)]",
        "#[greptime(json = true)]",
        r##"#[greptime(column = "a", column = "b")]"##,
        r##"#[row(table = "x")]"##,
        r##"#[greptime(unit = "ns", unit = "ns")]"##,
    ] {
        assert!(field(&format!("{attr} value: String")).is_err(), "{attr}");
    }
}
#[test]
fn unit_preserves_column_overrides_raw_names_and_runtime_path() {
    let Ok(model) = parse(
        r##"
        #[row(table = "market_ticks", rename_all = "camelCase", crate_path = "crate::sdk")]
        pub struct Tick<'a> {
            #[greptime(timestamp, unit = "ns")] event_time: i64,
            #[greptime(tag, column = "venue")] exchange_name: &'a str,
            r#type: Option<String>, bytes: &'a [u8],
        }"##,
    ) else {
        panic!("valid column overrides and runtime path must parse");
    };
    assert_eq!(model.table.as_deref(), Some("market_ticks"));
    let columns: Vec<_> = model
        .fields
        .iter()
        .map(|field| field.column.as_str())
        .collect();
    assert_eq!(columns, ["eventTime", "venue", "type", "bytes"]);
    assert_eq!(model.fields[0].unit, Some(TimeUnit::Nanosecond));
    assert_eq!(model.fields[1].semantic, SemanticType::Tag);
    assert!(model.fields[2].nullable);
    assert_eq!(model.fields[3].kind, Kind::Binary);
    let runtime = model.runtime;
    assert_eq!(quote::quote!(#runtime).to_string(), "crate :: sdk");
}
#[test]
fn unit_case_conversion_and_column_uniqueness() {
    rejects(
        "struct __ { #[greptime(timestamp)] ts: i64 }",
        "table name must be nonempty",
    );
    let Ok(model) =
        parse(r##"#[row(table = "valid")] struct __ { #[greptime(timestamp)] ts: i64 }"##)
    else {
        panic!("explicit valid table name must parse");
    };
    assert_eq!(model.table.as_deref(), Some("valid"));
    let Ok(inferred) = parse("struct HTTPPrice { #[greptime(timestamp)] ts: i64 }") else {
        panic!("valid inferred table name must parse");
    };
    assert_eq!(inferred.table.as_deref(), Some("http_price"));
    let span = proc_macro2::Span::call_site();
    let Ok(lowercase) = rename("event_time", Some("lowercase"), span) else {
        panic!("lowercase naming must succeed");
    };
    assert_eq!(lowercase, "event_time");
    for (style, expected) in [
        ("snake_case", "http_price"),
        ("camelCase", "httpPrice"),
        ("PascalCase", "HttpPrice"),
        ("SCREAMING_SNAKE_CASE", "HTTP_PRICE"),
        ("kebab-case", "http-price"),
        ("lowercase", "httpprice"),
        ("UPPERCASE", "HTTPPRICE"),
    ] {
        let Ok(renamed) = rename("HTTPPrice", Some(style), span) else {
            panic!("supported naming style must succeed: {style}");
        };
        assert_eq!(renamed, expected);
    }
    for (source, expected) in [
        ("HTTPServerID", "http_server_id"),
        (
            "\u{c5}ngstr\u{f6}mHTTPServerID",
            "\u{e5}ngstr\u{f6}m_http_server_id",
        ),
        ("e\u{301}Value", "e\u{301}_value"),
    ] {
        let Ok(renamed) = rename(source, Some("snake_case"), span) else {
            panic!("snake_case naming must succeed: {source}");
        };
        assert_eq!(renamed, expected);
    }
    rejects(
        r##"#[row(rename_all = "SCREAMING-KEBAB-CASE")] struct S { #[greptime(timestamp)] ts: i64 }"##,
        "unsupported rename_all",
    );
    rejects(
        r##"struct D { #[greptime(timestamp, column = "value")] ts: i64, value: u64 }"##,
        "unique",
    );
    for name in ["", "bad\0name"] {
        let literal = syn::LitStr::new(name, proc_macro2::Span::call_site());
        let literal = quote::quote!(#literal).to_string();
        rejects(
            &format!("#[row(table = {literal})] struct S {{ #[greptime(timestamp)] ts: i64 }}"),
            "nonempty",
        );
    }
}
#[test]
fn unit_temporal_decimal_and_json_attributes_validate_storage_types() {
    let Ok(model) = parse(
        r##"struct S {
        #[greptime(timestamp)] ts: TimestampMicrosecond,
        #[greptime(decimal(precision = 38, scale = 9))] price: Option<i128>,
        #[greptime(json)] metadata: String,
    }"##,
    ) else {
        panic!("valid temporal, decimal and JSON fields must parse");
    };
    assert_eq!(model.fields[0].unit, Some(TimeUnit::Microsecond));
    assert_eq!(model.fields[1].decimal, Some((38, 9)));
    assert_eq!(model.fields[1].kind, Kind::Decimal128);
    assert_eq!(model.fields[2].kind, Kind::Json);
    for source in [
        r##"#[greptime(unit = "ns")] value: String"##,
        r##"#[greptime(unit = "minutes")] value: i64"##,
        r##"#[greptime(unit = "ms")] value: TimestampSecond"##,
        "#[greptime(json)] value: Vec<u8>",
        "#[greptime(decimal(precision = 0, scale = 0))] value: i128",
        "#[greptime(decimal(precision = 39, scale = 0))] value: i128",
        "#[greptime(decimal(precision = 4, scale = 5))] value: i128",
        "#[greptime(decimal(precision = 4))] value: i128",
        "#[greptime(decimal(precision = 4, scale = 0))] value: f64",
        "value: Decimal128",
    ] {
        assert!(field(source).is_err(), "{source}");
    }
}

#[test]
fn unit_rejects_blank_schema_names_without_trimming_valid_names() {
    for blank in [" ", "\t", " \t "] {
        let table =
            format!("#[row(table = {blank:?})] struct S {{ #[greptime(timestamp)] ts: i64 }}");
        rejects(&table, "nonempty");
        let column = format!("#[greptime(column = {blank:?})] value: i64");
        assert!(field(&column).is_err());
    }
    let Ok(model) = parse(
        r#"#[row(table = " padded ")] struct S { #[greptime(timestamp, column = " time ")] ts: i64 }"#,
    ) else {
        panic!("nonblank padded schema names must parse");
    };
    assert_eq!(model.table.as_deref(), Some(" padded "));
    assert_eq!(model.fields[0].column, " time ");
    assert!(!generate::expand(model).is_empty());
}
