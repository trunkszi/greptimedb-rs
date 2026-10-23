//! Validate the row schema before generating column and projection implementations.

use std::collections::BTreeSet;

use syn::{
    Attribute, Data, DeriveInput, Error, Fields, GenericArgument, Ident, LitInt, LitStr, Path,
    PathArguments, Result, Token, Type, Visibility, ext::IdentExt, meta::ParseNestedMeta,
    spanned::Spanned,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    String,
    Binary,
    Date,
    DateTime,
    Timestamp,
    Decimal128,
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SemanticType {
    Tag,
    Timestamp,
    Field,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

pub struct Field {
    pub ident: Ident,
    pub column: String,
    pub ty: Type,
    pub kind: Kind,
    pub nullable: bool,
    pub semantic: SemanticType,
    pub unit: Option<TimeUnit>,
    pub decimal: Option<(u32, u32)>,
}

pub struct Model {
    pub ident: Ident,
    pub vis: Visibility,
    pub generics: syn::Generics,
    pub runtime: Path,
    pub table: Option<String>,
    pub fields: Vec<Field>,
}

#[derive(Default)]
struct Options {
    table: Option<LitStr>,
    rename_all: Option<LitStr>,
    crate_path: Option<LitStr>,
}

#[derive(Default)]
struct FieldOptions {
    column: Option<LitStr>,
    semantic: Option<SemanticType>,
    unit: Option<TimeUnit>,
    decimal: Option<(u32, u32)>,
    json: Option<()>,
}

fn set_once<T>(slot: &mut Option<T>, value: T, meta: &ParseNestedMeta<'_>) -> Result<()> {
    if slot.is_some() {
        return Err(meta.error("duplicate or conflicting attribute"));
    }
    *slot = Some(value);
    Ok(())
}

fn flag(meta: &ParseNestedMeta<'_>) -> Result<()> {
    if meta.input.is_empty() || meta.input.peek(Token![,]) {
        Ok(())
    } else {
        Err(meta.error("expected a bare flag"))
    }
}

impl Options {
    fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut options = Self::default();
        for attr in attrs {
            if attr.path().is_ident("greptime") {
                return Err(Error::new_spanned(
                    attr,
                    "greptime attributes belong on fields",
                ));
            }
            if !attr.path().is_ident("row") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                let slot = if meta.path.is_ident("table") {
                    &mut options.table
                } else if meta.path.is_ident("rename_all") {
                    &mut options.rename_all
                } else if meta.path.is_ident("crate_path") {
                    &mut options.crate_path
                } else {
                    return Err(meta.error("unknown row attribute"));
                };
                let value = meta.value()?.parse()?;
                set_once(slot, value, &meta)
            })?;
        }
        Ok(options)
    }
}

impl FieldOptions {
    fn parse(attrs: &[Attribute]) -> Result<Self> {
        let mut options = Self::default();
        for attr in attrs {
            if attr.path().is_ident("row") {
                return Err(Error::new_spanned(
                    attr,
                    "row attributes belong on the struct",
                ));
            }
            if !attr.path().is_ident("greptime") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                let semantic = if meta.path.is_ident("tag") {
                    Some(SemanticType::Tag)
                } else if meta.path.is_ident("timestamp") {
                    Some(SemanticType::Timestamp)
                } else if meta.path.is_ident("field") {
                    Some(SemanticType::Field)
                } else {
                    None
                };
                if let Some(semantic) = semantic {
                    flag(&meta)?;
                    set_once(&mut options.semantic, semantic, &meta)
                } else if meta.path.is_ident("column") {
                    let value = meta.value()?.parse()?;
                    set_once(&mut options.column, value, &meta)
                } else if meta.path.is_ident("unit") {
                    let value: LitStr = meta.value()?.parse()?;
                    let unit = match value.value().as_str() {
                        "s" => TimeUnit::Second,
                        "ms" => TimeUnit::Millisecond,
                        "us" => TimeUnit::Microsecond,
                        "ns" => TimeUnit::Nanosecond,
                        _ => return Err(Error::new(value.span(), "expected s, ms, us, or ns")),
                    };
                    set_once(&mut options.unit, unit, &meta)
                } else if meta.path.is_ident("decimal") {
                    let mut precision = None;
                    let mut scale = None;
                    meta.parse_nested_meta(|part| {
                        let slot = if part.path.is_ident("precision") {
                            &mut precision
                        } else if part.path.is_ident("scale") {
                            &mut scale
                        } else {
                            return Err(part.error("expected decimal precision or scale"));
                        };
                        let value: LitInt = part.value()?.parse()?;
                        set_once(slot, value.base10_parse::<u32>()?, &part)
                    })?;
                    let precision =
                        precision.ok_or_else(|| meta.error("decimal requires precision"))?;
                    let scale = scale.ok_or_else(|| meta.error("decimal requires scale"))?;
                    if !(1..=38).contains(&precision) || scale > precision {
                        return Err(
                            meta.error("decimal requires precision 1..=38 and scale <= precision")
                        );
                    }
                    set_once(&mut options.decimal, (precision, scale), &meta)
                } else if meta.path.is_ident("json") {
                    flag(&meta)?;
                    set_once(&mut options.json, (), &meta)
                } else {
                    Err(meta.error("unknown greptime attribute"))
                }
            })?;
        }
        Ok(options)
    }
}

impl Model {
    pub fn parse(input: &DeriveInput) -> Result<Self> {
        let options = Options::parse(&input.attrs)?;
        if input.generics.type_params().next().is_some()
            || input.generics.const_params().next().is_some()
            || input.generics.where_clause.is_some()
            || input
                .generics
                .lifetimes()
                .any(|parameter| !parameter.bounds.is_empty())
        {
            return Err(Error::new_spanned(
                &input.generics,
                "Row supports unbounded lifetime parameters only",
            ));
        }
        let Data::Struct(data) = &input.data else {
            return Err(Error::new_spanned(
                input,
                "Row requires a struct with named fields",
            ));
        };
        let Fields::Named(fields) = &data.fields else {
            return Err(Error::new_spanned(
                &data.fields,
                "Row requires named fields",
            ));
        };
        if fields.named.is_empty() {
            return Err(Error::new_spanned(
                fields,
                "Row requires at least one field",
            ));
        }
        let style = options.rename_all.as_ref().map(LitStr::value);
        if let Some(style) = &options.rename_all {
            rename("field", Some(style.value().as_str()), style.span())?;
        }
        let runtime: Path = match options.crate_path {
            Some(path) => {
                syn::parse_str(&path.value()).map_err(|error| Error::new(path.span(), error))?
            }
            None => syn::parse_quote!(::greptimedb),
        };
        if runtime
            .segments
            .iter()
            .any(|segment| !matches!(segment.arguments, PathArguments::None))
        {
            return Err(Error::new_spanned(
                runtime,
                "crate_path must be a module path without generic arguments",
            ));
        }
        let (table, table_span) = match options.table {
            Some(name) => (name.value(), name.span()),
            None => (
                snake_case(&input.ident.unraw().to_string()),
                input.ident.span(),
            ),
        };
        validate_name(&table, table_span, "table")?;
        let mut columns = BTreeSet::new();
        let mut members = Vec::with_capacity(fields.named.len());
        for field in &fields.named {
            let options = FieldOptions::parse(&field.attrs)?;
            let ident = field
                .ident
                .as_ref()
                .ok_or_else(|| Error::new(field.span(), "expected field name"))?;
            let column = match &options.column {
                Some(column) => column.value(),
                None => rename(&ident.unraw().to_string(), style.as_deref(), ident.span())?,
            };
            validate_name(&column, field.span(), "column")?;
            if !columns.insert(column.clone()) {
                return Err(Error::new(field.span(), "column names must be unique"));
            }
            let (base, nullable) = nullable_type(&field.ty)?;
            let (mut kind, intrinsic_unit) = base_kind(base)?;
            let semantic = options.semantic.unwrap_or(SemanticType::Field);
            if options.json.is_some() {
                if kind != Kind::String {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "json requires a string field",
                    ));
                }
                kind = Kind::Json;
            }
            if options.decimal.is_some() {
                if kind != Kind::Decimal128 {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "decimal requires i128 or Decimal128",
                    ));
                }
            } else if kind == Kind::Decimal128 {
                return Err(Error::new_spanned(
                    &field.ty,
                    "Decimal128 requires precision and scale attributes",
                ));
            }
            if let Some(unit) = options.unit {
                if !matches!(kind, Kind::I64 | Kind::Timestamp) {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "unit requires i64 or a timestamp wrapper",
                    ));
                }
                if intrinsic_unit.is_some_and(|intrinsic| intrinsic != unit) {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "unit conflicts with timestamp wrapper",
                    ));
                }
                kind = Kind::Timestamp;
            }
            if semantic == SemanticType::Timestamp {
                if nullable || !matches!(kind, Kind::I64 | Kind::Timestamp) {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "timestamp requires a non-nullable i64 or timestamp wrapper",
                    ));
                }
                kind = Kind::Timestamp;
            }
            members.push(Field {
                ident: ident.clone(),
                column,
                ty: field.ty.clone(),
                kind,
                nullable,
                semantic,
                unit: options
                    .unit
                    .or(intrinsic_unit)
                    .or_else(|| (kind == Kind::Timestamp).then_some(TimeUnit::Nanosecond)),
                decimal: options.decimal,
            });
        }
        if members
            .iter()
            .filter(|field| field.semantic == SemanticType::Timestamp)
            .count()
            != 1
        {
            return Err(Error::new_spanned(
                input,
                "Row requires exactly one #[greptime(timestamp)] field",
            ));
        }
        Ok(Self {
            ident: input.ident.clone(),
            vis: input.vis.clone(),
            generics: input.generics.clone(),
            runtime,
            table: Some(table),
            fields: members,
        })
    }
}

fn validate_name(value: &str, span: proc_macro2::Span, kind: &str) -> Result<()> {
    if value.trim().is_empty() || value.contains('\0') {
        Err(Error::new(
            span,
            format!("{kind} name must be nonempty and contain no NUL"),
        ))
    } else {
        Ok(())
    }
}

fn words(value: &str) -> Vec<String> {
    let chars: Vec<_> = value.chars().collect();
    let mut words = Vec::new();
    let mut word = String::new();
    for (index, &current) in chars.iter().enumerate() {
        if current == '_' || current == '-' || current.is_whitespace() {
            if !word.is_empty() {
                words.push(std::mem::take(&mut word).to_lowercase());
            }
            continue;
        }
        let previous = word.chars().rev().find(|ch| ch.is_alphanumeric());
        let next_lower = chars.get(index + 1).is_some_and(|next| next.is_lowercase());
        let boundary = current.is_uppercase()
            && previous.is_some_and(|previous| {
                previous.is_lowercase()
                    || previous.is_numeric()
                    || (previous.is_uppercase() && next_lower)
            });
        if boundary {
            words.push(std::mem::take(&mut word).to_lowercase());
        }
        word.push(current);
    }
    if !word.is_empty() {
        words.push(word.to_lowercase());
    }
    words
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

pub fn snake_case(value: &str) -> String {
    words(value).join("_")
}

fn rename(value: &str, style: Option<&str>, span: proc_macro2::Span) -> Result<String> {
    let style = match style {
        None => return Ok(value.to_owned()),
        Some("lowercase") => return Ok(value.to_lowercase()),
        Some("UPPERCASE") => return Ok(value.to_uppercase()),
        Some(style) => style,
    };
    let words = words(value);
    Ok(match style {
        "snake_case" => snake_case(value),
        "camelCase" => words
            .iter()
            .enumerate()
            .map(|(index, word)| {
                if index == 0 {
                    word.clone()
                } else {
                    capitalize(word)
                }
            })
            .collect(),
        "PascalCase" => words.iter().map(|word| capitalize(word)).collect(),
        "SCREAMING_SNAKE_CASE" => words.join("_").to_uppercase(),
        "kebab-case" => words.join("-"),
        _ => return Err(Error::new(span, "unsupported rename_all case")),
    })
}

fn one_type_argument(segment: &syn::PathSegment) -> Result<&Type> {
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(Error::new_spanned(
            segment,
            "expected exactly one type argument",
        ));
    };
    match arguments.args.iter().collect::<Vec<_>>().as_slice() {
        [GenericArgument::Type(inner)] => Ok(inner),
        _ => Err(Error::new_spanned(
            arguments,
            "expected exactly one type argument",
        )),
    }
}

fn nullable_type(ty: &Type) -> Result<(&Type, bool)> {
    if let Type::Path(path) = ty
        && path.qself.is_none()
        && let Some(segment) = path.path.segments.last()
        && segment.ident == "Option"
    {
        if path
            .path
            .segments
            .iter()
            .take(path.path.segments.len() - 1)
            .any(|segment| !matches!(segment.arguments, PathArguments::None))
        {
            return Err(Error::new_spanned(
                ty,
                "unexpected generic arguments before Option",
            ));
        }
        return Ok((one_type_argument(segment)?, true));
    }
    Ok((ty, false))
}

fn base_kind(ty: &Type) -> Result<(Kind, Option<TimeUnit>)> {
    if let Type::Reference(reference) = ty {
        if let Some(lifetime) = &reference.lifetime
            && lifetime.ident == "static"
        {
            return Err(Error::new_spanned(
                ty,
                "Row does not support static references",
            ));
        }
        if reference.mutability.is_some() {
            return Err(Error::new_spanned(
                ty,
                "Row does not support mutable references",
            ));
        }
        return match reference.elem.as_ref() {
            Type::Path(path) if path.path.is_ident("str") => Ok((Kind::String, None)),
            Type::Slice(slice) if matches!(slice.elem.as_ref(), Type::Path(path) if path.path.is_ident("u8")) => {
                Ok((Kind::Binary, None))
            }
            _ => Err(Error::new_spanned(
                ty,
                "borrowed fields must be &str or &[u8]",
            )),
        };
    }
    let Type::Path(path) = ty else {
        return Err(Error::new_spanned(ty, "unsupported Row field type"));
    };
    if path.qself.is_some() {
        return Err(Error::new_spanned(
            ty,
            "associated field types are unsupported",
        ));
    }
    let segment = path
        .path
        .segments
        .last()
        .ok_or_else(|| Error::new_spanned(ty, "expected field type"))?;
    let name = segment.ident.to_string();
    if name == "Vec" {
        if path
            .path
            .segments
            .iter()
            .take(path.path.segments.len() - 1)
            .any(|segment| !matches!(segment.arguments, PathArguments::None))
        {
            return Err(Error::new_spanned(
                ty,
                "unexpected generic arguments before Vec",
            ));
        }
        let inner = one_type_argument(segment)?;
        if matches!(inner, Type::Path(path) if path.path.is_ident("u8")) {
            return Ok((Kind::Binary, None));
        }
        return Err(Error::new_spanned(ty, "only Vec<u8> is supported"));
    }
    if !matches!(segment.arguments, PathArguments::None)
        || path
            .path
            .segments
            .iter()
            .any(|segment| !matches!(segment.arguments, PathArguments::None))
    {
        return Err(Error::new_spanned(
            ty,
            "unexpected generic arguments in Row field",
        ));
    }
    Ok(match name.as_str() {
        "bool" => (Kind::Bool, None),
        "i8" => (Kind::I8, None),
        "i16" => (Kind::I16, None),
        "i32" => (Kind::I32, None),
        "i64" => (Kind::I64, None),
        "u8" => (Kind::U8, None),
        "u16" => (Kind::U16, None),
        "u32" => (Kind::U32, None),
        "u64" => (Kind::U64, None),
        "f32" => (Kind::F32, None),
        "f64" => (Kind::F64, None),
        "String" => (Kind::String, None),
        "Date" => (Kind::Date, None),
        "DateTime" => (Kind::DateTime, None),
        "i128" | "Decimal128" => (Kind::Decimal128, None),
        "TimestampSecond" => (Kind::Timestamp, Some(TimeUnit::Second)),
        "TimestampMillisecond" => (Kind::Timestamp, Some(TimeUnit::Millisecond)),
        "TimestampMicrosecond" => (Kind::Timestamp, Some(TimeUnit::Microsecond)),
        "TimestampNanosecond" => (Kind::Timestamp, Some(TimeUnit::Nanosecond)),
        _ => return Err(Error::new_spanned(ty, "unsupported Row field type")),
    })
}
