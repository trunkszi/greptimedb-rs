//! Generate typed column buffers and borrowed row access without row-wise values.
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{GenericArgument, PathArguments, Type, ext::IdentExt};

use crate::model::{Field, Kind, Model, SemanticType, TimeUnit};

fn inner_type(field: &Field) -> &Type {
    if field.nullable
        && let Type::Path(path) = &field.ty
        && let Some(segment) = path.path.segments.last()
        && let PathArguments::AngleBracketed(args) = &segment.arguments
        && let Some(GenericArgument::Type(ty)) = args.args.first()
    {
        return ty;
    }
    &field.ty
}
fn canonical(field: &Field) -> TokenStream {
    let ty = inner_type(field);
    let ty = match ty {
        Type::Reference(_) if matches!(field.kind, Kind::String | Kind::Json) => {
            quote!(::std::string::String)
        }
        Type::Reference(_) if field.kind == Kind::Binary => {
            quote!(::std::vec::Vec<::core::primitive::u8>)
        }
        _ => quote!(#ty),
    };
    if field.nullable {
        quote!(::core::option::Option<#ty>)
    } else {
        ty
    }
}
fn borrow(field: &Field) -> TokenStream {
    let name = &field.ident;
    if matches!(inner_type(field), Type::Reference(_)) {
        return quote!(row.#name);
    }
    match (field.kind, field.nullable) {
        (Kind::String | Kind::Json, false) => quote!(row.#name.as_str()),
        (Kind::Binary, false) => quote!(row.#name.as_slice()),
        (Kind::String | Kind::Json | Kind::Binary, true) => quote!(row.#name.as_deref()),
        _ => quote!(row.#name),
    }
}
fn owned(field: &Field) -> TokenStream {
    let name = &field.ident;
    if matches!(inner_type(field), Type::Reference(_)) {
        return quote!(self.#name);
    }
    match (field.kind, field.nullable) {
        (Kind::String | Kind::Json | Kind::Binary, false) => {
            quote!(::std::borrow::ToOwned::to_owned(self.#name))
        }
        (Kind::String | Kind::Json | Kind::Binary, true) => {
            quote!(self.#name.map(::std::borrow::ToOwned::to_owned))
        }
        _ => quote!(self.#name),
    }
}
fn data_type(field: &Field, runtime: &syn::Path) -> TokenStream {
    let variant = match field.kind {
        Kind::Bool => quote!(Boolean),
        Kind::I8 => quote!(Int8),
        Kind::I16 => quote!(Int16),
        Kind::I32 => quote!(Int32),
        Kind::I64 => quote!(Int64),
        Kind::U8 => quote!(Uint8),
        Kind::U16 => quote!(Uint16),
        Kind::U32 => quote!(Uint32),
        Kind::U64 => quote!(Uint64),
        Kind::F32 => quote!(Float32),
        Kind::F64 => quote!(Float64),
        Kind::String => quote!(String),
        Kind::Binary => quote!(Binary),
        Kind::Date => quote!(Date),
        Kind::DateTime => quote!(DateTime),
        Kind::Json => quote!(Json),
        Kind::Timestamp => match field.unit.unwrap_or(TimeUnit::Nanosecond) {
            TimeUnit::Second => quote!(TimestampSecond),
            TimeUnit::Millisecond => quote!(TimestampMillisecond),
            TimeUnit::Microsecond => quote!(TimestampMicrosecond),
            TimeUnit::Nanosecond => quote!(TimestampNanosecond),
        },
        Kind::Decimal128 => {
            let (precision, scale) = field.decimal.unwrap_or((38, 0));
            quote!(Decimal128 { precision: #precision, scale: #scale })
        }
    };
    quote!(#runtime::types::ColumnDataType::#variant)
}

pub fn expand(model: Model) -> TokenStream {
    let Model {
        ident,
        vis,
        generics,
        runtime,
        table,
        fields,
    } = model;
    let batch = format_ident!("{}Batch", ident.unraw());
    let row_ref = format_ident!("{}Ref", ident.unraw());
    let view = format_ident!("{}View", ident.unraw());
    let iter = format_ident!("{}Iter", ident.unraw());
    let names: Vec<_> = fields.iter().map(|f| &f.ident).collect();
    let buffers: Vec<_> = fields
        .iter()
        .enumerate()
        .map(|(i, _)| format_ident!("__column_{i}"))
        .collect();
    let types: Vec<_> = fields.iter().map(canonical).collect();
    let borrowed: Vec<_> = fields.iter().map(borrow).collect();
    let owned: Vec<_> = fields.iter().map(owned).collect();
    let metas: Vec<_> = fields.iter().map(|f| {
        let column = &f.column;
        let nullable = f.nullable;
        let semantic = match f.semantic {
            SemanticType::Tag => quote!(Tag), SemanticType::Timestamp => quote!(Timestamp), SemanticType::Field => quote!(Field),
        };
        let data_type = data_type(f, &runtime);
        quote!(#runtime::row::ColumnMeta { name: #column, semantic_type: #runtime::types::SemanticType::#semantic, data_type: #data_type, nullable: #nullable })
    }).collect();
    let indices: Vec<_> = (0..fields.len()).collect();
    let key_indices: Vec<_> = fields
        .iter()
        .enumerate()
        .filter(|(_, f)| f.semantic != SemanticType::Field)
        .map(|(i, _)| i)
        .collect();
    let key_buffers: Vec<_> = key_indices.iter().map(|i| &buffers[*i]).collect();
    let key_types: Vec<_> = key_indices.iter().map(|i| &types[*i]).collect();
    let field_count = fields.len();
    let first = &buffers[0];
    let first_type = &types[0];
    let table = table.unwrap_or_else(|| crate::model::snake_case(&ident.unraw().to_string()));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();
    let mut lifetime_name = String::from("'__greptime");
    while generics
        .lifetimes()
        .any(|p| p.lifetime.to_string() == lifetime_name)
    {
        lifetime_name.push('_');
    }
    let lifetime = syn::Lifetime::new(&lifetime_name, proc_macro2::Span::mixed_site());
    let view_lifetimes: Vec<_> = generics.lifetimes().map(|_| quote!(#lifetime)).collect();
    let original_view = if view_lifetimes.is_empty() {
        quote!(#ident)
    } else {
        quote!(#ident<#(#view_lifetimes),*>)
    };
    let mut borrowed_generics = generics.clone();
    borrowed_generics
        .params
        .insert(0, syn::parse_quote!(#lifetime));
    let (borrowed_impl, _, borrowed_where) = borrowed_generics.split_for_impl();
    quote! {
        impl #impl_generics #runtime::row::Row for #ident #type_generics #where_clause {
            const TABLE_NAME: &'static ::core::primitive::str = #table;
            const COLUMNS: &'static [#runtime::row::ColumnMeta] = #batch::COLUMNS;
            type Batch = #batch;
            fn new_batch() -> Self::Batch { <#batch as ::core::default::Default>::default() }
            fn new_batch_with_capacity(rows: ::core::primitive::usize) -> Self::Batch { #batch::with_capacity(rows) }
            fn new_batch_with_policy(rows: ::core::primitive::usize, policy: #runtime::buffer_policy::BufferPolicy) -> Self::Batch { #batch::with_policy(rows, policy) }
            fn push_into_batch(&self, batch: &mut Self::Batch) { batch.push(self); }
        }
        impl #impl_generics #runtime::row::RowDecode for #ident #type_generics #where_clause {
            fn decode_record_batch_with_policy(record: &#runtime::row::decode::RecordBatch, policy: #runtime::buffer_policy::BufferPolicy) -> #runtime::error::Result<Self::Batch> {
                #batch::from_record_batch_with_policy(record, policy)
            }
        }
        #[doc = "A borrowed row whose variable-sized values are slices."]
        #[derive(::core::clone::Clone, ::core::marker::Copy)]
        #vis struct #row_ref<#lifetime> { #(pub #names: <#types as #runtime::row::Field>::Input<#lifetime>,)* }
        impl #borrowed_impl #runtime::row::BorrowedRow<#lifetime, #ident #type_generics> for #row_ref<#lifetime> #borrowed_where {
            fn push_into_batch(&self, batch: &mut #batch) { batch.push_ref(self); }
        }
        #[doc = "A row read directly from the batch's typed column buffers."]
        #[derive(::core::clone::Clone, ::core::marker::Copy)]
        #vis struct #view<#lifetime> { #(pub #names: <#types as #runtime::row::Field>::Input<#lifetime>,)* }
        impl<#lifetime> #view<#lifetime> {
            #[doc = "Reconstruct the source row, copying only its owned strings and byte vectors."]
            pub fn to_owned_row(&self) -> #original_view { #ident { #(#names: #owned,)* } }
            #[doc = "Borrow this view as an ingestion row without allocation."]
            pub fn as_ref(&self) -> #row_ref<#lifetime> { #row_ref { #(#names: self.#names,)* } }
        }
        #[doc = "Thread-affine reusable structure-of-arrays storage for one table."]
        #vis struct #batch {
            #(#buffers: <#types as #runtime::row::Field>::Buffer,)*
            __policy: #runtime::buffer_policy::BufferPolicy,
            __thread: ::core::marker::PhantomData<::std::rc::Rc<()>>,
        }
        impl ::core::default::Default for #batch {
            fn default() -> Self { Self::with_capacity(0) }
        }
        impl #batch {
            #[doc = "Logical column metadata in source declaration order."]
            pub const COLUMNS: &'static [#runtime::row::ColumnMeta] = &[#(#metas),*];
            #[doc = "Create empty reusable storage with the default retention policy."]
            pub fn new() -> Self { <Self as ::core::default::Default>::default() }
            #[doc = "Preallocate rows under the default retention policy."]
            pub fn with_capacity(rows: ::core::primitive::usize) -> Self { Self::with_policy(rows, ::core::default::Default::default()) }
            #[doc = "Preallocate rows and select the policy used by clear."]
            pub fn with_policy(rows: ::core::primitive::usize, policy: #runtime::buffer_policy::BufferPolicy) -> Self {
                Self { #(#buffers: <#types as #runtime::row::Field>::with_capacity(rows),)* __policy: policy, __thread: ::core::marker::PhantomData }
            }
            #[doc = "Decode named Arrow columns into reusable typed column storage.

# Errors
Rejects incompatible column names, types, roles, or null values."]
            pub fn from_record_batch(record: &#runtime::row::decode::RecordBatch) -> #runtime::error::Result<Self> {
                Self::from_record_batch_with_policy(record, ::core::default::Default::default())
            }
            #[doc = "Decode named Arrow columns with an explicit retention policy.

# Errors
Rejects incompatible column names, types, roles, or null values."]
            pub fn from_record_batch_with_policy(record: &#runtime::row::decode::RecordBatch, policy: #runtime::buffer_policy::BufferPolicy) -> #runtime::error::Result<Self> {
                let prepared = #runtime::row::PreparedRecordBatch::new(record, Self::COLUMNS)?;
                let decoded = Self {
                    #(#buffers: <#types as #runtime::row::ArrowField>::decode_column(prepared.column(#indices)?, &Self::COLUMNS[#indices])?,)*
                    __policy: policy,
                    __thread: ::core::marker::PhantomData,
                };
                <Self as #runtime::batch::BatchEncode>::validate_schema(&decoded)?;
                ::core::result::Result::Ok(decoded)
            }
            #[doc = "Reconstruct all rows, borrowing any source slice fields from this batch."]
            pub fn to_owned_rows<#lifetime>(&#lifetime self) -> ::std::vec::Vec<#original_view> {
                ::core::iter::Iterator::collect(::core::iter::Iterator::map(self.iter_views(), |view| view.to_owned_row()))
            }
            #[doc = "Number of accumulated rows."]
            pub fn len(&self) -> ::core::primitive::usize { <#first_type as #runtime::row::Field>::len(&self.#first) }
            #[doc = "Number of accumulated rows."]
            pub fn row_count(&self) -> ::core::primitive::usize { self.len() }
            #[doc = "Whether the batch contains no rows."]
            pub fn is_empty(&self) -> ::core::primitive::bool { self.len() == 0 }
            #[doc = "Copy a source row into the typed column buffers."]
            pub fn push #impl_generics (&mut self, row: &#ident #type_generics) #where_clause {
                self.push_ref(&#row_ref { #(#names: #borrowed,)* });
            }
            #[doc = "Append source rows while reusing the batch's typed column allocations."]
            pub fn extend_rows #borrowed_impl (
                &mut self,
                rows: impl ::core::iter::IntoIterator<Item = &#lifetime #ident #type_generics>,
            ) where #ident #type_generics: #lifetime {
                for row in rows { self.push(row); }
            }
            #[doc = "Append borrowed bytes without allocating an owned string or vector per row."]
            pub fn push_ref(&mut self, row: &#row_ref<'_>) { #(<#types as #runtime::row::Field>::push(&mut self.#buffers, row.#names);)* }
            #[doc = "Inspect one row without copying string or binary payloads."]
            pub fn get(&self, index: ::core::primitive::usize) -> ::core::option::Option<#view<'_>> {
                if index >= self.len() { return ::core::option::Option::None; }
                ::core::option::Option::Some(#view { #(#names: <#types as #runtime::row::Field>::get(&self.#buffers, index)?,)* })
            }
            #[doc = "Inspect one row without copying string or binary payloads."]
            pub fn get_view(&self, index: ::core::primitive::usize) -> ::core::option::Option<#view<'_>> { self.get(index) }
            #[doc = "Iterate borrowed rows without allocation."]
            pub fn iter_views(&self) -> #iter<'_> { #iter { batch: self, index: 0 } }
            #[doc = "Iterate borrowed rows without allocation."]
            pub fn iter(&self) -> #iter<'_> { self.iter_views() }
            #[doc = "Return retained value-buffer capacity in bytes."]
            pub fn allocated_bytes(&self) -> ::core::primitive::usize { <Self as #runtime::buffer_policy::MemoryBuffer>::allocated_bytes(self) }
            #[doc = "Return retained row capacity."]
            pub fn row_capacity(&self) -> ::core::primitive::usize { <Self as #runtime::buffer_policy::MemoryBuffer>::row_capacity(self) }
            #[doc = "Discard rows using this batch's retention policy."]
            pub fn clear(&mut self) { let policy = self.__policy; self.clear_with_policy(policy); }
            #[doc = "Discard rows using an explicit aggregate retention policy."]
            pub fn clear_with_policy(&mut self, policy: #runtime::buffer_policy::BufferPolicy) { <Self as #runtime::buffer_policy::MemoryBuffer>::clear_with_policy(self, policy); }
            #[doc = "Discard rows while retaining all warmed allocations."]
            pub fn clear_unbounded(&mut self) { self.clear_with_policy(#runtime::buffer_policy::BufferPolicy::unbounded()); }
            #[doc = "Release unused capacity while preserving rows."]
            pub fn shrink_to_fit(&mut self) { <Self as #runtime::buffer_policy::MemoryBuffer>::shrink_to_fit(self); }
            #[doc = "Validate and snapshot a columnar insert.\n\n# Errors\nInvalid values or a row count exceeding u32 reject encoding."]
            pub fn encode_insert_request(&self) -> #runtime::error::Result<#runtime::proto::v1::InsertRequest> { <Self as #runtime::batch::BatchEncode>::encode_insert_request(self) }
            #[doc = "Validate and snapshot a tag-and-time-key delete.\n\n# Errors\nInvalid values or a row count exceeding u32 reject encoding."]
            pub fn encode_delete_request(&self) -> #runtime::error::Result<#runtime::proto::v1::DeleteRequest> { <Self as #runtime::batch::BatchEncode>::encode_delete_request(self) }
        }
        impl #runtime::buffer_policy::MemoryBuffer for #batch {
            fn clear(&mut self) { let policy = self.__policy; self.clear_with_policy(policy); }
            fn allocated_bytes(&self) -> ::core::primitive::usize { 0 #(+ #runtime::buffer_policy::MemoryBuffer::allocated_bytes(&self.#buffers))* }
            fn row_capacity(&self) -> ::core::primitive::usize { ::core::iter::Iterator::max(::core::iter::IntoIterator::into_iter([#(#runtime::buffer_policy::MemoryBuffer::row_capacity(&self.#buffers)),*])).unwrap_or(0) }
            fn clear_with_policy(&mut self, policy: #runtime::buffer_policy::BufferPolicy) {
                let policy = if policy.should_shrink(self.row_capacity(), self.allocated_bytes()) {
                    #runtime::buffer_policy::BufferPolicy::new(policy.max_retained_rows, policy.max_retained_bytes / #field_count)
                } else { #runtime::buffer_policy::BufferPolicy::unbounded() };
                #(#runtime::buffer_policy::MemoryBuffer::clear_with_policy(&mut self.#buffers, policy);)*
            }
            fn shrink_to_fit(&mut self) { #(#runtime::buffer_policy::MemoryBuffer::shrink_to_fit(&mut self.#buffers);)* }
        }
        impl #runtime::batch::BatchEncode for #batch {
            fn table_name(&self) -> &::core::primitive::str { #table }
            fn row_count(&self) -> ::core::primitive::usize { self.len() }
            fn validate_schema(&self) -> #runtime::error::Result<()> {
                if self.len() > ::core::primitive::u32::MAX as ::core::primitive::usize { return ::core::result::Result::Err(#runtime::row::schema_error("row count exceeds the protobuf u32 limit")); }
                #(if <#types as #runtime::row::Field>::len(&self.#buffers) != self.len() { return ::core::result::Result::Err(#runtime::row::schema_error("column row counts differ")); }
                <#types as #runtime::row::Field>::validate(&self.#buffers, &Self::COLUMNS[#indices]).map_err(#runtime::row::field_error)?;)*
                ::core::result::Result::Ok(())
            }
            fn encode_insert_request_for(&self, table_name: &::core::primitive::str) -> #runtime::error::Result<#runtime::proto::v1::InsertRequest> {
                <Self as #runtime::batch::BatchEncode>::validate_schema(self)?;
                if table_name.trim().is_empty() { return ::core::result::Result::Err(#runtime::row::schema_error("table name must not be empty")); }
                ::core::result::Result::Ok(#runtime::proto::v1::InsertRequest { table_name: ::std::borrow::ToOwned::to_owned(table_name), row_count: self.len() as ::core::primitive::u32,
                    columns: ::std::vec![#(<#types as #runtime::row::Field>::to_column(&self.#buffers, &Self::COLUMNS[#indices]).map_err(#runtime::row::field_error)?),*] })
            }
            fn encode_delete_request_for(&self, table_name: &::core::primitive::str) -> #runtime::error::Result<#runtime::proto::v1::DeleteRequest> {
                <Self as #runtime::batch::BatchEncode>::validate_schema(self)?;
                if table_name.trim().is_empty() { return ::core::result::Result::Err(#runtime::row::schema_error("table name must not be empty")); }
                ::core::result::Result::Ok(#runtime::proto::v1::DeleteRequest { table_name: ::std::borrow::ToOwned::to_owned(table_name), row_count: self.len() as ::core::primitive::u32,
                    key_columns: ::std::vec![#(<#key_types as #runtime::row::Field>::to_column(&self.#key_buffers, &Self::COLUMNS[#key_indices]).map_err(#runtime::row::field_error)?),*] })
            }
        }
        #[doc = "An exact-size allocation-free iterator over borrowed batch rows."]
        #vis struct #iter<#lifetime> { batch: &#lifetime #batch, index: ::core::primitive::usize }
        impl<#lifetime> ::core::iter::Iterator for #iter<#lifetime> {
            type Item = #view<#lifetime>;
            fn next(&mut self) -> ::core::option::Option<Self::Item> { let row = self.batch.get(self.index)?; self.index += 1; ::core::option::Option::Some(row) }
            fn size_hint(&self) -> (::core::primitive::usize, ::core::option::Option<::core::primitive::usize>) { let remaining = self.batch.len() - self.index; (remaining, ::core::option::Option::Some(remaining)) }
        }
        impl ::core::iter::ExactSizeIterator for #iter<'_> {}
        impl ::core::iter::FusedIterator for #iter<'_> {}
    }
}
