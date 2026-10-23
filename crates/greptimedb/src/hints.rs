//! GreptimeDB metadata names and ingestion directives.

/// Database routing metadata key.
pub const GREPTIME_DB_HEADER: &str = "x-greptime-db-name";
/// Authentication metadata key.
pub const GREPTIME_AUTH_HEADER: &str = "x-greptime-auth";
/// Comma-separated ingestion directives metadata key.
pub const GREPTIME_HINTS_HEADER: &str = "x-greptime-hints";
/// Explicit server retry decision metadata key.
pub const GREPTIME_RETRY_HINT_HEADER: &str = "x-greptime-err-retry-hint";
/// Structured server error metadata key.
pub const GREPTIME_INNER_ERROR_HEADER: &str = "INNER_ERROR_MSG";
/// Default GreptimeDB catalog.
pub const DEFAULT_CATALOG: &str = "greptime";
/// Default GreptimeDB schema.
pub const DEFAULT_SCHEMA: &str = "public";

/// How a table merges rows sharing the same primary key and timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMode {
    /// Replace the complete existing row.
    LastRow,
    /// Replace only columns whose incoming values are non-null.
    LastNonNull,
}

impl MergeMode {
    /// Return the value accepted by GreptimeDB's `merge_mode` hint.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::LastRow => "last_row",
            Self::LastNonNull => "last_non_null",
        }
    }
}

/// Ingestion directives encoded into the `x-greptime-hints` metadata header.
///
/// Keys retain their first insertion position, and repeated assignments replace
/// the previous value, including assignments made through different builders.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteHints {
    entries: Vec<(String, String)>,
}

impl WriteHints {
    /// Construct an empty collection of hints.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Enable or disable automatic table creation.
    #[must_use]
    pub fn with_auto_create_table(self, enabled: bool) -> Self {
        self.with_hint("auto_create_table", enabled.to_string())
    }

    /// Set a table's time to live, such as `7d`.
    ///
    /// # Panics
    /// Panics if the value cannot be represented as one unambiguous hint.
    #[must_use]
    pub fn with_ttl(self, ttl: impl Into<String>) -> Self {
        self.with_hint("ttl", ttl)
    }

    /// Enable or disable append mode.
    #[must_use]
    pub fn with_append_mode(self, enabled: bool) -> Self {
        self.with_hint("append_mode", enabled.to_string())
    }

    /// Set the strategy for merging rows with matching keys.
    #[must_use]
    pub fn with_merge_mode(self, mode: MergeMode) -> Self {
        self.with_hint("merge_mode", mode.as_str())
    }

    /// Select the physical table backing a logical metric table.
    ///
    /// # Panics
    /// Panics if the value cannot be represented as one unambiguous hint.
    #[must_use]
    pub fn with_physical_table(self, table: impl Into<String>) -> Self {
        self.with_hint("physical_table", table)
    }

    /// Assign a named directive, replacing any previous value for that key.
    ///
    /// The server separates directives by commas and each key from its value at
    /// the first equals sign. It does not support escaping. Keys must be nonempty
    /// printable ASCII without commas or equals signs; values must be printable
    /// ASCII without commas. Neither may have surrounding whitespace.
    ///
    /// # Panics
    /// Panics if either component violates these wire constraints. Use
    /// [`Self::try_with_hint`] for values obtained from external input.
    #[must_use]
    pub fn with_hint(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        match self.try_with_hint(key, value) {
            Ok(hints) => hints,
            Err(error) => panic!("{error}"),
        }
    }

    /// Assign a directive after checking that the server can decode it exactly.
    ///
    /// Equals signs are allowed in values because only the first equals sign
    /// separates the key from its value. Empty values are also representable.
    ///
    /// # Errors
    /// Returns [`crate::error::Error::InvalidConfig`] for ambiguous delimiters,
    /// empty keys, surrounding whitespace, or non-printable ASCII.
    pub fn try_with_hint(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> crate::error::Result<Self> {
        let key = key.into();
        let value = value.into();
        if key.is_empty() || !valid_component(&key) || key.contains('=') || !valid_component(&value)
        {
            return Err(crate::error::Error::InvalidConfig(
                "hint keys must be nonempty printable ASCII without commas or equals signs; \
                 values must be printable ASCII without commas; surrounding whitespace is forbidden"
                    .to_owned(),
            ));
        }
        self.insert(key, value);
        Ok(self)
    }

    /// Return whether the collection contains no directives.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Encode the hints in insertion order as comma-separated `key=value` pairs.
    ///
    /// An empty collection produces no header.
    #[must_use]
    pub fn encode_header_value(&self) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        let mut encoded = String::new();
        for (key, value) in &self.entries {
            if !encoded.is_empty() {
                encoded.push(',');
            }
            encoded.push_str(key);
            encoded.push('=');
            encoded.push_str(value);
        }
        Some(encoded)
    }

    /// Overlay request-specific directives on top of the client defaults.
    pub(crate) fn merged_with(&self, extra: &Self) -> Self {
        let mut merged = self.clone();
        for (key, value) in &extra.entries {
            merged.insert(key.clone(), value.clone());
        }
        merged
    }

    fn insert(&mut self, key: String, value: String) {
        if let Some((_, existing)) = self.entries.iter_mut().find(|(name, _)| *name == key) {
            *existing = value;
        } else {
            self.entries.push((key, value));
        }
    }
}

fn valid_component(component: &str) -> bool {
    component.trim() == component
        && component
            .bytes()
            .all(|byte| (b' '..=b'~').contains(&byte) && byte != b',')
}
