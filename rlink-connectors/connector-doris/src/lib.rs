#[macro_use]
extern crate log;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate serde;

pub mod doris_sink;
pub mod stream_load;
pub mod rest;
pub mod http;

/// connect properties
pub const DORIS_HEADER_DELETE_SIGN: &'static str = "__DORIS_DELETE_SIGN__";
pub const DORIS_HEADER_SEQ_COL: &'static str = "function_column.sequence_col";
pub const DORIS_HEADER_COLUMNS: &'static str = "columns";
pub const DORIS_HEADER_FORMAT_JSON: &'static str = "json";
pub const DORIS_HEADER_FORMAT_CSV: &'static str = "csv";
pub const DORIS_HEADER_FORMAT: &'static str = "format";
pub const DORIS_HEADER_STRIP_OUTER_ARRAY: &'static str = "strip_outer_array";
pub const DORIS_HEADER_COLUMN_SEPARATOR: &'static str = "column_separator";
pub const DORIS_HEADER_LINE_DELIMITER: &'static str = "line_delimiter";
pub const DORIS_HEADER_USERNAME: &'static str = "username";
pub const DORIS_HEADER_PASSWORD: &'static str = "password";
pub const DORIS_DATABASE: &'static str = "database";
pub const DORIS_TABLE: &'static str = "table";
pub const DORIS_LABEL_PREFIX: &'static str = "label";

/// socket properties
pub const DORIS_FE_NODES: &'static str = "fe_nodes";

pub const DORIS_CONNECT_TIMEOUT_MS: &'static str = "connect_timeout_ms";

