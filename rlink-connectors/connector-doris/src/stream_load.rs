use std::borrow::BorrowMut;
use std::collections::HashMap;

use anyhow::anyhow;
use serde_derive::{Deserialize, Serialize};

use rlink::core::properties::Properties;
use rlink::utils::date_time::{current_timestamp, current_timestamp_millis, fmt_date_time, FMT_DATE_TIME};

use crate::http;
use crate::rest::random_backend;

// const LOAD_URL_PATTERN: &'static str = "http://{}/api/{}/{}/_stream_load?";
const DORIS_HEADER_DELETE_SIGN: &'static str = "__DORIS_DELETE_SIGN__";
const DORIS_HEADER_SEQ_COL: &'static str = "function_column.sequence_col";
const DORIS_HEADER_COLUMNS: &'static str = "columns";
const DORIS_HEADER_FORMAT_JSON: &'static str = "json";
const DORIS_HEADER_FORMAT_CSV: &'static str = "csv";
const DORIS_HEADER_FORMAT: &'static str = "format";
const DORIS_HEADER_STRIP_OUTER_ARRAY: &'static str = "strip_outer_array";
const DORIS_HEADER_COLUMN_SEPARATOR: &'static str = "column_separator";
const DORIS_HEADER_LINE_DELIMITER: &'static str = "line_delimiter";
pub const DORIS_HEADER_USERNAME: &'static str = "username";
pub const DORIS_HEADER_PASSWORD: &'static str = "password";

#[derive(Debug)]
pub enum SinkFormat {
    JSON,
    CSV,
}

#[derive(Debug)]
pub struct DorisConfigOption {
    pub(crate) fe_nodes: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) connect_timeout_ms: u32,
    pub(crate) read_timeout_ms: u32,
    pub(crate) sink_batch_size: u32,
    pub(crate) sink_max_retries: u32,
    pub(crate) sink_column_separator: String,
    pub(crate) sink_line_separator: String,
    pub(crate) sink_format: SinkFormat,
    pub(crate) sink_strip_outer_array: String,
}

enum LoadStatus {
    Success,
    PublishTimeout,
}

#[derive(Debug)]
pub struct LoadRequest {
    value: Vec<HashMap<String, String>>,
    database: String,
    table: String,
    columns: Vec<String>,
    delete: bool,
    seq_col: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(non_snake_case)]
pub struct RespContent {
    TxnId: i32,
    Label: String,
    Status: String,
    ExistingJobStatus: String,
    Message: String,
    NumberTotalRows: i64,
    NumberLoadedRows: i64,
    NumberFilteredRows: i32,
    NumberUnselectedRows: i32,
    LoadBytes: i64,
    LoadTimeMs: i32,
    BeginTxnTimeMs: i32,
    StreamLoadPutTimeMs: i32,
    ReadDataTimeMs: i32,
    WriteDataTimeMs: i32,
    CommitAndPublishTimeMs: i32,
    ErrorURL: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(non_snake_case)]
pub struct LoadResponse {
    status: i32,
    respMsg: String,
    respContent: RespContent,
}

impl LoadResponse {
    pub fn new() -> Self {
        LoadResponse {
            status: 200,
            respMsg: "request body is empty".to_string(),
            respContent: RespContent::new(),
        }
    }
}

impl RespContent {
    pub fn new() -> Self {
        RespContent {
            TxnId: 0,
            Label: "".to_string(),
            Status: "200".to_string(),
            ExistingJobStatus: "".to_string(),
            Message: "request body is empty".to_string(),
            NumberTotalRows: 0,
            NumberLoadedRows: 0,
            NumberFilteredRows: 0,
            NumberUnselectedRows: 0,
            LoadBytes: 0,
            LoadTimeMs: 0,
            BeginTxnTimeMs: 0,
            StreamLoadPutTimeMs: 0,
            ReadDataTimeMs: 0,
            WriteDataTimeMs: 0,
            CommitAndPublishTimeMs: 0,
            ErrorURL: "".to_string(),
        }
    }
}

pub fn load(options: &DorisConfigOption, load_request: LoadRequest) -> anyhow::Result<String> {
    if load_request.value.is_empty() {
        return Ok("Success".to_string());
    }
    let mut props = Properties::new();
    let prop = props.borrow_mut();
    let mut columns = load_request.columns;
    if load_request.delete {
        columns.push(DORIS_HEADER_DELETE_SIGN.to_string())
    }
    if !load_request.seq_col.is_empty() {
        prop.set_str(DORIS_HEADER_SEQ_COL, load_request.seq_col.as_str())
    }
    prop.set_str(DORIS_HEADER_COLUMNS, &columns.join(","));
    let v = &load_request.value;
    let value = match options.sink_format {
        SinkFormat::JSON => {
            prop.set_str(DORIS_HEADER_FORMAT, DORIS_HEADER_FORMAT_JSON);
            prop.set_str(DORIS_HEADER_STRIP_OUTER_ARRAY, options.sink_strip_outer_array.as_str());
            serde_json::to_string(v)?
        }
        SinkFormat::CSV => {
            prop.set_str(DORIS_HEADER_COLUMN_SEPARATOR, options.sink_column_separator.as_str());
            prop.set_str(DORIS_HEADER_LINE_DELIMITER, options.sink_line_separator.as_str());
            let mut s = String::new();

            for x in v {
                for i in 0..columns.len() {
                    let col = columns.get(i).unwrap();
                    let col_value = x.get(col).unwrap();
                    s.push_str(col_value);
                }
            }
            s
        }
    };

    prop.set_str(DORIS_HEADER_USERNAME, options.username.as_str());
    prop.set_str(DORIS_HEADER_PASSWORD, options.password.as_str());

    let start = current_timestamp_millis();
    let backend = random_backend(options);
    log::info!("random backend time: {}", current_timestamp_millis() - start);
    let url = format!("http://{}/api/{}/{}/_stream_load?", backend, load_request.database, load_request.table);
    let mut err = None;
    for i in 0..options.sink_max_retries {
        let res = load_batch(&value, prop, &url);
        match res {
            Ok(r) => {
                if r.status != 200 {
                    err = Some(anyhow!("stream load failure after retry {} times : {}", i, r.respMsg));
                } else {
                    log::info!("stream load success: {:?}", r.respContent);
                    break;
                }
            }
            Err(e) => {
                err = Some(e);
            }
        }
    }
    match err {
        Some(e) => {
            log::error!("stream load failure: {}", e);
            return Err(e);
        }
        None => {}
    }

    return Ok("Success".to_string());
}

pub fn load_batch(value: &String, prop:  &mut Properties, url: &String) -> anyhow::Result<LoadResponse> {
    let label = prop.get_string("label");
    let label = match label {
        Ok(l) => l,
        Err(_e) => {
            fmt_date_time(current_timestamp(), FMT_DATE_TIME)
        }
    };
    prop.set_str("label", label.as_str());
    let put_res = http::put::<String, LoadResponse>(prop, url, value.to_string())?;
    if put_res.status == 200 {
        Ok(put_res)
    } else {
        Err(anyhow!(put_res.respMsg))
    }
}
