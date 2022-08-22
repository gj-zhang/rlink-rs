use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::time::Duration;

use rlink::core::properties::Properties;
use rlink::utils::date_time::{current_timestamp_millis, fmt_date_time};

use crate::{DORIS_CONNECT_TIMEOUT_MS, DORIS_HEADER_COLUMN_SEPARATOR, DORIS_HEADER_COLUMNS, DORIS_HEADER_DELETE_SIGN, DORIS_HEADER_FORMAT, DORIS_HEADER_FORMAT_JSON, DORIS_HEADER_LINE_DELIMITER, DORIS_HEADER_PASSWORD, DORIS_HEADER_SEQ_COL, DORIS_HEADER_STRIP_OUTER_ARRAY, DORIS_HEADER_USERNAME, DORIS_LABEL_PREFIX};
use crate::http;
use crate::rest::random_backend;

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
    pub(crate) sink_default_seq_col: String,
    pub(crate) database: String,
    pub(crate) table: String,
}

pub struct DorisConfigOptionBuilder {
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
    pub(crate) sink_default_seq_col: String,
    pub(crate) database: String,
    pub(crate) table: String,
}

impl DorisConfigOptionBuilder {
    pub fn new() -> Self {
        DorisConfigOptionBuilder {
            fe_nodes: "".to_string(),
            username: "".to_string(),
            password: "".to_string(),
            connect_timeout_ms: 0,
            read_timeout_ms: 0,
            sink_batch_size: 0,
            sink_max_retries: 0,
            sink_column_separator: ",".to_string(),
            sink_line_separator: "\r\n".to_string(),
            sink_format: SinkFormat::JSON,
            sink_strip_outer_array: "true".to_string(),
            sink_default_seq_col: "".to_string(),
            database: "".to_string(),
            table: "".to_string(),
        }
    }

    pub fn with_fe_nodes(&mut self, fe_nodes: String) -> &mut Self {
        self.fe_nodes = fe_nodes;
        self
    }

    pub fn with_username(&mut self, username: String) -> &mut Self {
        self.username = username;
        self
    }

    pub fn with_password(&mut self, password: String) -> &mut Self {
        self.password = password;
        self
    }

    pub fn with_connect_timeout_ms(&mut self, connect_time_ms: u32) -> &mut Self {
        self.connect_timeout_ms = connect_time_ms;
        self
    }

    pub fn with_read_timeout_ms(&mut self, read_timeout_ms: u32) -> &mut Self {
        self.read_timeout_ms = read_timeout_ms;
        self
    }

    pub fn with_sink_batch_size(&mut self, sink_batch_size: u32) -> &mut Self {
        self.sink_batch_size = sink_batch_size;
        self
    }

    pub fn with_sink_max_retries(&mut self, sink_max_retries: u32) -> &mut Self {
        self.sink_max_retries = sink_max_retries;
        self
    }

    pub fn with_sink_column_separator(&mut self, sink_column_separator: String) -> &mut Self {
        self.sink_column_separator = sink_column_separator;
        self
    }

    pub fn with_sink_line_separator(&mut self, sink_line_separator: String) -> &mut Self {
        self.sink_line_separator = sink_line_separator;
        self
    }

    pub fn with_sink_format(&mut self, sink_format: SinkFormat) -> &mut Self {
        self.sink_format = sink_format;
        self
    }

    pub fn with_sink_strip_outer_array(&mut self, sink_strip_outer_array: String) -> &mut Self {
        self.sink_strip_outer_array = sink_strip_outer_array;
        self
    }

    pub fn with_sink_default_seq_col(&mut self, sink_default_seq_col: String) -> &mut Self {
        self.sink_default_seq_col = sink_default_seq_col;
        self
    }

    pub fn with_database(&mut self, database: String) -> &mut Self {
        self.database = database;
        self
    }

    pub fn with_table(&mut self, table: String) -> &mut Self {
        self.table = table;
        self
    }

    pub fn build(self) -> DorisConfigOption {
        DorisConfigOption {
            fe_nodes: self.fe_nodes,
            username: self.username,
            password: self.password,
            connect_timeout_ms: self.connect_timeout_ms,
            read_timeout_ms: self.read_timeout_ms,
            sink_batch_size: self.sink_batch_size,
            sink_max_retries: self.sink_max_retries,
            sink_column_separator: self.sink_column_separator,
            sink_line_separator: self.sink_line_separator,
            sink_format: self.sink_format,
            sink_strip_outer_array: self.sink_strip_outer_array,
            sink_default_seq_col: self.sink_default_seq_col,
            database: self.database,
            table: self.table,
        }
    }
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
    label_prefix: String,
}

impl LoadRequest {
    pub fn new(value: Vec<HashMap<String, String>>, database: String, table: String,
               columns: Vec<String>, delete: bool, seq_col: String, label_prefix: String) -> LoadRequest {
        LoadRequest {
            value,
            database,
            table,
            columns,
            delete,
            seq_col,
            label_prefix,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(non_snake_case)]
pub struct RespContent {
    TxnId: i32,
    Label: String,
    Status: String,
    #[serde(default = "default")]
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
    #[serde(default = "default")]
    ErrorURL: String,
}

fn default() -> String {
    "DEFAULT".to_string()
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(non_snake_case)]
pub struct LoadResponse {
    status: i32,
    respMsg: String,
    respContent: RespContent,
}

impl LoadResponse {
    pub fn new0(status: i32, resp_msg: String, resp_content: RespContent) -> Self {
        LoadResponse {
            status,
            respMsg: resp_msg,
            respContent: resp_content,
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
    prop.set_duration(DORIS_CONNECT_TIMEOUT_MS, Duration::from_millis(options.connect_timeout_ms as u64));
    prop.set_str(DORIS_LABEL_PREFIX, load_request.label_prefix.as_str());
    // let labels = prop.get_string(DORIS_LABEL_PREFIX);
    // let label = match labels {
    //     Ok(l) => l,
    //     Err(_e) => {
    //         let mut t = current_timestamp_millis().to_string();
    //         t.push_str("_rlink");
    //         t
    //     }
    // };
    // prop.set_str(DORIS_LABEL_PREFIX, label.as_str());


    let start = current_timestamp_millis();
    let backend = random_backend(options);
    info!("random backend time: {}", current_timestamp_millis() - start);
    let url = format!("http://{}/api/{}/{}/_stream_load?", backend, load_request.database, load_request.table);
    let mut err = None;
    for i in 0..options.sink_max_retries {
        let res = load_batch(&value, prop, &url);
        match res {
            Ok(r) => {
                if r.Status != "Success".to_string() {
                    err = Some(anyhow!("stream load failure after retry {} times : {}", i, r.Message));
                    std::thread::sleep(Duration::from_secs(i as u64));
                } else {
                    info!("stream load success: {:?}", r);
                    break;
                }
            }
            Err(e) => {
                err = Some(e);
                std::thread::sleep(Duration::from_secs(i as u64));
            }
        }
    }
    match err {
        Some(e) => {
            return Err(e);
        }
        None => {}
    }

    return Ok("Success".to_string());
}

pub fn load_batch(value: &String, prop: &Properties, url: &String) -> anyhow::Result<RespContent> {
    let put_res = http::put::<String, RespContent>(prop, url, value.to_string())?;
    if put_res.Status == "Success".to_string() {
        Ok(put_res)
    } else {
        Err(anyhow!(put_res.Message))
    }
}
