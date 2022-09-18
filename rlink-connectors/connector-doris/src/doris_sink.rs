use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::time::Duration;
use reqwest::Client;
use rlink::channel::utils::handover::Handover;

use rlink::core::checkpoint::CheckpointFunction;
use rlink::core::data_types::{DataType, Schema};
use rlink::core::element::{Element, FnSchema, Record};
use rlink::core::function::{Context, NamedFunction, OutputFormat};
use rlink::core::runtime::TaskId;
use rlink::utils::date_time::current_timestamp_millis;
use rlink::utils::thread::{async_runtime, async_sleep};
use rlink::utils;

use crate::stream_load::{DorisConfigOption, LoadRequest};
use crate::stream_load;

pub struct DorisSink {
    options: DorisConfigOption,
    task_id: TaskId,
    schema: Schema,
    header: String,
    handover: Option<Handover>,
}

impl DorisSink {
    pub fn new(options: DorisConfigOption) -> DorisSink {
        DorisSink {
            options,
            task_id: TaskId::default(),
            schema: Schema::empty(),
            header: "".to_string(),
            handover: None,
        }
    }
}

impl OutputFormat for DorisSink {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        self.task_id = context.task_id;
        self.schema = context.input_schema.clone().into();
        self.handover = Some(Handover::new(self.name(), context.task_id.to_tags(), 10000));
        let client = Client::builder()
            .connect_timeout(Duration::from_millis(self.options.connect_timeout_ms as u64))
            .build().expect("http client build error");

        let field_str_names: Vec<String> = self.schema.fields().iter().map(|x| {x.name().to_string()}).collect();

        let mut task = DorisSinkTask::new(self.options.clone()
                                      , self.task_id
                                      , self.schema.clone()
                                      , self.header.clone()
                                      , client
                                      , self.handover.as_ref().unwrap().clone(),
                                          field_str_names);


        let field_names: Vec<String> = self.schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| format!("{}:{}", index, field.name()))
            .collect();
        self.header = field_names.join("|");

        utils::thread::spawn("doris-sink-block", move || {
            async_runtime("doris_sink").block_on(async {
                task.run().await;
            });
        });
        info!("Doris Sink open success");
        Ok(())
    }

    fn write_record(&mut self, record: Record) {
        self.handover.as_ref().unwrap().produce(record).unwrap();
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::Empty
    }
}

#[derive(Clone)]
pub struct DorisSinkTask {
    options: DorisConfigOption,
    task_id: TaskId,
    schema: Schema,
    header: String,
    client: Client,
    handover: Handover,
    field_names: Vec<String>,
}

impl DorisSinkTask {
    pub fn new(
        options: DorisConfigOption, task_id: TaskId, schema: Schema,
        header: String, client: Client, handover: Handover, field_names: Vec<String>
    ) -> DorisSinkTask {
        DorisSinkTask {
            options,
            task_id,
            schema,
            header,
            client,
            handover,
            field_names,
        }
    }

    pub async fn run(&mut self) {
        let mut self_clone = self.clone();
        loop {
            let http_client = &self.client;
            self_clone.batch_send(http_client).await;
        }
    }

    pub async fn batch_send(&mut self, client: &Client) {
        let start = utils::date_time::current_timestamp_millis();
        let mut values: Vec<HashMap<String, String>> = Vec::new();
        for _i in 0..self.options.sink_batch_size {
            match self.handover.try_poll_next() {
                Ok(mut record) => {
                    let reader = record.as_buffer().as_reader(self.schema.as_type_ids());

                    let mut map = HashMap::new();
                    for i in 0..self.schema.fields().len() {
                        let field = self.schema.field(i);
                        let field_str = match field.data_type() {
                            DataType::Boolean => reader.get_bool(i).unwrap().to_string(),
                            DataType::Int8 => reader.get_i8(i).unwrap().to_string(),
                            DataType::UInt8 => reader.get_u8(i).unwrap().to_string(),
                            DataType::Int16 => reader.get_i16(i).unwrap().to_string(),
                            DataType::UInt16 => reader.get_i16(i).unwrap().to_string(),
                            DataType::Int32 => reader.get_i32(i).unwrap().to_string(),
                            DataType::UInt32 => reader.get_u32(i).unwrap().to_string(),
                            DataType::Int64 => reader.get_i64(i).unwrap().to_string(),
                            DataType::UInt64 => reader.get_u64(i).unwrap().to_string(),
                            DataType::Float32 => reader.get_f32(i).unwrap().to_string(),
                            DataType::Float64 => reader.get_f64(i).unwrap().to_string(),
                            DataType::Binary => match reader.get_str(i) {
                                Ok(s) => s.to_owned(),
                                Err(_e) => format!("{:?}", reader.get_binary(i).unwrap()),
                            },
                            DataType::String => reader.get_str(i).unwrap().to_string(),
                        };
                        map.insert(field.name().to_string(), field_str);
                        // field_str_names.push(field.name().to_string());
                    }
                    let seq_col = &self.options.sink_default_seq_col;
                    if !seq_col.is_empty() {
                        map.insert(seq_col.to_string(), current_timestamp_millis().to_string());
                        // field_str_names.push(seq_col.to_string());
                    }
                    values.push(map);
                }
                Err(e) => {
                    async_sleep(Duration::from_secs(1)).await;
                    info!("poll next record from handover error: {}", e);
                }
            }
        }

        let req = stream_load::LoadRequest::new(
            values,
            self.options.database.to_string(),
            self.options.table.to_string(),
            self.field_names.clone(),
            false,
            self.options.sink_default_seq_col.to_string(),
            format!("{}_{}_rlink", current_timestamp_millis(), self.task_id.num_tasks()),
        );
        let res = stream_load::load(&self.options, req, client).await;
        match res {
            Ok(_r) => {
                info!("stream load time in ms: {}", utils::date_time::current_timestamp_millis() - start);
            }
            Err(e) => panic!("task id: {:?}, stream load error: {}", self.task_id, e)
        }
    }
}

impl NamedFunction for DorisSink {
    fn name(&self) -> &str {
        "DorisSink"
    }
}

impl CheckpointFunction for DorisSink {}