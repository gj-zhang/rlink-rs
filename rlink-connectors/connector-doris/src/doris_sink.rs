use std::collections::HashMap;

use rlink::core::checkpoint::CheckpointFunction;
use rlink::core::data_types::{DataType, Schema};
use rlink::core::element::{Element, FnSchema, Record};
use rlink::core::function::{Context, NamedFunction, OutputFormat};
use rlink::core::runtime::TaskId;
use rlink::utils::date_time::current_timestamp_millis;

use crate::stream_load::{DorisConfigOption, LoadRequest};
use crate::stream_load;

#[derive(Debug)]
pub struct DorisSink {
    options: DorisConfigOption,
    task_id: TaskId,
    schema: Schema,
    header: String,
    laster_timestamp: u64,
}

impl DorisSink {
    pub fn new(options: DorisConfigOption) -> DorisSink {
        DorisSink {
            options,
            task_id: TaskId::default(),
            schema: Schema::empty(),
            header: "".to_string(),
            laster_timestamp: 0,
        }
    }
}

impl OutputFormat for DorisSink {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        self.task_id = context.task_id;
        self.schema = context.input_schema.clone().into();
        let field_names: Vec<String> = self.schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| format!("{}:{}", index, field.name()))
            .collect();
        self.header = field_names.join("|");
        info!("Doris Sink open success");
        Ok(())
    }

    fn write_record(&mut self, mut record: Record) {
        let reader = record.as_buffer().as_reader(self.schema.as_type_ids());
        let mut field_str_names = Vec::new();
        let mut values: Vec<HashMap<String, String>> = Vec::new();
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
            field_str_names.push(field.name().to_string());
        }
        let seq_col = &self.options.sink_default_seq_col;
        if !seq_col.is_empty() {
            map.insert(seq_col.to_string(), current_timestamp_millis().to_string());
            field_str_names.push(seq_col.to_string());
        }
        values.push(map);

        let req = stream_load::LoadRequest::new(
            values,
            self.options.database.to_string(),
            self.options.table.to_string(),
            field_str_names,
            false,
            self.options.sink_default_seq_col.to_string(),
                    format!("{}_{}_rlink", current_timestamp_millis(), self.task_id.num_tasks())
        );
        let res = stream_load::load(&self.options, req);
        match res {
            Ok(_r) => {},
            Err(e) => panic!("task id: {:?}, stream load error: {}", self.task_id, e)
        }
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::Empty
    }
}

impl NamedFunction for DorisSink {
    fn name(&self) -> &str {
        "DorisSink"
    }
}

impl CheckpointFunction for DorisSink {}