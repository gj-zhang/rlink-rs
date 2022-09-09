use std::borrow::BorrowMut;
use std::time::Duration;

use rlink::core::backend::{CheckpointBackend, KeyedStateBackend};
use rlink::core::data_stream::TDataStream;
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{Properties, SystemProperties};
use rlink::core::runtime::ClusterDescriptor;
use rlink::functions::source::vec_source;
use rlink_connector_doris::{DORIS_DATABASE, DORIS_FE_NODES, DORIS_HEADER_PASSWORD, DORIS_HEADER_SEQ_COL, DORIS_HEADER_STRIP_OUTER_ARRAY, DORIS_HEADER_USERNAME, DORIS_TABLE};
use rlink_connector_doris::doris_sink::DorisSink;
use rlink_connector_doris::stream_load::{DorisConfigOptionBuilder, SinkFormat};
use rlink_example_utils::buffer_gen::model;
use rlink_example_utils::gen_record::gen_records;
use rlink::core::data_types::Schema;

#[derive(Debug, Clone)]
pub struct DorisApp {}

impl StreamApp for DorisApp {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_application_name("rlink-doris-example");
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_checkpoint_interval(Duration::from_secs(30));
        properties.set_checkpoint(CheckpointBackend::Memory);
        let fe_nodes = "FE_HOST:FE_PORT".to_string();
        let username = "USERNAME".to_string();
        let password = "PASSWORD".to_string();
        let strip_outer_array = "true".to_string();
        let database = "DATABASE".to_string();
        let table = "TABLE".to_string();
        properties.set_string(DORIS_FE_NODES.to_string(), fe_nodes);
        properties.set_string(DORIS_HEADER_USERNAME.to_string(), username);
        properties.set_string(DORIS_HEADER_PASSWORD.to_string(), password);
        properties.set_string(DORIS_HEADER_STRIP_OUTER_ARRAY.to_string(), strip_outer_array);
        // properties.set_string(DORIS_HEADER_SEQ_COL.to_string(), default_seq_col);
        properties.set_string(DORIS_DATABASE.to_string(), database);
        properties.set_string(DORIS_TABLE.to_string(), table);
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let fe_nodes = properties.get_string(DORIS_FE_NODES).expect("doris fe nodes must not be empty");
        let username = properties.get_string(DORIS_HEADER_USERNAME).expect("doris username must not be empty");
        let password = properties.get_string(DORIS_HEADER_PASSWORD).or::<anyhow::Error>(Ok("".to_string())).unwrap();
        let strip_outer_array = properties.get_string(DORIS_HEADER_STRIP_OUTER_ARRAY).or::<anyhow::Error>(Ok("true".to_string())).unwrap();
        let database = properties.get_string(DORIS_DATABASE).expect("doris database must not be empty");
        let table = properties.get_string(DORIS_TABLE).expect("doris table must not be empty");

        let mut options_builder = DorisConfigOptionBuilder::new();
        let options_builder_ref = options_builder.borrow_mut();
        options_builder_ref.with_fe_nodes(fe_nodes)
            .with_username(username)
            .with_password(password)
            .with_connect_timeout_ms(10_000)
            .with_sink_batch_size(10)
            .with_sink_max_retries(3)
            .with_sink_format(SinkFormat::JSON)
            .with_sink_strip_outer_array(strip_outer_array)
            .with_database(database)
            .with_table(table);

        let default_seq_col_result = properties.get_string(DORIS_HEADER_SEQ_COL);

        match default_seq_col_result {
            Ok(s) => {
                options_builder_ref.with_sink_default_seq_col(s);
            }
            Err(e) => {
                error!("{}", e);
            }
        }

        let options = options_builder.build();
        env.register_source(vec_source(
            gen_records(),
            Schema::from(&model::FIELD_METADATA),
            1,
        ))
            .add_sink(DorisSink::new(options))
    }

    fn pre_worker_startup(&self, cluster_descriptor: &ClusterDescriptor) {
        info!("{}", cluster_descriptor.coordinator_manager.metrics_address)
    }
}