use std::time::Duration;
use rlink::core::backend::{CheckpointBackend, KeyedStateBackend};
use rlink::core::data_stream::TDataStream;
use rlink::core::data_types::Schema;
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{Properties, SystemProperties};
use rlink::core::runtime::ClusterDescriptor;
use rlink::functions::sink::print_sink;
use rlink::functions::source::vec_source;
use rlink_connector_doris::doris_sink::DorisSink;
use rlink_connector_doris::stream_load::{DorisConfigOption, SinkFormat};
use rlink_example_utils::gen_record::gen_records;
use rlink_example_utils::buffer_gen::model;

#[derive(Debug, Clone)]
pub struct DorisApp {}

impl StreamApp for DorisApp {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_application_name("rlink-doris-example");
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_checkpoint_interval(Duration::from_secs(30));
        properties.set_checkpoint(CheckpointBackend::Memory);

    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let options = DorisConfigOption::new(
            "172.35.88.131:8030".to_string(), "root".to_string(), "".to_string(),
            10_000, 10_000, 10, 1,
            "".to_string(), "".to_string(), SinkFormat::JSON, "true".to_string()
        );
        env.register_source(vec_source(
            gen_records(),
            Schema::from(&model::FIELD_METADATA),
            1
        ))
            .add_sink(DorisSink::new(options))
    }

    fn pre_worker_startup(&self, cluster_descriptor: &ClusterDescriptor) {
        println!("{}", cluster_descriptor.coordinator_manager.metrics_address)
    }
}