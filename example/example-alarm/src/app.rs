use std::time::Duration;

use rlink::channel::ChannelBaseOn;
use rlink::core::backend::CheckpointBackend;
use rlink::core::data_stream::{CoStream, TConnectedStreams, TDataStream, TKeyedStream};
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{FunctionProperties, Properties, SystemProperties};
use rlink::core::runtime::ClusterDescriptor;
use rlink::functions::flat_map::BroadcastFlagMapFunction;
use rlink::functions::key_selector::SchemaKeySelector;
use rlink::functions::reduce::{SchemaReduceFunction, sum};
use rlink::functions::window::SlidingEventTimeWindows;
use rlink::utils::process::parse_arg;
use rlink_connector_kafka::{BOOTSTRAP_SERVERS, GROUP_ID, KAFKA, OFFSET, TOPICS};
use rlink_connector_kafka::sink::builder::KafkaOutputFormatBuilder;
use rlink_connector_kafka::source::builder::KafkaInputFormatBuilder;
use crate::function::alert_flatmap_function::AlertFlatMapFunction;

use crate::function::key_selector_function::DynamicKeySelectorFunction;
use crate::function::rule_co_process_function::RuleCoProcessFunction;

const KAFKA_FN_SOURCE: &'static str = "kafka_fn_source";
const KAFKA_FN_RULE_SOURCE: &'static str = "kafka_fn_rule_source";
const KAFKA_FN_SINK: &'static str = "kafka_fn_sink";

#[derive(Clone, Debug)]
pub(crate) struct AlarmPlatformDemo {}

impl AlarmPlatformDemo {
    pub(crate) fn new() -> Self {
        AlarmPlatformDemo {}
    }
}

impl StreamApp for AlarmPlatformDemo {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_application_name("Real-Time Alarm Platform Demo");
        properties.set_pub_sub_channel_size(1024 * 1000);
        properties.set_pub_sub_channel_base(ChannelBaseOn::Unbounded);
        // properties.set_checkpoint_interval(Duration::from_secs(2 * 60));
        // properties.set_checkpoint(CheckpointBackend::MySql {
        //     endpoint: String::from("mysql://root@loaclhost:3306/rlink"),
        //     table: Some("rlink_ck".to_string()),
        // });

        let brokers = parse_arg("brokers").unwrap_or("172.37.101.16:9092".to_string());
        let event_topic = parse_arg("event_topic").unwrap_or("events".to_string());
        let rule_topic = parse_arg("rule_topic").unwrap_or("alarm_zgj_rules".to_string());
        let sink_topic = parse_arg("sink_topic").unwrap_or("alarm_zgj_alerts".to_string());
        let group_id = parse_arg("group_id").unwrap_or("zgj_test_1".to_string());
        let parallelism = parse_arg("parallelism").unwrap_or("1".to_string()).parse::<u16>().unwrap();

        let kafka_source_properties = {
            let mut source_properties = Properties::new();
            source_properties.set_u16("parallelism", parallelism);
            source_properties.set_str(TOPICS, event_topic.as_str());

            let mut kafka_properties = Properties::new();
            kafka_properties.set_str(BOOTSTRAP_SERVERS, brokers.as_str());
            kafka_properties.set_str(GROUP_ID, group_id.as_str());
            kafka_properties.set_str("message.timeout.ms", "30000");

            source_properties.extend_sub_properties(KAFKA, kafka_properties);

            // let offset_range = gen_kafka_offset_range(event_topic.as_str());
            // let offset_properties = offset_range.into();
            // source_properties.extend_sub_properties(OFFSET, offset_properties);

            source_properties
        };

        properties.extend_source(KAFKA_FN_SOURCE, kafka_source_properties);

        let kafka_rule_source_properties = {
            let mut source_properties = Properties::new();
            source_properties.set_u16("parallelism", parallelism);
            source_properties.set_str(TOPICS, rule_topic.as_str());

            let mut kafka_properties = Properties::new();
            kafka_properties.set_str(BOOTSTRAP_SERVERS, brokers.as_str());
            kafka_properties.set_str(GROUP_ID, group_id.as_str());
            source_properties.extend_sub_properties(KAFKA, kafka_properties);

            // let offset_range = gen_kafka_offset_range(rule_topic.as_str());
            // let offset_properties = offset_range.into();
            // source_properties.extend_sub_properties(OFFSET, offset_properties);

            source_properties
        };

        properties.extend_source(KAFKA_FN_RULE_SOURCE, kafka_rule_source_properties);

        let kafka_sink_properties = {
            let mut sink_properties = Properties::new();
            sink_properties.set_str(TOPICS, sink_topic.as_str());

            let mut kafka_properties = Properties::new();
            kafka_properties.set_str(BOOTSTRAP_SERVERS, brokers.as_str());

            sink_properties.extend_sub_properties(KAFKA, kafka_properties);
            sink_properties
        };

        properties.extend_sink(KAFKA_FN_SINK, kafka_sink_properties);

        info!("rlink properties: {}", properties.to_lines_string())
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let event_source = KafkaInputFormatBuilder::try_from(properties.to_source(KAFKA_FN_SOURCE))
            .unwrap().build(None);

        let rule_source = KafkaInputFormatBuilder::try_from(properties.to_source(KAFKA_FN_RULE_SOURCE))
            .unwrap().build(None);

        let sink = KafkaOutputFormatBuilder::try_from(properties.to_sink(KAFKA_FN_SINK))
            .unwrap().build();

        let rule_ds = env.register_source(rule_source)
            .flat_map(BroadcastFlagMapFunction::new());

        env.register_source(event_source)
            .connect(vec![CoStream::from(rule_ds)], RuleCoProcessFunction::new())
            .key_by(DynamicKeySelectorFunction::new())
            .flat_map(AlertFlatMapFunction::new())
            .add_sink(sink);
    }

    fn pre_worker_startup(&self, cluster_descriptor: &ClusterDescriptor) {
        info!("ClusterDescriptor {}", cluster_descriptor.to_string())
    }
}