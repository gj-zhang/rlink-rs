use serbuffer_gen::{Codegen, SchemaBuilder};
use serbuffer_gen::DataType::{F64, I64, STRING, U32, U64};

fn main() {

    Codegen::out_dir("buffer_gen")
        .schema(
            SchemaBuilder::new("alarm_rule")
                .field("ruleId", U32)
                .field("ruleState", STRING)
                .field("groupingKeyNames", STRING)
                .field("unique", STRING)
                .field("aggregateFieldName", STRING)
                .field("aggregatorFunctionType", STRING)
                .field("limitOperatorType", STRING)
                .field("limit", F64)
                .field("windowMinutes", U32)
                .field("controlType", STRING)
                ,
        )
        .schema(
            SchemaBuilder::new("alarm_event")
                .field("transactionId", U32)
                .field("eventTime", U32)
                .field("payeeId", U32)
                .field("beneficiaryId", F64)
                .field("paymentAmount", STRING)
                .field("paymentType", STRING)
                .field("ingestionTimestamp", U32)

        )
        .gen()
        .expect("buffer gen error");
}