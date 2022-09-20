use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{FnSchema, Record};
use crate::core::function::{Context, KeyedProcessFunction, NamedFunction};
use crate::core::runtime::CheckpointId;

#[derive(Debug)]
pub struct BaseKeyedProcessFunction {

}

impl BaseKeyedProcessFunction {

}

impl KeyedProcessFunction for BaseKeyedProcessFunction {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        unimplemented!("you should implement it by yourself")
    }

    fn process(&self, key: Record, record: Record) -> Option<Record> {
        unimplemented!("you should implement it by yourself")
    }

    fn close(&mut self) -> crate::core::Result<()> {
        unimplemented!("you should implement it by yourself")
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        unimplemented!("you should implement it by yourself")
    }

    fn parallelism(&self) -> u16 {
        unimplemented!("you should implement it by yourself")
    }
}

impl CheckpointFunction for BaseKeyedProcessFunction {
    fn consult_version(&mut self, context: &FunctionSnapshotContext, _handle: &Option<CheckpointHandle>) -> CheckpointId {
        todo!()
    }

    fn initialize_state(&mut self, _context: &FunctionSnapshotContext, _handle: &Option<CheckpointHandle>) {
        todo!()
    }

    fn snapshot_state(&mut self, _context: &FunctionSnapshotContext) -> Option<CheckpointHandle> {
        todo!()
    }
}

impl NamedFunction for BaseKeyedProcessFunction {
    fn name(&self) -> &str {
        "BaseKeyedProcessFunction"
    }
}