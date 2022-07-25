use rlink::core::element::{Element, FnSchema, Record};
use rlink::core::function::{Context, OutputFormat};
use crate::stream_load::DorisConfigOption;

#[derive(Debug)]
pub struct DorisSink {
    options: DorisConfigOption
}

impl DorisSink {
    pub fn new(options: DorisConfigOption) -> DorisSink {
        DorisSink {
            options
        }
    }
}

impl OutputFormat for DorisSink {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        info!("doris sink open success.");
        Ok(())
    }

    fn write_record(&mut self, record: Record) {
        
    }

    fn write_element(&mut self, element: Element) {
        todo!()
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        todo!()
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        todo!()
    }
}