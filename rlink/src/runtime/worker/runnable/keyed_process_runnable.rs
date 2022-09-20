use std::borrow::{Borrow, BorrowMut};
use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, Record};
use crate::core::function::{KeyedProcessFunction, KeySelectorFunction};
use crate::core::operator::DefaultStreamOperator;
use crate::core::runtime::{CheckpointId, OperatorId, TaskId};
use crate::metrics::metric::Counter;
use crate::metrics::register_counter;
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

pub(crate) struct KeyedProcessRunnable {
    operator_id: OperatorId,
    task_id: TaskId,

    context: Option<RunnableContext>,

    stream_key_by: Option<DefaultStreamOperator<dyn KeySelectorFunction>>,
    stream_keyed_process: DefaultStreamOperator<dyn KeyedProcessFunction>,
    next_runnable: Option<Box<dyn Runnable>>,
    
    counter: Counter,
}

impl KeyedProcessRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_key_by: Option<DefaultStreamOperator<dyn KeySelectorFunction>>,
        stream_keyed_process: DefaultStreamOperator<dyn KeyedProcessFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        KeyedProcessRunnable {
            operator_id,
            task_id: TaskId::default(),
            context: None,
            stream_key_by,
            stream_keyed_process,
            next_runnable,
            counter: Counter::default(),
        }
    }
}

impl Runnable for KeyedProcessRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context)?;

        self.task_id = context.task_descriptor.task_id;

        self.context = Some(context.clone());

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_keyed_process.operator_fn.open(&fun_context)?;
        self.stream_key_by
            .as_mut()
            .map(|s: &mut DefaultStreamOperator<dyn KeySelectorFunction>| s.operator_fn.open(&fun_context));

        let fn_name = self.stream_keyed_process.operator_fn.as_ref().name();

        self.counter = register_counter(format!("KeyedReduce_{}", fn_name), self.task_id.to_tags());

        info!("KeyedProcessRunnable Opened. task_id={:?}", self.task_id);
        Ok(())
    }

    fn run(&mut self, mut element: Element) {
        match element {
            Element::Record(mut record) => {

                let key = match &self.stream_key_by {
                    Some(stream_key_by) => stream_key_by.operator_fn.get_key(record.borrow_mut()),
                    None => Record::with_capacity(0),
                };

                self.stream_keyed_process.operator_fn.as_mut().process(key, record);

                self.counter.fetch_add(1);
            }
            Element::Barrier(barrier) => {
                let checkpoint_id = barrier.checkpoint_id;
                let snapshot_context = {
                    let context = self.context.as_ref().unwrap();
                    context.checkpoint_context(self.operator_id, checkpoint_id, None)
                };
                self.checkpoint(snapshot_context);

                self.next_runnable.as_mut().unwrap().run(Element::Barrier(barrier));
            }
            _ => {
                self.next_runnable.as_mut().unwrap().run(element);
            }
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
        self.stream_key_by.as_mut().map(|s| s.operator_fn.close());
        self.stream_keyed_process.operator_fn.close()?;
        self.next_runnable.as_mut().unwrap().close()
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        // let handle = self
        //     .stream_keyed_process
        //     .operator_fn
        //     .snapshot_state(&snapshot_context)
        //     .unwrap_or(CheckpointHandle::default());
        //
        // let fn_handle = KeyedProcessCheckpointHandle::from(handle.handle.as_str());
        // self.completed_checkpoint_id = fn_handle.completed_checkpoint_id;
        //
        // let ck = Checkpoint {
        //     operator_id: snapshot_context.operator_id,
        //     task_id: snapshot_context.task_id,
        //     checkpoint_id: snapshot_context.checkpoint_id,
        //     completed_checkpoint_id: self.completed_checkpoint_id,
        //     handle: CheckpointHandle {
        //         handle: String::from("test"),
        //     },
        // };
        // submit_checkpoint(ck).map(|ck| {
        //     error!(
        //         "{:?} submit checkpoint error. maybe report channel is full, checkpoint: {:?}",
        //         snapshot_context.operator_id, ck
        //     )
        // });
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct KeyedProcessCheckpointHandle {
    #[serde(rename = "c_ck")]
    completed_checkpoint_id: Option<CheckpointId>,
}

impl KeyedProcessCheckpointHandle {
    pub fn new(
        completed_checkpoint_id: Option<CheckpointId>,
    ) -> Self {
        KeyedProcessCheckpointHandle {
            completed_checkpoint_id,
        }
    }
}

impl ToString for KeyedProcessCheckpointHandle {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

impl<'a> From<&'a str> for KeyedProcessCheckpointHandle {
    fn from(handle: &'a str) -> Self {
        if handle.is_empty() {
            KeyedProcessCheckpointHandle::default()
        } else if handle.starts_with("[") {
            KeyedProcessCheckpointHandle {
                completed_checkpoint_id: None,
            }
        } else {
            serde_json::from_str(handle).unwrap()
        }
    }
}