use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use rlink::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use rlink::core::runtime::TaskId;

#[derive(Debug, Clone)]
pub struct KafkaCheckpointFunction {
    pub(crate) state_recorder: Option<KafkaSourceStateRecorder>,
    #[allow(dead_code)]
    pub(crate) application_id: String,
    #[allow(dead_code)]
    pub(crate) task_id: TaskId,
    topic: String,
    partition: i32,
}

impl KafkaCheckpointFunction {
    pub fn new(application_id: String, task_id: TaskId, topic: &str, partition: i32) -> Self {
        KafkaCheckpointFunction {
            state_recorder: None,
            application_id,
            task_id,
            topic: topic.to_string(),
            partition,
        }
    }

    pub fn as_state_mut(&mut self) -> &mut KafkaSourceStateRecorder {
        self.state_recorder.as_mut().unwrap()
    }
}

impl CheckpointFunction for KafkaCheckpointFunction {
    fn initialize_state(
        &mut self,
        context: &FunctionSnapshotContext,
        handle: &Option<CheckpointHandle>,
    ) {
        self.state_recorder = Some(KafkaSourceStateRecorder::new(
            self.topic.as_str(),
            self.partition,
        ));
        info!("Checkpoint initialize, context: {:?}", context);

        if context.checkpoint_id.is_default() || handle.is_none() {
            return;
        }

        let handle = handle.as_ref().unwrap();

        let state_cache = self.state_recorder.as_mut().unwrap();
        state_cache
            .update_from_snapshot(handle.handle.as_str())
            .unwrap();

        info!(
            "load state value from checkpoint({:?}): {:?}",
            context.checkpoint_id, handle.handle
        );
    }

    fn snapshot_state(&mut self, context: &FunctionSnapshotContext) -> Option<CheckpointHandle> {
        let handle = self.state_recorder.as_ref().unwrap().snapshot();
        debug!("Checkpoint snapshot: {:?}, context: {:?}", handle, context);

        Some(CheckpointHandle { handle })
    }
}

#[derive(Serialize, Deserialize)]
struct OffsetSnapshot<'a> {
    topic: &'a str,
    partition: i32,
    offset: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct KafkaSourceStateRecorder {
    topic: String,
    partition: i32,
    offset: Arc<AtomicI64>,
}

impl KafkaSourceStateRecorder {
    pub fn new(topic: &str, partition: i32) -> Self {
        KafkaSourceStateRecorder {
            topic: topic.to_string(),
            partition,
            offset: Arc::new(AtomicI64::new(i64::MIN)),
        }
    }

    pub fn update(&self, offset: i64) {
        self.offset.store(offset, Ordering::Relaxed);
    }

    pub fn update_from_snapshot(&self, snapshot_handle: &str) -> anyhow::Result<()> {
        let offset_snapshot: OffsetSnapshot = serde_json::from_str(snapshot_handle)?;

        if !offset_snapshot.topic.eq(self.topic.as_str())
            || offset_snapshot.partition != self.partition
        {
            return Err(anyhow!("Does not belong to the checkpoint of the task"));
        }

        self.update(offset_snapshot.offset.unwrap_or(i64::MIN));
        Ok(())
    }

    pub fn snapshot(&self) -> String {
        let offset = {
            let offset = self.offset.load(Ordering::Relaxed);
            if offset == i64::MIN {
                None
            } else {
                Some(offset)
            }
        };

        serde_json::to_string(&OffsetSnapshot {
            topic: self.topic.as_str(),
            partition: self.partition,
            offset,
        })
        .unwrap()
    }

    pub fn get(&self) -> Option<i64> {
        let offset = self.offset.load(Ordering::Relaxed);
        if offset == i64::MIN {
            None
        } else {
            Some(offset)
        }
    }
}
