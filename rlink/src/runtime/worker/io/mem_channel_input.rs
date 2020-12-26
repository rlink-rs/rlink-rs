use crate::api::backend::KeyedStateBackend;
use crate::api::checkpoint::{CheckpointHandle, CheckpointedFunction, FunctionSnapshotContext};
use crate::api::element::{Barrier, Element, Record};
use crate::api::function::{Context, Function};
use crate::api::input::{InputFormat, InputSplitSource};
use crate::api::properties::{Properties, SystemProperties};
use crate::api::split::{InputSplit, InputSplitAssigner};
use crate::api::window::Window;
use crate::channel::{
    mb, named_bounded, ElementReceiver, ElementSender, RecvTimeoutError, TryRecvError,
};
use crate::metrics::Tag;
use crate::runtime::worker::io::create_mem_channel;
use crate::storage::keyed_state::{ReducingState, ReducingStateWrap, StateKey};
use crate::utils;
use crate::utils::date_time::timestamp_str;
use metrics::gauge;
use std::str::FromStr;
use std::time::Duration;

pub(crate) const WINDOWS_FINISH_CHECKPOINT_ID: u64 = 0;

#[derive(Debug)]
pub(crate) struct MemChannelInputFormat {
    chain_id: u32,
    dependency_chain_id: u32,
    task_number: u16,

    /// receive from dependency chain
    receiver: Option<ElementReceiver>,

    job_id: String,
    state_mode: Option<KeyedStateBackend>,
    /// `Watermark` window to state iter thread
    window_to_state_sender: Option<ElementSender>,
    /// window's state iter to `InputFormat` thread
    stat_to_input_receiver: Option<ElementReceiver>,

    window_output_begin_ts: u64,
    window_start_flag: bool,

    checkpoint: Option<InputCheckpointed>,

    elapsed_guava: Option<String>,
}

impl MemChannelInputFormat {
    pub fn new(chain_id: u32, dependency_chain_id: u32) -> Self {
        MemChannelInputFormat {
            chain_id,
            dependency_chain_id,
            task_number: 0,
            receiver: None,
            job_id: "".to_string(),
            state_mode: None,
            window_to_state_sender: None,
            stat_to_input_receiver: None,
            window_output_begin_ts: 0,
            window_start_flag: false,
            checkpoint: None,
            elapsed_guava: None,
        }
    }

    fn state_handle(&mut self, finish_window: u64) {
        info!(
            "apply checkpoint `finish_window` timestamp: {}",
            finish_window
        );

        let state_receiver = {
            let tags = vec![
                Tag("chain_id".to_string(), format!("{}", self.chain_id)),
                Tag("task_number".to_string(), format!("{}", self.task_number)),
            ];

            let (sender, receiver) =
                named_bounded("MemChannelInputFormat_Window2State", tags, 5, mb(10));
            self.window_to_state_sender = Some(sender);
            receiver
        };

        let state_sender = {
            let tags = vec![
                Tag("chain_id".to_string(), format!("{}", self.chain_id)),
                Tag("task_number".to_string(), format!("{}", self.task_number)),
            ];

            let (sender, receiver) =
                named_bounded("MemChannelInputFormat_Stat2Input", tags, 1024, mb(10));
            self.stat_to_input_receiver = Some(receiver);
            sender
        };

        let dependency_chain_id = self.dependency_chain_id;
        let task_number = self.task_number;
        let state_mode = self.state_mode.as_ref().unwrap().clone();
        utils::spawn("memory-channel", move || loop {
            match state_receiver.recv_timeout(Duration::from_secs(120)) {
                Ok(element) => {
                    let watermark = element.as_watermark();
                    debug!(
                        "receiver windows {} to drop",
                        timestamp_str(watermark.timestamp)
                    );

                    let drop_windows = watermark.drop_windows.as_ref().unwrap();
                    for window in drop_windows {
                        debug!("begin iter window({:?})", window);

                        if window.max_timestamp() <= finish_window {
                            info!(
                                "checkpoint hand={}, window have been processed, skipped",
                                finish_window
                            );
                            continue;
                        }

                        let state_key =
                            StateKey::new(window.clone(), dependency_chain_id, task_number);
                        MemChannelInputFormat::iter_stat(
                            state_key,
                            state_mode.clone(),
                            &state_sender,
                        );
                        debug!("finish window({:?}) iter", window);
                    }

                    let all_window_finish_barrier = Barrier::new(WINDOWS_FINISH_CHECKPOINT_ID);
                    state_sender
                        .send_timeout(
                            Element::Barrier(all_window_finish_barrier),
                            Duration::from_secs(1),
                        )
                        .unwrap();
                }
                Err(RecvTimeoutError::Timeout) => {
                    warn!("MemoryInput state receive timeout");
                }
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("MemoryInput state channel has disconnected");
                }
            }
        });
    }

    fn iter_stat(state_key: StateKey, state_mode: KeyedStateBackend, state_sender: &ElementSender) {
        let reducing_state = ReducingStateWrap::new(&state_key, state_mode);

        match reducing_state {
            Some(reducing_state) => {
                {
                    let mut state_iter = reducing_state.iter();
                    loop {
                        match state_iter.next() {
                            Some((mut key, val)) => {
                                key.extend(val).expect("key value merge error");
                                key.trigger_window = Some(state_key.window.clone());
                                state_sender
                                    .try_send_loop(Element::Record(key), Duration::from_secs(1));
                            }
                            None => {
                                // send Barrier as the window's finish flag.
                                // use window maximum time as barrier checkpoint_id.
                                let checkpoint_id = state_key.window.max_timestamp();
                                let mut window_finish_barrier = Barrier::new(checkpoint_id);
                                window_finish_barrier.partition_num = state_key.task_number;

                                state_sender
                                    .send_timeout(
                                        Element::Barrier(window_finish_barrier),
                                        Duration::from_secs(1),
                                    )
                                    .unwrap();
                                break;
                            }
                        }
                    }

                    drop(state_iter);
                }

                reducing_state.destroy();
            }
            None => {
                warn!("try to iter window({:?}) but not found", &state_key);
            }
        }
    }
}

impl InputSplitSource for MemChannelInputFormat {
    fn create_input_splits(&self, min_num_splits: u32) -> Vec<InputSplit> {
        let mut input_splits = Vec::with_capacity(min_num_splits as usize);
        for partition_num in 0..min_num_splits {
            input_splits.push(InputSplit::new(partition_num, Properties::new()));
        }
        input_splits
    }

    fn get_input_split_assigner(&self, _input_splits: Vec<InputSplit>) -> InputSplitAssigner {
        unimplemented!()
    }
}

impl Function for MemChannelInputFormat {
    fn get_name(&self) -> &str {
        "MemChannelInputFormat"
    }
}

impl InputFormat for MemChannelInputFormat {
    fn open(&mut self, input_split: InputSplit, context: &Context) {
        self.chain_id = context.chain_id;
        self.dependency_chain_id = context.dependency_chain_id;
        self.task_number = context.task_number;

        // checkpoint init
        let mut input_checkpoint = InputCheckpointed::new(
            context.job_id.clone(),
            context.chain_id,
            context.task_number,
        );
        input_checkpoint.initialize_state(
            &context.get_checkpoint_context(),
            &context.checkpoint_handle,
        );
        self.checkpoint = Some(input_checkpoint);

        let state_mode = context
            .job_properties
            .get_keyed_state_backend()
            .unwrap_or(KeyedStateBackend::Memory);
        self.state_mode = Some(state_mode);
        self.job_id = context.job_id.clone();

        self.receiver = Some(create_mem_channel(self.dependency_chain_id, context.task_number).1);
        self.window_start_flag = false;

        self.state_handle(self.checkpoint.as_ref().unwrap().handle());

        self.elapsed_guava = Some(format!(
            "WindowDropElapsed_chain_id_{}_task_number_{}_Guava",
            self.chain_id, self.task_number,
        ));

        info!(
            "MemChannelInputFormat({}) open. `partition_num`: {}, `chain_id`={}, subscribe `dependency_chain_id`={}",
            self.get_name(),
            input_split.get_split_number(),
            self.chain_id,
            self.dependency_chain_id,
        );
    }

    fn reached_end(&self) -> bool {
        false
    }

    fn next_record(&mut self) -> Option<Record> {
        None
    }

    fn next_element(&mut self) -> Option<Element> {
        if !self.window_start_flag {
            // try receive `Watermark` from memory channel
            // notice: if current one window data consumption, the next window is read
            match self.receiver.as_ref().unwrap().try_recv() {
                Ok(element) => {
                    if element.is_watermark() {
                        debug!("begin send Watermark to sink");
                        self.window_to_state_sender
                            .as_ref()
                            .unwrap()
                            .try_send_loop(element, Duration::from_secs(1));
                        self.window_output_begin_ts = utils::date_time::current_timestamp_millis();
                        self.window_start_flag = true;
                    } else if element.is_record() {
                        panic!("Unsupported element type");
                    }
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    panic!("{} receiver recv `Disconnected`", self.get_name());
                }
            }
        }

        // try receive from state iter thread
        match self.stat_to_input_receiver.as_ref().unwrap().try_recv() {
            Ok(element) => {
                if element.is_barrier() {
                    let checkpoint_id = element.as_barrier().checkpoint_id;
                    if checkpoint_id == WINDOWS_FINISH_CHECKPOINT_ID {
                        // batch window's finish Barrier flag

                        let window_output_end_ts = utils::date_time::current_timestamp_millis();
                        let elapsed = window_output_end_ts - self.window_output_begin_ts;
                        debug!("window sink elapsed {}ms", elapsed);

                        gauge!(
                            "WindowDropElapsed",
                            elapsed as i64,
                            "manager_id" => crate::metrics::global_metrics::get_manager_id(),
                            "chain_id" => format!("{}",self.chain_id),
                            "task_number" => format!("{}",self.task_number),
                        );

                        self.window_start_flag = false;

                        // ignore batch window's finish Barrier flag
                        None
                    } else {
                        // the `Watermark` is per window's finish Barrier flag.
                        // update handle and emit `Watermark`, will trigger checkpoint operation in
                        // the `SourceRunnable`. see
                        // `self.checkpoint(row.as_barrier().checkpoint_id);` in SourceRunnable::run
                        let finish_window = checkpoint_id;
                        self.checkpoint.as_mut().unwrap().set_handle(finish_window);
                        debug!("update checkpoint handle={}", finish_window);

                        Some(element)
                    }
                } else {
                    Some(element)
                }
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                panic!(
                    "{} `stat_to_input_receiver` `Disconnected`",
                    self.get_name()
                );
            }
        }
    }

    fn close(&mut self) {}

    fn get_checkpoint(&mut self) -> Option<Box<&mut dyn CheckpointedFunction>> {
        match self.checkpoint.as_mut() {
            Some(checkpoint) => Some(Box::new(checkpoint)),
            None => None,
        }
    }
}

/// MemoryInputFormat Checkpoint
///
/// the `handle` is maximum timestamp for drop window,
/// always the `checkpoint_id` == `handle` in `CheckpointHandle`
#[derive(Debug)]
pub struct InputCheckpointed {
    job_id: String,
    chain_id: u32,
    task_number: u16,

    finish_window: u64,
}

impl InputCheckpointed {
    pub fn new(job_id: String, chain_id: u32, task_number: u16) -> Self {
        InputCheckpointed {
            job_id,
            chain_id,
            task_number,
            finish_window: 0,
        }
    }

    pub fn set_handle(&mut self, finish_window: u64) {
        self.finish_window = finish_window;
    }

    pub fn handle(&self) -> u64 {
        self.finish_window
    }
}

impl CheckpointedFunction for InputCheckpointed {
    fn initialize_state(
        &mut self,
        context: &FunctionSnapshotContext,
        handle: &Option<CheckpointHandle>,
    ) {
        if context.checkpoint_id > 0 && handle.is_some() {
            let data = handle.as_ref().unwrap();
            self.finish_window = u64::from_str(data.handle.as_str())
                .expect("`InputCheckpointed` handle must be u64");
        }
    }

    fn snapshot_state(&mut self, _context: &FunctionSnapshotContext) -> CheckpointHandle {
        CheckpointHandle {
            handle: self.finish_window.to_string(),
        }
    }
}
