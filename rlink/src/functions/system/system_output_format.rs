use std::collections::HashMap;

use crate::channel::ElementSender;
use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, FnSchema, Partition, StreamStatus};
use crate::core::function::{Context, NamedFunction, OutputFormat};
use crate::core::properties::SystemProperties;
use crate::core::runtime::{ChannelKey, JobId, TaskId};
use crate::dag::execution_graph::ExecutionEdge;
use crate::pub_sub::{memory, network, ChannelType, DEFAULT_CHANNEL_SIZE};

/// support job's Multiplexing, but only one channel mode(memory/network) support
pub(crate) struct SystemOutputFormat {
    task_id: TaskId,
    channel_type: ChannelType,
    // Vec<JobId(self), Vec<(TaskId(child), ElementSender)>)>
    job_senders: Vec<(JobId, Vec<(TaskId, ElementSender)>)>,
}

impl SystemOutputFormat {
    pub fn new() -> Self {
        SystemOutputFormat {
            task_id: TaskId::default(),
            channel_type: ChannelType::Memory,
            job_senders: Vec::new(),
        }
    }
}

#[async_trait]
impl OutputFormat for SystemOutputFormat {
    async fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        self.task_id = context.task_id;

        let parents: Vec<String> = context
            .children
            .iter()
            .map(|(node, edge)| {
                format!(
                    "Node: {:?}--{:?}--> {:?}",
                    &context.task_id, edge, node.task_id
                )
            })
            .collect();
        info!("publish\n  {}", parents.join("\n  "));

        let channel_size = context
            .application_properties
            .get_pub_sub_channel_size()
            .unwrap_or(DEFAULT_CHANNEL_SIZE);

        let mut memory_jobs = Vec::new();
        let mut network_jobs = Vec::new();

        context
            .children
            .iter()
            .for_each(|(execution_node, execution_edge)| match execution_edge {
                ExecutionEdge::Memory => memory_jobs.push(execution_node.task_id.clone()),
                ExecutionEdge::Network => network_jobs.push(execution_node.task_id.clone()),
            });

        if memory_jobs.len() == 0 && network_jobs.len() == 0 {
            panic!("child job not found");
        }

        if memory_jobs.len() > 0 && network_jobs.len() > 0 {
            panic!("only one channel mode(memory/network) support");
        }

        if memory_jobs.len() > 0 {
            self.channel_type = ChannelType::Memory;

            let task_senders = memory::publish(&context.task_id, &memory_jobs, channel_size);

            let mut job_senders = HashMap::new();
            for (channel_key, sender) in task_senders {
                let target_task_id = channel_key.target_task_id;
                if context.task_id.task_number != target_task_id.task_number {
                    panic!("the task `task_number` conflict in memory channel");
                }

                job_senders
                    .entry(target_task_id.job_id)
                    .or_insert(Vec::new())
                    .push((target_task_id, sender));
            }

            for (job_id, senders) in job_senders {
                // todo support multiplexing
                if senders.len() != 1 {
                    panic!("only `Forward` support in memory channel");
                }
                self.job_senders.push((job_id, senders));
            }
        }

        if network_jobs.len() > 0 {
            self.channel_type = ChannelType::Network;
            let task_senders = network::publish(&context.task_id, &network_jobs, channel_size);

            let child_parallelism = task_senders[0].0.target_task_id.num_tasks;

            // group by `job_id`
            let mut job_senders = HashMap::new();
            for (channel_key, sender) in task_senders {
                let target_task_id = channel_key.target_task_id;
                if child_parallelism != target_task_id.num_tasks {
                    panic!("the task `num_tasks` conflict in network channel");
                }

                job_senders
                    .entry(target_task_id.job_id)
                    .or_insert(Vec::new())
                    .push((target_task_id, sender));
            }

            for (job_id, mut task_senders) in job_senders {
                if task_senders.len() != child_parallelism as usize {
                    panic!("the job `num_tasks` conflict in network channel");
                }

                // sort `task_senders` by `TaskId.task_number`
                task_senders.sort_by(|a, b| a.0.task_number.cmp(&b.0.task_number));
                for i in 0..task_senders.len() {
                    if task_senders[i].0.task_number as usize != i {
                        panic!("lost task");
                    }
                }

                self.job_senders.push((job_id, task_senders));
            }
        }
        Ok(())
    }

    async fn write_element(&mut self, mut element: Element) {
        match self.channel_type {
            ChannelType::Memory => {
                // Multiplexing publish
                if self.job_senders.len() == 1 {
                    let (_job_id, task_senders) = &self.job_senders[0];
                    let (task_id, sender) = &task_senders[0];

                    element.set_channel_key(ChannelKey {
                        source_task_id: self.task_id,
                        target_task_id: *task_id,
                    });
                    sender.send(element).await.unwrap()
                } else {
                    for (_job, task_senders) in &self.job_senders {
                        let (task_id, sender) = &task_senders[0];

                        element.set_channel_key(ChannelKey {
                            source_task_id: self.task_id,
                            target_task_id: *task_id,
                        });
                        sender.send(element.clone()).await.unwrap()
                    }
                }
            }
            ChannelType::Network => {
                if self.job_senders.len() == 1 {
                    let (_job_id, task_senders) = &self.job_senders[0];
                    let (_task_id, sender) =
                        task_senders.get(element.partition() as usize).unwrap();
                    sender.send(element).await.unwrap();
                } else {
                    for (_job, task_senders) in &self.job_senders {
                        let (_task_id, sender) =
                            task_senders.get(element.partition() as usize).unwrap();
                        sender.send(element.clone()).await.unwrap()
                    }
                }
            }
        }
    }

    async fn close(&mut self) -> crate::core::Result<()> {
        let stream_status = Element::StreamStatus(StreamStatus::new(0, true));
        for (_job_id, task_senders) in &self.job_senders {
            for (_task_id, sender) in task_senders {
                sender.send(stream_status.clone()).await.unwrap();
            }
        }
        // self.job_senders.iter().for_each(|(_job_id, task_senders)| {
        //     task_senders.iter().for_each(|(_task_id, sender)| {
        //         sender.send(stream_status.clone()).await.unwrap();
        //     });
        // });

        Ok(())
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        input_schema
    }
}

impl NamedFunction for SystemOutputFormat {
    fn name(&self) -> &str {
        "SystemOutputFormat"
    }
}

#[async_trait]
impl CheckpointFunction for SystemOutputFormat {
    async fn initialize_state(
        &mut self,
        _context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    ) {
    }

    async fn snapshot_state(
        &mut self,
        _context: &FunctionSnapshotContext,
    ) -> Option<CheckpointHandle> {
        None
    }
}
