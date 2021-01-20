use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::time::Duration;

use crate::api::element::{Element, Partition, Record};
use crate::api::function::{Context, Function, OutputFormat};
use crate::api::runtime::{ChannelKey, JobId, TaskId};
use crate::channel::ElementSender;
use crate::dag::execution_graph::ExecutionEdge;
use crate::pub_sub::{memory, network, ChannelType};

/// support job's Multiplexing, but only one channel mode(memory/network) support
pub(crate) struct SystemOutputFormat {
    task_id: TaskId,
    channel_type: ChannelType,
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

impl OutputFormat for SystemOutputFormat {
    fn open(&mut self, context: &Context) -> crate::api::Result<()> {
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

            let task_senders = memory::publish(&context.task_id, &memory_jobs);

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
            let task_senders = network::publish(&context.task_id, &network_jobs);

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

    fn write_record(&mut self, _record: Record) {}

    fn write_element(&mut self, mut element: Element) {
        match self.channel_type {
            ChannelType::Memory => {
                // Multiplexing publish
                if self.job_senders.len() == 1 {
                    let (_job_id, task_senders) = &self.job_senders[0];
                    let (task_id, sender) = &task_senders[0];
                    match element.borrow_mut() {
                        Element::Record(record) => {
                            record.channel_key = ChannelKey {
                                source_task_id: self.task_id,
                                target_task_id: *task_id,
                            };
                        }
                        _ => {}
                    }
                    sender.try_send_loop(element, Duration::from_secs(1))
                } else {
                    for (_job, task_senders) in &self.job_senders {
                        let (task_id, sender) = &task_senders[0];
                        match element.borrow_mut() {
                            Element::Record(record) => {
                                record.channel_key = ChannelKey {
                                    source_task_id: self.task_id,
                                    target_task_id: *task_id,
                                };
                            }
                            _ => {}
                        }
                        sender.try_send_loop(element.clone(), Duration::from_secs(1))
                    }
                }
            }
            ChannelType::Network => {
                if self.job_senders.len() == 1 {
                    let (_job_id, task_senders) = &self.job_senders[0];
                    let (_task_id, sender) =
                        task_senders.get(element.get_partition() as usize).unwrap();
                    sender.try_send_loop(element, Duration::from_secs(1));
                } else {
                    for (_job, task_senders) in &self.job_senders {
                        let (_task_id, sender) =
                            task_senders.get(element.get_partition() as usize).unwrap();
                        sender.try_send_loop(element.clone(), Duration::from_secs(1))
                    }
                }
            }
        }
    }

    fn close(&mut self) -> crate::api::Result<()> {
        Ok(())
    }
}

impl Function for SystemOutputFormat {
    fn get_name(&self) -> &str {
        "SystemOutputFormat"
    }
}
