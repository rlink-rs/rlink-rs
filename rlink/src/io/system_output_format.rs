use crate::api::element::{Element, Partition, Record};
use crate::api::function::{Context, Function, OutputFormat};
use crate::api::runtime::TaskId;
use crate::channel::ElementSender;
use crate::dag::execution_graph::ExecutionEdge;
use crate::io::pub_sub::{publish, ChannelType};
use std::collections::HashMap;
use std::time::Duration;

/// support job's Multiplexing, but only one channel mode(memory/network) support
pub struct SystemOutputFormat {
    channel_type: ChannelType,
    job_senders: Vec<(u32, Vec<(TaskId, ElementSender)>)>,
}

impl SystemOutputFormat {
    pub fn new() -> Self {
        SystemOutputFormat {
            channel_type: ChannelType::Memory,
            job_senders: Vec::new(),
        }
    }
}

impl OutputFormat for SystemOutputFormat {
    fn open(&mut self, context: &Context) {
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

            let task_senders = publish(&context.task_id, &memory_jobs, self.channel_type);

            let mut job_senders = HashMap::new();
            for (task_id, sender) in task_senders {
                if context.task_id.task_number != task_id.task_number {
                    panic!("the task `task_number` conflict in memory channel");
                }

                job_senders
                    .entry(task_id.job_id)
                    .or_insert(Vec::new())
                    .push((task_id, sender));
            }

            for (job_id, senders) in job_senders {
                if senders.len() != 1 {
                    panic!("only `Forward` support in memory channel");
                }
                self.job_senders.push((job_id, senders));
            }
        }

        if network_jobs.len() > 0 {
            self.channel_type = ChannelType::Memory;
            let task_senders = publish(&context.task_id, &network_jobs, self.channel_type);

            let child_parallelism = task_senders[0].0.num_tasks;

            // group by `job_id`
            let mut job_senders = HashMap::new();
            for (task_id, sender) in task_senders {
                if child_parallelism != task_id.num_tasks {
                    panic!("the task `num_tasks` conflict in network channel");
                }

                job_senders
                    .entry(task_id.job_id)
                    .or_insert(Vec::new())
                    .push((task_id, sender));
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
    }

    fn write_record(&mut self, _record: Record) {}

    fn write_element(&mut self, element: Element) {
        match self.channel_type {
            ChannelType::Memory => {
                // Multiplexing publish
                if self.job_senders.len() == 1 {
                    let (_job_id, task_senders) = &self.job_senders[0];
                    let (_task_id, sender) = &task_senders[0];
                    sender.try_send_loop(element.clone(), Duration::from_secs(1))
                } else {
                    for (_job, task_senders) in &self.job_senders {
                        let (_task_id, sender) = &task_senders[0];
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

    fn close(&mut self) {}
}

impl Function for SystemOutputFormat {
    fn get_name(&self) -> &str {
        "SystemOutputFormat"
    }
}
