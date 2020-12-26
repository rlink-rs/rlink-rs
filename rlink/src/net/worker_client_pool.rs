use crate::channel::ElementSender;
use crate::net::worker_client::Client;
use crate::runtime::{JobDescriptor, TaskManagerDescriptor};
use crate::storage::metadata::MetadataLoader;
use crate::utils::get_runtime;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct WorkerClientPool {
    partition_num: u16,
    metadata_loader: MetadataLoader,
    chain_id: u32,
    task_number: u16,
    dependency_chain_id: u32,
    sender: ElementSender,
}

impl WorkerClientPool {
    pub fn new(
        partition_num: u16,
        metadata_loader: MetadataLoader,
        chain_id: u32,
        task_number: u16,
        dependency_chain_id: u32,
        sender: ElementSender,
    ) -> Self {
        WorkerClientPool {
            partition_num,
            metadata_loader,
            chain_id,
            task_number,
            dependency_chain_id,
            sender,
        }
    }

    pub fn build(&mut self) {
        let job_descriptor = self.metadata_loader.get_job_descriptor_from_cache();

        let current_task_manager =
            get_current_task_manager(&job_descriptor, self.chain_id, self.task_number)
                .expect("current TaskManager not found");
        let task_managers_in_current_node =
            get_task_mgr_in_current_node(&job_descriptor, &current_task_manager);

        let address = get_dep_task_mgr_addrs(
            job_descriptor,
            self.dependency_chain_id,
            task_managers_in_current_node,
        );

        let self_clone = self.clone();
        get_runtime().block_on(self_clone.build_pool(address))
    }

    pub async fn build_pool(&self, addrs: Vec<(String, SocketAddr)>) {
        let mut join_handlers = Vec::new();
        for (dep_task_mgr_id, addr) in &addrs {
            let self_clone = self.clone();
            let addr = addr.clone();
            let dep_chain_id = self.dependency_chain_id;
            let dep_task_mgr_id = dep_task_mgr_id.to_string();

            let join_handler =
                tokio::spawn(self_clone.poll_task(addr, dep_chain_id, dep_task_mgr_id));

            join_handlers.push(join_handler);
        }

        // must block the async method, ensure `tokio::runtime::Runtime` are not destroyed.
        for join_handler in join_handlers {
            join_handler
                .await
                .expect("The task being joined has panicked");
        }

        error!("client pool is end");
        // tokio::time::delay_for(Duration::from_secs(60 * 60)).await;
    }

    async fn poll_task(self, addr: SocketAddr, dep_chain_id: u32, dep_task_mgr_id: String) {
        let pool = self.clone();
        let dep_task_mgr_id = dep_task_mgr_id.as_str();
        loop {
            match pool.poll_task0(addr, dep_chain_id, dep_task_mgr_id).await {
                Ok(_) => break,
                Err(e) => {
                    error!(
                        "pool task error for client(ip={}, dependency_chain_id={}). {}",
                        addr, dep_chain_id, e
                    );
                    tokio::time::delay_for(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    }

    async fn poll_task0(
        &self,
        addr: SocketAddr,
        dep_chain_id: u32,
        dep_task_mgr_id: &str,
    ) -> anyhow::Result<()> {
        let mut client = Client::new(
            addr,
            self.chain_id,
            dep_chain_id,
            dep_task_mgr_id,
            self.partition_num,
        )
        .await?;
        let rt = client.send(5000, self.sender.clone()).await;
        client.close_rough();

        rt
    }
}

fn get_dep_task_mgr_addrs(
    job_descriptor: JobDescriptor,
    dependency_chain_id: u32,
    task_mgrs_in_current_node: Vec<TaskManagerDescriptor>,
) -> Vec<(String, SocketAddr)> {
    let mut dep_task_mgr_addrs = Vec::new();
    for task_manager_descriptor in &job_descriptor.task_managers {
        let dependency_tasks = task_manager_descriptor
            .chain_tasks
            .get(&dependency_chain_id);

        if dependency_tasks.is_some() {
            let task_manager_id = task_manager_descriptor.task_manager_id.as_str();
            let task_manager_address =
                SocketAddr::from_str(&task_manager_descriptor.task_manager_address)
                    .expect("parse address error");

            let is_same_node = task_mgrs_in_current_node
                .iter()
                .find(|tm| tm.task_manager_id.eq(task_manager_id))
                .is_some();

            let address = if is_same_node {
                let local_addr = SocketAddr::new(
                    IpAddr::from_str("127.0.0.1").unwrap(),
                    task_manager_address.port(),
                );

                info!("replace address {} to {}", task_manager_address, local_addr);
                local_addr
            } else {
                task_manager_address
            };

            info!(
                "add dependency task_manager_id={}, address={}",
                &task_manager_id, &address
            );
            dep_task_mgr_addrs.push((task_manager_id.to_string(), address));
        }
    }

    dep_task_mgr_addrs
}

fn get_current_task_manager(
    job_descriptor: &JobDescriptor,
    chain_id: u32,
    task_number: u16,
) -> Option<TaskManagerDescriptor> {
    for task_manager_descriptor in &job_descriptor.task_managers {
        if let Some(task_descriptors) = task_manager_descriptor.chain_tasks.get(&chain_id) {
            for task_descriptor in task_descriptors {
                if task_descriptor.task_number == task_number {
                    return Some(task_manager_descriptor.clone());
                }
            }
        }
    }

    None
}

fn get_task_mgr_in_current_node(
    job_descriptor: &JobDescriptor,
    current_task_manager_descriptor: &TaskManagerDescriptor,
) -> Vec<TaskManagerDescriptor> {
    let cur_addr_str = current_task_manager_descriptor
        .task_manager_address
        .as_str();
    let cur_addr = SocketAddr::from_str(cur_addr_str)
        .expect(format!("parse ip={} address error", cur_addr_str).as_str());

    let mut task_mgr_descs = Vec::new();
    for task_manager_descriptor in &job_descriptor.task_managers {
        let addr_str = task_manager_descriptor.task_manager_address.as_str();
        let addr = SocketAddr::from_str(addr_str)
            .expect(format!("parse ip={} address error", addr_str).as_str());

        if cur_addr.ip().eq(&addr.ip()) {
            task_mgr_descs.push(task_manager_descriptor.clone());
        }
    }

    task_mgr_descs
}
