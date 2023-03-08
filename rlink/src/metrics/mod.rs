pub mod metric;
pub(crate) mod worker_proxy;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::ops::Deref;
use tokio::sync::RwLock;

use crate::utils::process::sys_info_metric_task;

pub use metric::register_counter;
pub use metric::register_gauge;
pub use metric::Tag;

#[derive(Clone)]
pub(crate) struct MetricHandle {
    prometheus_handle: PrometheusHandle,
}

impl MetricHandle {
    pub fn new(prometheus_handle: PrometheusHandle) -> Self {
        Self { prometheus_handle }
    }

    pub fn render(&self) -> String {
        self.prometheus_handle.render()
    }
}

lazy_static! {
    static ref METRIC_HADNLE: RwLock<Option<MetricHandle>> = RwLock::new(None);
}

pub(crate) async fn metric_handle() -> MetricHandle {
    let metric_handle: &RwLock<Option<MetricHandle>> = &*METRIC_HADNLE;
    let metric_handle = metric_handle.read().await;
    let metric_handle = metric_handle.deref().clone();
    metric_handle.unwrap()
}

pub(crate) async fn install_recorder(task_manager_id: &str) -> anyhow::Result<()> {
    let handle = PrometheusBuilder::new()
        .add_global_label("task_manager_id", task_manager_id)
        .install_recorder()
        .map(|h| MetricHandle::new(h))
        .map_err(|e| anyhow!(e))?;

    let metric_handle: &RwLock<Option<MetricHandle>> = &*METRIC_HADNLE;
    let mut metric_handle = metric_handle.write().await;
    *metric_handle = Some(handle);

    std::thread::spawn(|| sys_info_metric_task());

    return Ok(());
}
