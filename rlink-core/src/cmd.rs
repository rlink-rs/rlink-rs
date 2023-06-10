use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;

pub trait CmdBody: Serialize + DeserializeOwned {}

pub trait CmdFactory
where
    Self: Send + Sync,
{
    fn name(&self) -> &str;
    fn interval(&self) -> Duration;
    fn create_server(&self) -> Box<dyn CmdServer>;
    fn create_client(&self) -> Box<dyn CmdClient>;
}

#[async_trait]
pub trait CmdServer
where
    Self: Send + Sync,
{
    async fn request(&self, data: &[u8]) -> anyhow::Result<String>;
}

#[async_trait]
pub trait CmdClient
where
    Self: Send + Sync,
{
    async fn submit(&self, data: &[u8]) -> anyhow::Result<()>;
}
