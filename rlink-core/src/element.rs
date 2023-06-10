use std::any::Any;
use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;

pub trait Element:
    Clone + Any + Send + Sync + Debug + Serialize + DeserializeOwned + 'static
{
}

impl<T> Element for T where
    T: Clone + Any + Send + Sync + Debug + Serialize + DeserializeOwned + 'static
{
}
