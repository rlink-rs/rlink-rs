use std::collections::HashMap;
use std::sync::Arc;

use rlink_core::cmd::CmdFactory;

use crate::timer::WindowTimer;

#[derive(Clone)]
pub struct Cmd {
    cmd_factories: HashMap<String, Arc<Box<dyn CmdFactory>>>,
    timer: WindowTimer,
}

impl Cmd {
    pub fn new(cmd_factories: Vec<Arc<Box<dyn CmdFactory>>>, timer: WindowTimer) -> Self {
        let mut map = HashMap::new();
        for c in cmd_factories {
            map.insert(c.name().to_string(), c);
        }

        Self {
            cmd_factories: map,
            timer,
        }
    }
}
