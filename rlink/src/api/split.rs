use crate::api::properties::Properties;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct InputSplit {
    split_number: u32,
    properties: Properties,
}

impl InputSplit {
    pub fn new(split_number: u32, properties: Properties) -> Self {
        InputSplit {
            split_number,
            properties,
        }
    }

    pub fn get_split_number(&self) -> u32 {
        self.split_number
    }

    pub fn get_properties(&self) -> &Properties {
        &self.properties
    }
}

impl Default for InputSplit {
    fn default() -> Self {
        InputSplit::new(0, Properties::new())
    }
}

pub struct InputSplitAssigner {
    input_splits: Vec<InputSplit>,
}

impl InputSplitAssigner {
    pub fn new(input_splits: Vec<InputSplit>) -> Self {
        InputSplitAssigner { input_splits }
    }

    pub fn get_next_input_split(&mut self, _host: String, _task_id: usize) -> Option<InputSplit> {
        self.input_splits.pop()
    }
}
