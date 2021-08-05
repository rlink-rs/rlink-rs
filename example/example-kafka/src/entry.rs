#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SerDeEntity {
    pub timestamp: u64,
    pub name: String,
    pub value: i64,
}
