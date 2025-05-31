#[derive(serde::Deserialize, Debug)]
pub struct IncomingMessage {
    pub partitionId: Option<usize>,
    pub message: String,
}
