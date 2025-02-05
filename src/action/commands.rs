use super::{CreateOp, DelOp, ReadOp, PeerOp, UpdateOp};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateCommand {
    pub op: CreateOp,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadCommand {
    pub op: ReadOp,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateCommand {
    pub op: UpdateOp,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteCommand {
    pub op: DelOp,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerCommand {
    pub op: PeerOp,
}
