use std::sync::Arc;

use crate::storage::Storage;

#[derive(Clone)]
pub struct SharedState {
    pub storage: Arc<dyn Storage>,
}

impl SharedState {
    pub fn new(storage: Arc<dyn Storage>) -> SharedState {
        SharedState { storage }
    }
}
