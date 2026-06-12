use std::sync::Arc;

use tokio::sync::RwLock;

use crate::metrics::MetricsSnapshot;

/// Snapshot compartilhado — sampler escreve, GET /metrics lê sem rebuild.
pub struct MetricsCache {
    snap: RwLock<Option<Arc<MetricsSnapshot>>>,
}

impl MetricsCache {
    pub fn new() -> Self {
        Self {
            snap: RwLock::new(None),
        }
    }

    pub async fn store(&self, snap: MetricsSnapshot) {
        *self.snap.write().await = Some(Arc::new(snap));
    }

    pub async fn get(&self) -> Option<Arc<MetricsSnapshot>> {
        self.snap.read().await.clone()
    }
}
