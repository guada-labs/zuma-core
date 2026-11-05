use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;
use tokio::sync::RwLock;

/// Unique identifier for a document in the mesh.
///
/// Wrapped in a newtype to prevent stringly-typed APIs and enforce
/// identity conventions across the engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocumentId(String);

impl DocumentId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for DocumentId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for DocumentId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Monotonically increasing logical epoch/revision for a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Epoch(pub u64);

impl Epoch {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Epoch {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

/// A unit of change applied to a document.
///
/// `payload` uses [`Bytes`] to allow zero-copy sharing and transmission
/// across network and storage channels without unnecessary deep buffer clones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocChange {
    /// Expected base epoch or epoch assigned to this change.
    pub epoch: Epoch,
    /// Serialized change payload (CRDT delta, operation, or raw change bytes).
    pub payload: Bytes,
}

impl DocChange {
    pub fn new(epoch: Epoch, payload: impl Into<Bytes>) -> Self {
        Self {
            epoch,
            payload: payload.into(),
        }
    }
}

/// Serialized point-in-time snapshot of a document state and epoch.
///
/// Uses [`Bytes`] to allow sharing snapshot buffers across network and storage
/// layers without reallocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocSnapshot {
    pub id: DocumentId,
    pub epoch: Epoch,
    pub data: Bytes,
}

impl DocSnapshot {
    pub fn new(id: DocumentId, epoch: Epoch, data: impl Into<Bytes>) -> Self {
        Self {
            id,
            epoch,
            data: data.into(),
        }
    }
}

/// Document state representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    pub id: DocumentId,
    pub epoch: Epoch,
    pub state: Bytes,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DocError {
    #[error("document not found: {0}")]
    NotFound(DocumentId),

    #[error("epoch conflict for document {id}: expected epoch > {current:?}, got {actual:?}")]
    EpochConflict {
        id: DocumentId,
        current: Epoch,
        actual: Epoch,
    },

    #[error("invalid document payload: {0}")]
    InvalidData(String),

    #[error("storage error: {0}")]
    Storage(String),
}

/// Asynchronous, engine-agnostic document storage trait.
///
/// Acts as the state seam for the Zuma mesh. A concrete CRDT implementation
/// lands in F2; this trait decouples the replication loop from the underlying
/// storage engine.
///
/// # Ownership
/// - Read and snapshot operations return owned structures wrapping [`Bytes`] handles,
///   enabling cheap reference-counted buffer access without memory reallocation.
/// - Mutation operations (`apply`, `restore`) consume change and snapshot payloads
///   by value, transferring buffer ownership directly into the storage implementation.
#[async_trait::async_trait]
pub trait DocStore: Send + Sync {
    /// Reads the current state of a document if it exists.
    async fn read(&self, id: &DocumentId) -> Result<Option<Document>, DocError>;

    /// Applies a change to the document, advancing its epoch and updating its state.
    async fn apply(&self, id: &DocumentId, change: DocChange) -> Result<Document, DocError>;

    /// Takes a snapshot of the document at its current state and epoch.
    async fn snapshot(&self, id: &DocumentId) -> Result<DocSnapshot, DocError>;

    /// Restores or overwrites a document state from a snapshot.
    async fn restore(&self, snapshot: DocSnapshot) -> Result<Document, DocError>;
}

/// In-memory mock implementation of [`DocStore`] for tests and initial wiring.
#[derive(Debug, Default)]
pub struct MemoryDocStore {
    docs: RwLock<HashMap<DocumentId, Document>>,
}

impl MemoryDocStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl DocStore for MemoryDocStore {
    async fn read(&self, id: &DocumentId) -> Result<Option<Document>, DocError> {
        let guard = self.docs.read().await;
        Ok(guard.get(id).cloned())
    }

    async fn apply(&self, id: &DocumentId, change: DocChange) -> Result<Document, DocError> {
        let mut guard = self.docs.write().await;
        if let Some(doc) = guard.get_mut(id) {
            if change.epoch <= doc.epoch {
                return Err(DocError::EpochConflict {
                    id: id.clone(),
                    current: doc.epoch,
                    actual: change.epoch,
                });
            }
            doc.epoch = change.epoch;
            let mut new_state = Vec::with_capacity(doc.state.len() + change.payload.len());
            new_state.extend_from_slice(&doc.state);
            new_state.extend_from_slice(&change.payload);
            doc.state = Bytes::from(new_state);
            Ok(doc.clone())
        } else {
            let doc = Document {
                id: id.clone(),
                epoch: change.epoch,
                state: change.payload,
            };
            guard.insert(id.clone(), doc.clone());
            Ok(doc)
        }
    }

    async fn snapshot(&self, id: &DocumentId) -> Result<DocSnapshot, DocError> {
        let guard = self.docs.read().await;
        let doc = guard
            .get(id)
            .ok_or_else(|| DocError::NotFound(id.clone()))?;
        Ok(DocSnapshot {
            id: doc.id.clone(),
            epoch: doc.epoch,
            data: doc.state.clone(),
        })
    }

    async fn restore(&self, snapshot: DocSnapshot) -> Result<Document, DocError> {
        let mut guard = self.docs.write().await;
        let doc = Document {
            id: snapshot.id.clone(),
            epoch: snapshot.epoch,
            state: snapshot.data,
        };
        guard.insert(snapshot.id, doc.clone());
        Ok(doc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_apply_snapshot_restore_roundtrip() {
        let store = MemoryDocStore::new();
        let doc_id = DocumentId::new("config-doc");

        // Initial read returns None
        assert_eq!(store.read(&doc_id).await.unwrap(), None);

        // First apply creates document at epoch 1
        let c1 = DocChange::new(Epoch(1), Bytes::from_static(b"k1=v1;"));
        let doc = store.apply(&doc_id, c1).await.unwrap();
        assert_eq!(doc.epoch, Epoch(1));
        assert_eq!(doc.state, Bytes::from_static(b"k1=v1;"));

        // Second apply appends change and advances to epoch 2
        let c2 = DocChange::new(Epoch(2), Bytes::from_static(b"k2=v2;"));
        let doc = store.apply(&doc_id, c2).await.unwrap();
        assert_eq!(doc.epoch, Epoch(2));
        assert_eq!(doc.state, Bytes::from_static(b"k1=v1;k2=v2;"));

        // Stale epoch triggers EpochConflict
        let stale = DocChange::new(Epoch(2), Bytes::from_static(b"stale"));
        let err = store.apply(&doc_id, stale).await.unwrap_err();
        assert!(matches!(err, DocError::EpochConflict { .. }));

        // Take snapshot at epoch 2
        let snap = store.snapshot(&doc_id).await.unwrap();
        assert_eq!(snap.id, doc_id);
        assert_eq!(snap.epoch, Epoch(2));
        assert_eq!(snap.data, Bytes::from_static(b"k1=v1;k2=v2;"));

        // Restore snapshot into a fresh store
        let restore_store = MemoryDocStore::new();
        let restored_doc = restore_store.restore(snap).await.unwrap();
        assert_eq!(restored_doc.id, doc_id);
        assert_eq!(restored_doc.epoch, Epoch(2));
        assert_eq!(restored_doc.state, Bytes::from_static(b"k1=v1;k2=v2;"));

        // Verify read on restored store matches
        let read_doc = restore_store.read(&doc_id).await.unwrap().unwrap();
        assert_eq!(read_doc, restored_doc);
    }

    #[tokio::test]
    async fn test_snapshot_non_existent_doc_fails() {
        let store = MemoryDocStore::new();
        let doc_id = DocumentId::new("missing");
        let err = store.snapshot(&doc_id).await.unwrap_err();
        assert_eq!(err, DocError::NotFound(doc_id));
    }
}
