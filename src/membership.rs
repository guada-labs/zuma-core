use std::collections::HashMap;
use std::fmt;
use thiserror::Error;
use tokio::sync::RwLock;

/// Placeholder identifier for a device/member in the mesh.
///
/// Will be finalized by the cryptographic identity decision (#8)
/// when Ed25519 public keys / certificate fingerprints are settled in F1.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MemberId(String);

impl MemberId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MemberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for MemberId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for MemberId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Membership status of a device in the swarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberStatus {
    Active,
    Revoked,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MembershipError {
    #[error("member already exists: {0}")]
    AlreadyExists(MemberId),

    #[error("member not found: {0}")]
    NotFound(MemberId),

    #[error("member is already revoked: {0}")]
    AlreadyRevoked(MemberId),

    #[error("storage error: {0}")]
    Storage(String),
}

/// Asynchronous trait defining device swarm membership operations.
///
/// Answers which devices belong to this mesh and how they are added or revoked.
/// Cryptographic validation, signed logs, and epoch transitions are deferred to F1.
#[async_trait::async_trait]
pub trait Membership: Send + Sync {
    /// Lists all currently active member identifiers in the mesh.
    async fn list_members(&self) -> Result<Vec<MemberId>, MembershipError>;

    /// Verifies whether the specified member is currently active in the mesh.
    async fn verify_member(&self, id: &MemberId) -> Result<bool, MembershipError>;

    /// Adds a new member to the mesh.
    async fn add_member(&self, id: MemberId) -> Result<(), MembershipError>;

    /// Revokes an existing member from the mesh.
    async fn revoke_member(&self, id: &MemberId) -> Result<(), MembershipError>;
}

/// In-memory mock implementation of [`Membership`] for tests and initial integration.
#[derive(Debug, Default)]
pub struct MemoryMembership {
    members: RwLock<HashMap<MemberId, MemberStatus>>,
}

impl MemoryMembership {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl Membership for MemoryMembership {
    async fn list_members(&self) -> Result<Vec<MemberId>, MembershipError> {
        let guard = self.members.read().await;
        let mut active: Vec<MemberId> = guard
            .iter()
            .filter(|(_, status)| **status == MemberStatus::Active)
            .map(|(id, _)| id.clone())
            .collect();
        active.sort();
        Ok(active)
    }

    async fn verify_member(&self, id: &MemberId) -> Result<bool, MembershipError> {
        let guard = self.members.read().await;
        Ok(matches!(guard.get(id), Some(MemberStatus::Active)))
    }

    async fn add_member(&self, id: MemberId) -> Result<(), MembershipError> {
        let mut guard = self.members.write().await;
        if matches!(guard.get(&id), Some(MemberStatus::Active)) {
            return Err(MembershipError::AlreadyExists(id));
        }
        guard.insert(id, MemberStatus::Active);
        Ok(())
    }

    async fn revoke_member(&self, id: &MemberId) -> Result<(), MembershipError> {
        let mut guard = self.members.write().await;
        match guard.get_mut(id) {
            Some(status) => {
                if *status == MemberStatus::Revoked {
                    return Err(MembershipError::AlreadyRevoked(id.clone()));
                }
                *status = MemberStatus::Revoked;
                Ok(())
            }
            None => Err(MembershipError::NotFound(id.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_revoke_member_lifecycle() {
        let membership = MemoryMembership::new();
        let dev1 = MemberId::new("node-inari");
        let dev2 = MemberId::new("node-predator");

        // Initially no active members
        assert_eq!(
            membership.list_members().await.unwrap(),
            Vec::<MemberId>::new()
        );
        assert!(!membership.verify_member(&dev1).await.unwrap());

        // Add first member
        membership.add_member(dev1.clone()).await.unwrap();
        assert!(membership.verify_member(&dev1).await.unwrap());
        assert_eq!(membership.list_members().await.unwrap(), vec![dev1.clone()]);

        // Duplicate active member fails
        let err = membership.add_member(dev1.clone()).await.unwrap_err();
        assert_eq!(err, MembershipError::AlreadyExists(dev1.clone()));

        // Add second member
        membership.add_member(dev2.clone()).await.unwrap();
        assert!(membership.verify_member(&dev2).await.unwrap());
        assert_eq!(
            membership.list_members().await.unwrap(),
            vec![dev1.clone(), dev2.clone()]
        );

        // Revoke first member
        membership.revoke_member(&dev1).await.unwrap();
        assert!(!membership.verify_member(&dev1).await.unwrap());
        assert_eq!(membership.list_members().await.unwrap(), vec![dev2.clone()]);

        // Revoking already revoked member returns AlreadyRevoked error
        let err = membership.revoke_member(&dev1).await.unwrap_err();
        assert_eq!(err, MembershipError::AlreadyRevoked(dev1.clone()));

        // Revoking non-existent member returns NotFound
        let unknown = MemberId::new("node-unknown");
        let err = membership.revoke_member(&unknown).await.unwrap_err();
        assert_eq!(err, MembershipError::NotFound(unknown));

        // Re-adding a previously revoked member restores it to active
        membership.add_member(dev1.clone()).await.unwrap();
        assert!(membership.verify_member(&dev1).await.unwrap());
    }
}
