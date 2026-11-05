//! Zuma core engine library.
//!
//! Networking, sync engine, CRDT, and distributed storage seams for the Zuma device swarm.

pub mod doc;
pub mod membership;

pub use doc::{
    DocChange, DocError, DocSnapshot, DocStore, Document, DocumentId, Epoch, MemoryDocStore,
};
pub use membership::{MemberId, MemberStatus, Membership, MembershipError, MemoryMembership};
