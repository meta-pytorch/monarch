/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Namespace-based mesh registry for discovering and connecting to remote meshes.
//!
//! This module provides a namespace abstraction for registering and looking up
//! meshes (host, proc, and actor) by name. Names are unique within a namespace,
//! and the full key is composed of (namespace, kind, name).
//!
//! # Example
//!
//! ```ignore
//! let ns = InMemoryNamespace::new("my.prefix.tier");
//!
//! // Register meshes
//! ns.register("workers", &actor_mesh_ref).await?;
//! ns.register("procs", &proc_mesh_ref).await?;
//!
//! // Lookup meshes
//! let actors: ActorMeshRef<MyActor> = ns.get("workers").await?;
//! let procs: ProcMeshRef = ns.get("procs").await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;

use async_trait::async_trait;
use hyperactor::actor::Referable;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::v1::ActorMeshRef;
use crate::v1::HostMeshRef;
use crate::v1::ProcMeshRef;

/// The kind of mesh being registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MeshKind {
    Host,
    Proc,
    Actor,
}

impl MeshKind {
    /// Returns the string representation of this kind.
    pub fn as_str(&self) -> &'static str {
        match self {
            MeshKind::Host => "host",
            MeshKind::Proc => "proc",
            MeshKind::Actor => "actor",
        }
    }
}

impl std::fmt::Display for MeshKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Errors that can occur during namespace operations.
#[derive(Debug, thiserror::Error)]
pub enum NamespaceError {
    #[error("serialization failed: {0}")]
    SerializationError(String),
    #[error("deserialization failed: {0}")]
    DeserializationError(String),
    #[error("operation failed: {0}")]
    OperationError(String),
    #[error("not found: {0}")]
    NotFound(String),
}

/// Trait for mesh types that can be registered in a namespace.
///
/// This trait is implemented for `HostMeshRef`, `ProcMeshRef`, and `ActorMeshRef<A>`,
/// allowing them to be registered and looked up using the generic `register` and `get`
/// methods on `Namespace`.
pub trait Registrable: Serialize + DeserializeOwned + Send + Sync {
    /// The kind of mesh this type represents.
    fn kind() -> MeshKind;
}

impl Registrable for HostMeshRef {
    fn kind() -> MeshKind {
        MeshKind::Host
    }
}

impl Registrable for ProcMeshRef {
    fn kind() -> MeshKind {
        MeshKind::Proc
    }
}

impl<A: Referable> Registrable for ActorMeshRef<A> {
    fn kind() -> MeshKind {
        MeshKind::Actor
    }
}

/// A namespace for registering and looking up meshes.
///
/// Namespaces provide isolation for mesh names. A mesh registered as "foo" in
/// namespace "a.b.c" does not conflict with "foo" in namespace "x.y.z".
///
/// The full key for a registered mesh is `{namespace}.{kind}.{name}`, e.g.,
/// `my.namespace.actor.workers`.
#[async_trait]
pub trait Namespace {
    /// The namespace name (e.g., "my.namespace").
    fn name(&self) -> &str;

    /// Register a mesh under the given name.
    ///
    /// The mesh type determines the kind (host, proc, or actor) automatically
    /// via the `Registrable` trait.
    async fn register<T: Registrable>(&self, name: &str, mesh: &T) -> Result<(), NamespaceError>;

    /// Lookup a mesh by name.
    ///
    /// The mesh type must be specified (e.g., `ns.get::<ProcMeshRef>("name")`).
    async fn get<T: Registrable>(&self, name: &str) -> Result<T, NamespaceError>;

    /// Unregister a mesh by name.
    async fn unregister<T: Registrable>(&self, name: &str) -> Result<(), NamespaceError>;

    /// Check if a mesh exists in this namespace.
    async fn contains<T: Registrable>(&self, name: &str) -> Result<bool, NamespaceError>;
}

/// An in-memory namespace implementation for testing.
#[derive(Debug)]
pub struct InMemoryNamespace {
    namespace_name: String,
    data: RwLock<HashMap<String, Vec<u8>>>,
}

impl InMemoryNamespace {
    /// Create a new in-memory namespace with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            namespace_name: name.into(),
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Build the full key string: `{namespace}.{kind}.{name}`.
    fn full_key(&self, kind: MeshKind, name: &str) -> String {
        format!("{}.{}.{}", self.namespace_name, kind.as_str(), name)
    }
}

#[async_trait]
impl Namespace for InMemoryNamespace {
    fn name(&self) -> &str {
        &self.namespace_name
    }

    async fn register<T: Registrable>(&self, name: &str, mesh: &T) -> Result<(), NamespaceError> {
        let data = serde_json::to_vec(mesh)
            .map_err(|e| NamespaceError::SerializationError(e.to_string()))?;
        let key = self.full_key(T::kind(), name);
        self.data
            .write()
            .map_err(|e| NamespaceError::OperationError(e.to_string()))?
            .insert(key.clone(), data);
        tracing::debug!(
            key = %key,
            "registered mesh to in-memory namespace"
        );
        Ok(())
    }

    async fn get<T: Registrable>(&self, name: &str) -> Result<T, NamespaceError> {
        let key = self.full_key(T::kind(), name);
        let data = self
            .data
            .read()
            .map_err(|e| NamespaceError::OperationError(e.to_string()))?
            .get(&key)
            .cloned()
            .ok_or(NamespaceError::NotFound(key))?;
        serde_json::from_slice(&data)
            .map_err(|e| NamespaceError::DeserializationError(e.to_string()))
    }

    async fn unregister<T: Registrable>(&self, name: &str) -> Result<(), NamespaceError> {
        let key = self.full_key(T::kind(), name);
        self.data
            .write()
            .map_err(|e| NamespaceError::OperationError(e.to_string()))?
            .remove(&key);
        tracing::debug!(
            key = %key,
            "unregistered mesh from in-memory namespace"
        );
        Ok(())
    }

    async fn contains<T: Registrable>(&self, name: &str) -> Result<bool, NamespaceError> {
        let key = self.full_key(T::kind(), name);
        Ok(self
            .data
            .read()
            .map_err(|e| NamespaceError::OperationError(e.to_string()))?
            .contains_key(&key))
    }
}

/// A shared namespace that can be either in-memory or SMC-backed.
#[derive(Debug)]
pub enum SharedNamespaceInner {
    /// In-memory namespace for testing.
    InMemory(InMemoryNamespace),
    /// SMC-backed namespace for production (fbcode only).
    #[cfg(fbcode_build)]
    Smc(crate::v1::meta::SmcNamespace),
}

#[async_trait]
impl Namespace for SharedNamespaceInner {
    fn name(&self) -> &str {
        match self {
            SharedNamespaceInner::InMemory(ns) => ns.name(),
            #[cfg(fbcode_build)]
            SharedNamespaceInner::Smc(ns) => ns.name(),
        }
    }

    async fn register<T: Registrable>(&self, name: &str, mesh: &T) -> Result<(), NamespaceError> {
        match self {
            SharedNamespaceInner::InMemory(ns) => ns.register(name, mesh).await,
            #[cfg(fbcode_build)]
            SharedNamespaceInner::Smc(ns) => ns.register(name, mesh).await,
        }
    }

    async fn get<T: Registrable>(&self, name: &str) -> Result<T, NamespaceError> {
        match self {
            SharedNamespaceInner::InMemory(ns) => ns.get(name).await,
            #[cfg(fbcode_build)]
            SharedNamespaceInner::Smc(ns) => ns.get(name).await,
        }
    }

    async fn unregister<T: Registrable>(&self, name: &str) -> Result<(), NamespaceError> {
        match self {
            SharedNamespaceInner::InMemory(ns) => ns.unregister::<T>(name).await,
            #[cfg(fbcode_build)]
            SharedNamespaceInner::Smc(ns) => ns.unregister::<T>(name).await,
        }
    }

    async fn contains<T: Registrable>(&self, name: &str) -> Result<bool, NamespaceError> {
        match self {
            SharedNamespaceInner::InMemory(ns) => ns.contains::<T>(name).await,
            #[cfg(fbcode_build)]
            SharedNamespaceInner::Smc(ns) => ns.contains::<T>(name).await,
        }
    }
}

/// Type alias for a shared namespace.
pub type SharedNamespace = Arc<SharedNamespaceInner>;

/// Global namespace holder.
static GLOBAL_NAMESPACE: OnceLock<SharedNamespace> = OnceLock::new();

/// Set the global namespace. Can only be called once.
pub fn set_global_namespace(namespace: SharedNamespace) -> Result<(), SharedNamespace> {
    GLOBAL_NAMESPACE.set(namespace)
}

/// Get the global namespace, if configured.
pub fn global_namespace() -> Option<&'static SharedNamespace> {
    GLOBAL_NAMESPACE.get()
}

/// Create a shared in-memory namespace.
pub fn create_in_memory_namespace(name: impl Into<String>) -> SharedNamespace {
    Arc::new(SharedNamespaceInner::InMemory(InMemoryNamespace::new(name)))
}

/// Create a shared SMC namespace.
#[cfg(fbcode_build)]
pub fn create_smc_namespace(
    fb: fbinit::FacebookInit,
    namespace_name: String,
    tier: String,
) -> Result<SharedNamespace, NamespaceError> {
    let smc_ns = crate::v1::meta::SmcNamespace::new(fb, namespace_name, tier)?;
    Ok(Arc::new(SharedNamespaceInner::Smc(smc_ns)))
}

/// Create a shared SMC namespace from configuration.
#[cfg(fbcode_build)]
pub fn create_smc_namespace_from_config(
    fb: fbinit::FacebookInit,
    namespace_name: String,
) -> Result<SharedNamespace, NamespaceError> {
    let smc_ns = crate::v1::meta::SmcNamespace::from_config(fb, namespace_name)?;
    Ok(Arc::new(SharedNamespaceInner::Smc(smc_ns)))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn make_host_mesh_ref(name: &str) -> HostMeshRef {
        // Parse a HostMeshRef from string format: "name:host1,host2@region"
        let s = format!("{}:tcp:127.0.0.1:1234,tcp:127.0.0.1:1235@replica=2/1", name);
        HostMeshRef::from_str(&s).unwrap()
    }

    #[tokio::test]
    async fn test_register_and_get() {
        let ns = InMemoryNamespace::new("test.namespace");

        let mesh = make_host_mesh_ref("test_mesh");

        // Register
        ns.register("my_hosts", &mesh).await.unwrap();

        // Get
        let retrieved: HostMeshRef = ns.get("my_hosts").await.unwrap();
        assert_eq!(retrieved, mesh);
    }

    #[tokio::test]
    async fn test_contains() {
        let ns = InMemoryNamespace::new("test.namespace");

        let mesh = make_host_mesh_ref("workers");

        // Not registered yet
        assert!(!ns.contains::<HostMeshRef>("my_hosts").await.unwrap());

        // Register
        ns.register("my_hosts", &mesh).await.unwrap();

        // Now exists
        assert!(ns.contains::<HostMeshRef>("my_hosts").await.unwrap());

        // Different name doesn't exist
        assert!(!ns.contains::<HostMeshRef>("other").await.unwrap());
    }

    #[tokio::test]
    async fn test_unregister() {
        let ns = InMemoryNamespace::new("test.namespace");

        let mesh = make_host_mesh_ref("workers");

        ns.register("my_hosts", &mesh).await.unwrap();
        assert!(ns.contains::<HostMeshRef>("my_hosts").await.unwrap());

        // Unregister
        ns.unregister::<HostMeshRef>("my_hosts").await.unwrap();
        assert!(!ns.contains::<HostMeshRef>("my_hosts").await.unwrap());

        // Get after unregister should fail
        let result: Result<HostMeshRef, _> = ns.get("my_hosts").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let ns = InMemoryNamespace::new("test.namespace");

        let result: Result<HostMeshRef, _> = ns.get("nonexistent").await;
        assert!(matches!(result, Err(NamespaceError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_multiple_meshes() {
        let ns = InMemoryNamespace::new("test");

        let mesh1 = make_host_mesh_ref("mesh1");
        let mesh2 = make_host_mesh_ref("mesh2");

        // Register both under different names
        ns.register("hosts_a", &mesh1).await.unwrap();
        ns.register("hosts_b", &mesh2).await.unwrap();

        // Retrieve each correctly
        let retrieved1: HostMeshRef = ns.get("hosts_a").await.unwrap();
        let retrieved2: HostMeshRef = ns.get("hosts_b").await.unwrap();

        assert_eq!(retrieved1, mesh1);
        assert_eq!(retrieved2, mesh2);
    }

    #[tokio::test]
    async fn test_overwrite_registration() {
        let ns = InMemoryNamespace::new("test");

        let mesh1 = make_host_mesh_ref("mesh1");
        let mesh2 = make_host_mesh_ref("mesh2");

        // Register first mesh
        ns.register("hosts", &mesh1).await.unwrap();
        let retrieved: HostMeshRef = ns.get("hosts").await.unwrap();
        assert_eq!(retrieved, mesh1);

        // Overwrite with second mesh
        ns.register("hosts", &mesh2).await.unwrap();
        let retrieved: HostMeshRef = ns.get("hosts").await.unwrap();
        assert_eq!(retrieved, mesh2);
    }

    #[test]
    fn test_mesh_kind_as_str() {
        assert_eq!(MeshKind::Host.as_str(), "host");
        assert_eq!(MeshKind::Proc.as_str(), "proc");
        assert_eq!(MeshKind::Actor.as_str(), "actor");
    }

    #[test]
    fn test_name() {
        let ns = InMemoryNamespace::new("my.namespace");
        assert_eq!(ns.name(), "my.namespace");
    }

    #[test]
    fn test_registrable_impl_for_host_mesh_ref() {
        assert_eq!(HostMeshRef::kind(), MeshKind::Host);
    }

    #[test]
    fn test_registrable_impl_for_proc_mesh_ref() {
        assert_eq!(ProcMeshRef::kind(), MeshKind::Proc);
    }
}
