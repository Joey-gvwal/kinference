//! The one door to the API server. `KubeApi` is the live implementation;
//! `Recorded`, in tests, answers from objects the test supplies.

use std::future::Future;

use futures::StreamExt as _;
use futures::stream::BoxStream;
use kube::Api;
use kube::api::{DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::core::NamespaceResourceScope;
use kube::runtime::watcher;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::controller::Error;

/// A namespaced object the control plane reads or writes.
pub trait Object:
    kube::Resource<Scope = NamespaceResourceScope, DynamicType = ()>
    + Clone
    + std::fmt::Debug
    + Serialize
    + DeserializeOwned
    + Send
    + Sync
    + 'static
{
}

impl<K> Object for K where
    K: kube::Resource<Scope = NamespaceResourceScope, DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static
{
}

/// What the control plane asks of the API server. `namespace: None` is
/// every namespace. Errors are `kube::Error`: the loops read a 404 as
/// "absent" and a 409 as "lost the race".
pub trait ApiServer: Clone + Send + Sync + 'static {
    /// `kube::runtime::watcher` over `K`, with `cfg`'s selectors.
    fn watch<K: Object>(
        &self,
        namespace: Option<&str>,
        cfg: watcher::Config,
    ) -> BoxStream<'static, Result<watcher::Event<K>, watcher::Error>>;

    fn list<K: Object>(
        &self,
        namespace: Option<&str>,
    ) -> impl Future<Output = Result<Vec<K>, kube::Error>> + Send;

    fn get<K: Object>(
        &self,
        namespace: &str,
        name: &str,
    ) -> impl Future<Output = Result<K, kube::Error>> + Send;

    fn create<K: Object>(
        &self,
        namespace: &str,
        object: &K,
    ) -> impl Future<Output = Result<K, kube::Error>> + Send;

    /// Optimistic: `object`'s `resourceVersion` must be current.
    fn replace<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        object: &K,
    ) -> impl Future<Output = Result<K, kube::Error>> + Send;

    /// Server-side apply as `manager`, taking back conflicting fields.
    fn apply<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        manager: &str,
        object: &K,
    ) -> impl Future<Output = Result<(), kube::Error>> + Send;

    fn delete<K: Object>(
        &self,
        namespace: &str,
        name: &str,
    ) -> impl Future<Output = Result<(), kube::Error>> + Send;

    /// Merge-patch `{"status": status}` on the status subresource.
    fn patch_status<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        status: &serde_json::Value,
    ) -> impl Future<Output = Result<(), kube::Error>> + Send;
}

/// The API server over a `kube::Client`.
#[derive(Clone)]
pub struct KubeApi {
    client: kube::Client,
}

impl KubeApi {
    /// In-cluster credentials only; a kubeconfig is refused. Discovery's
    /// client: the pod IPs it finds are unreachable from outside the cluster.
    pub fn in_cluster() -> Result<Self, Error> {
        let cfg = kube::Config::incluster_env().map_err(Error::InCluster)?;
        let client = kube::Client::try_from(cfg).map_err(|source| Error::Client {
            what: "the in-cluster config",
            source,
        })?;
        Ok(Self { client })
    }

    /// In-cluster credentials, else `~/.kube/config`, so the CRD side can
    /// run against a local cluster.
    pub async fn infer() -> Result<Self, Error> {
        let cfg = kube::Config::infer().await.map_err(Error::Infer)?;
        let client = kube::Client::try_from(cfg).map_err(|source| Error::Client {
            what: "the inferred config",
            source,
        })?;
        Ok(Self { client })
    }

    fn namespaced<K: Object>(&self, namespace: &str) -> Api<K> {
        Api::namespaced(self.client.clone(), namespace)
    }

    fn scoped<K: Object>(&self, namespace: Option<&str>) -> Api<K> {
        match namespace {
            Some(ns) => Api::namespaced(self.client.clone(), ns),
            None => Api::all(self.client.clone()),
        }
    }
}

impl ApiServer for KubeApi {
    fn watch<K: Object>(
        &self,
        namespace: Option<&str>,
        cfg: watcher::Config,
    ) -> BoxStream<'static, Result<watcher::Event<K>, watcher::Error>> {
        watcher::watcher(self.scoped::<K>(namespace), cfg).boxed()
    }

    async fn list<K: Object>(&self, namespace: Option<&str>) -> Result<Vec<K>, kube::Error> {
        let list = self
            .scoped::<K>(namespace)
            .list(&ListParams::default())
            .await?;
        Ok(list.items)
    }

    async fn get<K: Object>(&self, namespace: &str, name: &str) -> Result<K, kube::Error> {
        self.namespaced::<K>(namespace).get(name).await
    }

    async fn create<K: Object>(&self, namespace: &str, object: &K) -> Result<K, kube::Error> {
        self.namespaced::<K>(namespace)
            .create(&PostParams::default(), object)
            .await
    }

    async fn replace<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        object: &K,
    ) -> Result<K, kube::Error> {
        self.namespaced::<K>(namespace)
            .replace(name, &PostParams::default(), object)
            .await
    }

    async fn apply<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        manager: &str,
        object: &K,
    ) -> Result<(), kube::Error> {
        self.namespaced::<K>(namespace)
            .patch(
                name,
                &PatchParams::apply(manager).force(),
                &Patch::Apply(object),
            )
            .await?;
        Ok(())
    }

    async fn delete<K: Object>(&self, namespace: &str, name: &str) -> Result<(), kube::Error> {
        self.namespaced::<K>(namespace)
            .delete(name, &DeleteParams::default())
            .await?;
        Ok(())
    }

    async fn patch_status<K: Object>(
        &self,
        namespace: &str,
        name: &str,
        status: &serde_json::Value,
    ) -> Result<(), kube::Error> {
        let patch = serde_json::json!({ "status": status });
        self.namespaced::<K>(namespace)
            .patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
pub mod recorded;

#[cfg(test)]
mod tests;
