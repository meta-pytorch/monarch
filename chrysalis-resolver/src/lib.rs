/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Deployment resolvers for Chrysalis command-line tools.

use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::process::ExitStatus;
use std::str::FromStr;

use serde_json::Value;
use serde_json::json;
use thiserror::Error;
use tokio::process::Command;

const MAST_READ_TIER: &str = "mast.api.read";
const DEFAULT_MAST_PORT: u16 = 26600;

/// A deployment reference that can produce a complete CLI connection policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolverSpec {
    /// A MAST job whose first placed task hosts the Chrysalis root.
    Mast { job: String },
}

/// An identity provider selected by a deployment resolver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityProvider {
    /// Meta's mTLS identity provider.
    Meta,
}

/// CLI connection values returned by a deployment resolver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Resolution {
    join: String,
    carrier: String,
    identity: IdentityProvider,
}

impl Resolution {
    /// Returns the address or pinned token used to join the deployment.
    pub fn join(&self) -> &str {
        &self.join
    }

    /// Returns the local carrier specification suitable for the deployment.
    pub fn carrier(&self) -> &str {
        &self.carrier
    }

    /// Returns the identity provider required by the deployment.
    pub const fn identity(&self) -> IdentityProvider {
        self.identity
    }
}

impl ResolverSpec {
    /// Returns whether `value` names a resolver supported by this build.
    pub fn recognizes(value: &str) -> bool {
        matches!(value.split_once("://"), Some(("mast", _)))
    }

    /// Resolves this deployment into CLI connection values.
    pub async fn resolve(&self) -> Result<Resolution, ResolveError> {
        match self {
            Self::Mast { job } => resolve_mast(job).await,
        }
    }
}

impl FromStr for ResolverSpec {
    type Err = ParseResolverError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (scheme, target) = value
            .split_once("://")
            .ok_or(ParseResolverError::MissingScheme)?;
        match scheme {
            "mast" if target.is_empty() => Err(ParseResolverError::EmptyMastJob),
            "mast" if target.contains(['/', '?', '#']) => Err(ParseResolverError::InvalidMastJob),
            "mast" => Ok(Self::Mast {
                job: target.to_owned(),
            }),
            _ => Err(ParseResolverError::UnsupportedScheme {
                scheme: scheme.to_owned(),
            }),
        }
    }
}

/// A malformed or unsupported resolver URL.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ParseResolverError {
    /// The value does not contain a resolver scheme.
    #[error("resolver URL requires a scheme")]
    MissingScheme,
    /// The resolver scheme is not registered.
    #[error("unsupported resolver scheme: {scheme}")]
    UnsupportedScheme { scheme: String },
    /// A MAST resolver omitted its job name.
    #[error("MAST resolver requires a job name")]
    EmptyMastJob,
    /// A MAST job name contains URL path or query syntax.
    #[error("MAST resolver accepts a job name, not a path or query")]
    InvalidMastJob,
}

/// A deployment could not be resolved.
#[derive(Debug, Error)]
pub enum ResolveError {
    /// The MAST status command could not be started.
    #[error("query MAST job {job}")]
    Query {
        job: String,
        #[source]
        source: std::io::Error,
    },
    /// The MAST status command failed.
    #[error("query MAST job {job} failed with {status}: {stderr}")]
    QueryFailed {
        job: String,
        status: ExitStatus,
        stderr: String,
    },
    /// The MAST status command returned no JSON object.
    #[error("MAST status for {job} contained no JSON response")]
    MissingResponse { job: String },
    /// The MAST job is no longer connectable.
    #[error("MAST job {job} is {state}")]
    Terminal { job: String, state: String },
    /// The MAST job has no task group named `nodes`.
    #[error("MAST job {job} has no nodes task group")]
    MissingTaskGroup { job: String },
    /// No root task has been placed yet.
    #[error("MAST job {job} has no placed root task")]
    MissingPlacement { job: String },
    /// MAST returned an invalid task IP.
    #[error("MAST job {job} returned invalid task IP {address}")]
    InvalidAddress { job: String, address: String },
}

async fn resolve_mast(job: &str) -> Result<Resolution, ResolveError> {
    let status = query_mast_status(job).await?;
    resolve_mast_status(job, &status, DEFAULT_MAST_PORT)
}

async fn query_mast_status(job: &str) -> Result<Value, ResolveError> {
    let request = json!({"request": {"hpcJobName": job}}).to_string();
    let output = Command::new("thriftdbg")
        .args([
            "sendRequest",
            "getHpcJobStatus",
            &request,
            "--tier",
            MAST_READ_TIER,
        ])
        .output()
        .await
        .map_err(|source| ResolveError::Query {
            job: job.to_owned(),
            source,
        })?;
    if !output.status.success() {
        return Err(ResolveError::QueryFailed {
            job: job.to_owned(),
            status: output.status,
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str(line).ok())
        .ok_or_else(|| ResolveError::MissingResponse {
            job: job.to_owned(),
        })
}

fn resolve_mast_status(job: &str, status: &Value, port: u16) -> Result<Resolution, ResolveError> {
    if let Some(state @ ("DEAD" | "FAILED" | "COMPLETE")) = status["state"].as_str() {
        return Err(ResolveError::Terminal {
            job: job.to_owned(),
            state: state.to_owned(),
        });
    }
    let tasks = status["taskGroups"]
        .as_array()
        .and_then(|groups| groups.iter().find(|group| group["name"] == "nodes"))
        .and_then(|group| group["tasks"].as_array())
        .ok_or_else(|| ResolveError::MissingTaskGroup {
            job: job.to_owned(),
        })?;
    let mut placements = tasks
        .iter()
        .filter_map(|task| Some((task["hostname"].as_str()?, task["taskIp"].as_str()?)))
        .collect::<Vec<_>>();
    placements.sort_unstable_by_key(|(hostname, _)| *hostname);
    let (_, root_ip) = placements
        .first()
        .ok_or_else(|| ResolveError::MissingPlacement {
            job: job.to_owned(),
        })?;
    let ip: IpAddr = root_ip.parse().map_err(|_| ResolveError::InvalidAddress {
        job: job.to_owned(),
        address: (*root_ip).to_owned(),
    })?;
    let join = SocketAddr::new(ip, port);
    let carrier = SocketAddr::new(
        match ip {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        },
        0,
    );
    Ok(Resolution {
        join: format!("udp://{join}"),
        carrier: format!("udp://{carrier}"),
        identity: IdentityProvider::Meta,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_resolver_urls() {
        assert_eq!(
            "mast://scale_job".parse(),
            Ok(ResolverSpec::Mast {
                job: "scale_job".into()
            })
        );
        assert!(ResolverSpec::recognizes("mast://scale_job"));
        assert!(!ResolverSpec::recognizes("udp://[::1]:26600"));
    }

    #[test]
    fn rejects_malformed_resolver_urls() {
        assert_eq!(
            "mast://".parse::<ResolverSpec>(),
            Err(ParseResolverError::EmptyMastJob)
        );
        assert_eq!(
            "mast://job/path".parse::<ResolverSpec>(),
            Err(ParseResolverError::InvalidMastJob)
        );
        assert_eq!(
            "other://job".parse::<ResolverSpec>(),
            Err(ParseResolverError::UnsupportedScheme {
                scheme: "other".into()
            })
        );
    }

    #[test]
    fn resolves_first_hostname_to_ipv6_connection_policy() {
        let status = json!({
            "state": "RUNNING",
            "taskGroups": [{
                "name": "nodes",
                "tasks": [
                    {"hostname": "host-z", "taskIp": "2401:db00::2"},
                    {"hostname": "host-a", "taskIp": "2401:db00::1"},
                ],
            }],
        });

        assert_eq!(
            resolve_mast_status("job", &status, 26600).expect("resolve MAST status"),
            Resolution {
                join: "udp://[2401:db00::1]:26600".into(),
                carrier: "udp://[::]:0".into(),
                identity: IdentityProvider::Meta,
            }
        );
    }

    #[test]
    fn resolves_ipv4_connection_policy() {
        let status = json!({
            "state": "RUNNING",
            "taskGroups": [{
                "name": "nodes",
                "tasks": [{"hostname": "host-a", "taskIp": "10.0.0.1"}],
            }],
        });

        assert_eq!(
            resolve_mast_status("job", &status, 1234).expect("resolve MAST status"),
            Resolution {
                join: "udp://10.0.0.1:1234".into(),
                carrier: "udp://0.0.0.0:0".into(),
                identity: IdentityProvider::Meta,
            }
        );
    }

    #[test]
    fn rejects_terminal_or_unplaced_jobs() {
        let terminal = json!({"state": "DEAD"});
        assert!(matches!(
            resolve_mast_status("job", &terminal, 26600),
            Err(ResolveError::Terminal { .. })
        ));

        let unplaced = json!({
            "state": "PENDING",
            "taskGroups": [{"name": "nodes", "tasks": []}],
        });
        assert!(matches!(
            resolve_mast_status("job", &unplaced, 26600),
            Err(ResolveError::MissingPlacement { .. })
        ));
    }
}
