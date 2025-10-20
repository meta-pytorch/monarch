/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::str::FromStr;

use anyhow;
use hyperactor::channel::ChannelAddr;
use hyperactor::channel::MetaTlsAddr;

/// Extended type to represent a system address which can be a ChannelAdd or a MAST job name.
#[derive(Clone, Debug)]
pub struct SystemAddr(ChannelAddr);

impl From<SystemAddr> for ChannelAddr {
    fn from(system_addr: SystemAddr) -> ChannelAddr {
        system_addr.0
    }
}

impl FromStr for SystemAddr {
    type Err = anyhow::Error;

    #[cfg(fbcode_build)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let handle = tokio::runtime::Handle::try_current()?;
        tokio::task::block_in_place(|| handle.block_on(parse_system_address_or_mast_job(s)))
            .map(Self)
    }
    #[cfg(not(fbcode_build))]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ChannelAddr::from_str(s).map(SystemAddr)
    }
}

/// Parse a system address or MAST job name into a ChannelAddr. If the address is a MAST job name,
/// job definition will be fetched to extract the SMC tier, then SMC is queried to get the system
/// address.
#[cfg(fbcode_build)]
async fn parse_system_address_or_mast_job(address: &str) -> Result<ChannelAddr, anyhow::Error> {
    use hyperactor_meta_lib::system_resolution::SMCClient;
    use hyperactor_meta_lib::system_resolution::canonicalize_hostname;

    match ChannelAddr::from_str(address) {
        Ok(addr) => Ok(addr),
        Err(channel_err) => {
            let smc_tier = match get_smc_tier(address).await {
                Ok(Some(smc_tier)) => smc_tier,
                // job is not found, return channel parse error.
                Ok(None) => anyhow::bail!(
                    "could not resolve system address from {}: {}",
                    address,
                    channel_err
                ),
                Err(e) => {
                    anyhow::bail!(e);
                }
            };
            let (host, port) = SMCClient::new(fbinit::expect_init(), smc_tier)?
                .get_system_address()
                .await?;
            let channel_address = ChannelAddr::MetaTls {
                addr: MetaTlsAddr::Host {
                    hostname: canonicalize_hostname(&host),
                    port,
                },
                label: Some("system".to_string()),
            };
            Ok(channel_address)
        }
    }
}

/// Get the SMC tier for a given MAST job name. Returns None if the job is not found.
#[cfg(fbcode_build)]
async fn get_smc_tier(job_name: &str) -> Result<Option<String>, anyhow::Error> {
    use hpcscheduler;
    use hpcscheduler_srclients;
    use hpcscheduler_srclients::thrift;

    /// This should match the key used in the MAST job definition when job was created.
    /// For example: https://github.com/fairinternal/xlformers/blob/5db99239e7fa2cc08ca16232edc670b13003e172/core/monarch/mast.py#L446
    static SMC_TIER_APPLICATION_METADATA_KEY: &str = "monarch_system_smc_tier";

    let client = hpcscheduler_srclients::make_HpcSchedulerService_srclient!(
        fbinit::expect_init(),
        tiername = "mast.api.read"
    )?;
    let request = hpcscheduler::GetHpcJobStatusRequest {
        hpcJobName: job_name.to_string(),
        ..Default::default()
    };
    let response = match client.getHpcJobStatus(&request).await {
        Ok(response) => response,
        Err(thrift::errors::GetHpcJobStatusError::e(e)) => {
            if e.errorCode == hpcscheduler::HpcSchedulerErrorCode::JOB_NOT_FOUND {
                return Ok(None);
            } else {
                anyhow::bail!(e);
            }
        }
        Err(e) => anyhow::bail!(e),
    };
    if response.state != hpcscheduler::HpcJobState::RUNNING {
        anyhow::bail!("job {} is not running", job_name);
    }
    let request = hpcscheduler::GetHpcJobDefinitionRequest {
        hpcJobName: job_name.to_string(),
        ..Default::default()
    };
    let response = client.getHpcJobDefinition(&request).await?;
    let metadata = match response.jobDefinition.applicationMetadata {
        Some(metadata) => metadata,
        None => anyhow::bail!("no application metadata found in job definition"),
    };
    match metadata.get(SMC_TIER_APPLICATION_METADATA_KEY) {
        Some(smc_tier) => Ok(Some(smc_tier.to_string())),
        None => anyhow::bail!("did not find smc tier in application metadata"),
    }
}
