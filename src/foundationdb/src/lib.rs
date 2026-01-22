// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Common FoundationDB utilities for Materialize.
//!
//! This crate provides shared functionality for FoundationDB-backed
//! implementations across Materialize, including network initialization
//! and URL parsing utilities.

use std::sync::OnceLock;

use foundationdb::api::NetworkAutoStop;
use mz_ore::url::SensitiveUrl;

/// FoundationDB network singleton.
///
/// The FoundationDB client requires exactly one network initialization per process.
/// This singleton ensures that the network is initialized exactly once and shared
/// across all FDB users in the process.
///
/// Normally, we'd need to drop this to clean up the network, but since we
/// never expect to exit normally, it's fine to leak it.
static FDB_NETWORK: OnceLock<NetworkAutoStop> = OnceLock::new();

/// Initialize the FoundationDB network.
///
/// This function is safe to call multiple times - only the first call will
/// actually initialize the network, subsequent calls return the existing
/// network handle.
///
/// # Safety
///
/// The underlying `foundationdb::boot()` call is unsafe because it must only
/// be called once per process. This function uses a `OnceLock` to ensure
/// that guarantee is upheld.
pub fn init_network() -> &'static NetworkAutoStop {
    FDB_NETWORK.get_or_init(|| unsafe { foundationdb::boot() })
}

/// Configuration parsed from a FoundationDB URL.
///
/// FoundationDB URLs have the format:
/// `foundationdb:[/path/to/cluster/file]?options=--search_path=<prefix>`
///
/// The cluster file path is optional - if not provided, the default
/// `/etc/foundationdb/fdb.cluster` is used.
#[derive(Clone, Debug)]
pub struct FdbConfig {
    /// The path to the FDB cluster file, or None to use the default.
    pub cluster_file_path: Option<String>,
    /// The prefix path components for the directory layer.
    pub prefix: Vec<String>,
}

impl FdbConfig {
    /// Parse a FoundationDB URL into configuration.
    ///
    /// # URL Format
    ///
    /// The URL format is: `foundationdb:[/path/to/cluster/file]?options=--search_path=<prefix>`
    ///
    /// - The scheme must be `foundationdb`
    /// - The path (optional) specifies the cluster file location
    /// - The `options` query parameter with `--search_path=<prefix>` specifies
    ///   the directory prefix to use (similar to PostgreSQL's search_path)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Use default cluster file with a prefix
    /// let url = "foundationdb:?options=--search_path=my_app/consensus";
    ///
    /// // Specify cluster file and prefix
    /// let url = "foundationdb:/etc/foundationdb/fdb.cluster?options=--search_path=my_app";
    /// ```
    pub fn parse(url: &SensitiveUrl) -> Result<Self, anyhow::Error> {
        let mut prefix = Vec::new();

        for (key, value) in url.query_pairs() {
            match &*key {
                "options" => {
                    if let Some(path) = value.strip_prefix("--search_path=") {
                        prefix = path.split('/').map(|s| s.to_owned()).collect();
                    } else {
                        anyhow::bail!("unrecognized FoundationDB URL options parameter: {value}");
                    }
                }
                key => {
                    anyhow::bail!("unrecognized FoundationDB URL query parameter: {key}: {value}");
                }
            }
        }

        let cluster_file_path = if url.0.cannot_be_a_base() {
            None
        } else {
            let path = url.0.path();
            if path.is_empty() || path == "/" {
                None
            } else {
                Some(path.to_owned())
            }
        };

        Ok(FdbConfig {
            cluster_file_path,
            prefix,
        })
    }

    /// Returns the cluster file path as an Option<&str> suitable for
    /// passing to `Database::new()`.
    pub fn cluster_file(&self) -> Option<&str> {
        self.cluster_file_path.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    #[test]
    fn test_parse_url_with_prefix() {
        let url =
            SensitiveUrl::from_str("foundationdb:?options=--search_path=my_app/consensus").unwrap();
        let config = FdbConfig::parse(&url).unwrap();
        assert_eq!(config.cluster_file_path, None);
        assert_eq!(config.prefix, vec!["my_app", "consensus"]);
    }

    #[test]
    fn test_parse_url_with_path_and_prefix() {
        let url = SensitiveUrl::from_str(
            "foundationdb:/etc/foundationdb/fdb.cluster?options=--search_path=test",
        )
        .unwrap();
        let config = FdbConfig::parse(&url).unwrap();
        assert_eq!(
            config.cluster_file_path,
            Some("/etc/foundationdb/fdb.cluster".to_owned())
        );
        assert_eq!(config.prefix, vec!["test"]);
    }

    #[test]
    fn test_parse_url_no_options() {
        let url = SensitiveUrl::from_str("foundationdb:").unwrap();
        let config = FdbConfig::parse(&url).unwrap();
        assert_eq!(config.cluster_file_path, None);
        assert!(config.prefix.is_empty());
    }

    #[test]
    fn test_parse_url_invalid_option() {
        let url = SensitiveUrl::from_str("foundationdb:?options=--invalid=value").unwrap();
        assert!(FdbConfig::parse(&url).is_err());
    }

    #[test]
    fn test_parse_url_invalid_query_param() {
        let url = SensitiveUrl::from_str("foundationdb:?unknown=value").unwrap();
        assert!(FdbConfig::parse(&url).is_err());
    }
}
