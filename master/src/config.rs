//! Runtime configuration via `cupidmq.conf` (key=value).

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_CONTROL_PORT: u16 = 9750;
const DEFAULT_METRICS_PORT: u16 = 9752;

#[derive(Debug, Clone, Default)]
pub struct FileConfig {
    pub values: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub config_path: Option<PathBuf>,
    pub host: String,
    pub control_port: u16,
    pub metrics_port: u16,
    pub control: String,
    pub metrics: String,
    pub history_interval_ms: u64,
    pub history_cap: usize,
    /// Override dashboard static files (`index.html` + assets). Empty = embedded (release builds) or API only (dev).
    pub dashboard_dir: Option<String>,
}

impl FileConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        Ok(Self {
            values: parse_conf_text(&raw),
        })
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    pub fn get_usize(&self, key: &str) -> Option<usize> {
        self.get_str(key)?.parse().ok()
    }

    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.get_str(key).and_then(parse_size)
    }

    pub fn get_u16(&self, key: &str) -> Option<u16> {
        self.get_str(key)?.parse().ok()
    }
}

pub fn parse_conf_text(raw: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for line in raw.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim().to_ascii_lowercase();
        let val = v.trim().trim_matches('"').trim_matches('\'').to_string();
        if !key.is_empty() && !val.is_empty() {
            out.insert(key, val);
        }
    }
    out
}

/// Accepts plain bytes or suffixes: kb, mb, gb (and k/m/g).
pub fn parse_size(raw: &str) -> Option<u64> {
    let s = raw.trim().to_ascii_lowercase().replace('_', "");
    if s.is_empty() {
        return None;
    }

    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    for (suffix, mul) in [
        ("gb", GB),
        ("g", GB),
        ("mb", MB),
        ("m", MB),
        ("kb", KB),
        ("k", KB),
    ] {
        if let Some(n) = s.strip_suffix(suffix) {
            let base: f64 = n.trim().parse().ok()?;
            return Some((base * mul as f64) as u64);
        }
    }
    s.parse().ok()
}

pub fn format_socket_addr(host: &str, port: u16) -> String {
    let host = host.trim();
    if host.starts_with('[') {
        format!("{host}:{port}")
    } else if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

pub fn resolve_config_path(explicit: Option<&Path>, cwd: &Path) -> PathBuf {
    if let Some(p) = explicit {
        return p.to_path_buf();
    }
    if let Ok(env) = std::env::var("CUPIDMQ_CONFIG") {
        if !env.is_empty() {
            return PathBuf::from(env);
        }
    }
    cwd.join("cupidmq.conf")
}

pub fn resolve(
    file: Option<&FileConfig>,
    config_path: Option<PathBuf>,
    cli: CliOverrides,
) -> ResolvedConfig {
    let pick_usize = |key: &str, cli: Option<usize>, default: usize| -> usize {
        cli.or_else(|| file.and_then(|f| f.get_usize(key)))
            .unwrap_or(default)
    };
    let pick_u64 = |key: &str, cli: Option<u64>, default: u64| -> u64 {
        cli.or_else(|| file.and_then(|f| f.get_u64(key)))
            .unwrap_or(default)
    };

    let dashboard_dir = cli
        .dashboard_dir
        .clone()
        .or_else(|| file.and_then(|f| f.get_str("dashboard_dir").map(str::to_string)));
    let (host, control_port, metrics_port) = resolve_bind(&cli, file);
    let control = format_socket_addr(&host, control_port);
    let metrics = format_socket_addr(&host, metrics_port);

    ResolvedConfig {
        config_path,
        host,
        control_port,
        metrics_port,
        control,
        metrics,
        history_interval_ms: pick_u64("history_interval_ms", cli.history_interval_ms, 2000),
        history_cap: pick_usize("history_cap", cli.history_cap, 1800),
        dashboard_dir,
    }
}

fn resolve_bind(cli: &CliOverrides, file: Option<&FileConfig>) -> (String, u16, u16) {
    let host = cli
        .host
        .as_deref()
        .map(str::to_string)
        .or_else(|| file.and_then(|f| f.get_str("host").map(str::to_string)))
        .unwrap_or_else(|| DEFAULT_HOST.to_string());

    let control_port = cli
        .control_port
        .or_else(|| file.and_then(|f| f.get_u16("control_port")))
        .unwrap_or(DEFAULT_CONTROL_PORT);

    let metrics_port = cli
        .metrics_port
        .or_else(|| file.and_then(|f| f.get_u16("metrics_port")))
        .unwrap_or(DEFAULT_METRICS_PORT);

    (host, control_port, metrics_port)
}

#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    pub host: Option<String>,
    pub control_port: Option<u16>,
    pub metrics_port: Option<u16>,
    pub history_interval_ms: Option<u64>,
    pub history_cap: Option<usize>,
    pub dashboard_dir: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_conf_ignores_comments() {
        let m = parse_conf_text(
            "# comment\nhost=0.0.0.0\ncontrol_port=9750\n\nhistory_cap=1800",
        );
        assert_eq!(m.get("host").map(String::as_str), Some("0.0.0.0"));
        assert_eq!(m.get("control_port").map(String::as_str), Some("9750"));
        assert_eq!(m.get("history_cap").map(String::as_str), Some("1800"));
    }

    #[test]
    fn resolve_host_and_ports() {
        let file = FileConfig {
            values: parse_conf_text(
                "host=0.0.0.0\ncontrol_port=9750\nmetrics_port=9752",
            ),
        };
        let resolved = resolve(Some(&file), None, CliOverrides::default());
        assert_eq!(resolved.host, "0.0.0.0");
        assert_eq!(resolved.control_port, 9750);
        assert_eq!(resolved.metrics_port, 9752);
        assert_eq!(resolved.control, "0.0.0.0:9750");
        assert_eq!(resolved.metrics, "0.0.0.0:9752");
    }

    #[test]
    fn resolve_cli_overrides_file() {
        let file = FileConfig {
            values: parse_conf_text("host=0.0.0.0\ncontrol_port=9750\nmetrics_port=9752"),
        };
        let resolved = resolve(
            Some(&file),
            None,
            CliOverrides {
                host: Some("10.0.0.5".into()),
                control_port: Some(19750),
                ..Default::default()
            },
        );
        assert_eq!(resolved.host, "10.0.0.5");
        assert_eq!(resolved.control_port, 19750);
        assert_eq!(resolved.metrics_port, 9752);
    }

    #[test]
    fn format_ipv6_socket_addr() {
        assert_eq!(format_socket_addr("::1", 9750), "[::1]:9750");
        assert_eq!(format_socket_addr("[::1]", 9750), "[::1]:9750");
    }

    #[test]
    fn parse_size_units() {
        assert_eq!(parse_size("1024mb"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_size("512m"), Some(512 * 1024 * 1024));
        assert_eq!(parse_size("1073741824"), Some(1073741824));
    }
}
