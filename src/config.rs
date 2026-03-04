use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub channels: Vec<String>,
    pub max_age_days: u32,
    pub batch_limit: u32,
    pub embed_budget: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            channels: Vec::new(),
            max_age_days: 90,
            batch_limit: 5000,
            embed_budget: 50,
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        let path = config_dir()?.join("config.json");
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(&path)
            .map_err(|e| ConfigError::ReadFailed(path.clone(), e))?;
        let config: Self = serde_json::from_str(&content)
            .map_err(|e| ConfigError::ParseFailed(path, e))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.max_age_days == 0 {
            return Err(ConfigError::InvalidValue("max_age_days must be > 0".into()));
        }
        if self.batch_limit == 0 {
            return Err(ConfigError::InvalidValue("batch_limit must be > 0".into()));
        }
        for ch in &self.channels {
            Self::validate_channel_id(ch)?;
        }
        Ok(())
    }

    pub fn is_channel_allowed(&self, channel_id: &str) -> bool {
        self.channels.is_empty() || self.channels.iter().any(|c| c == channel_id)
    }

    pub fn validate_channel_id(channel_id: &str) -> Result<(), ConfigError> {
        if channel_id.starts_with('C') && channel_id.len() > 1 && channel_id[1..].chars().all(|c| c.is_ascii_alphanumeric()) {
            Ok(())
        } else {
            Err(ConfigError::InvalidChannelId(channel_id.to_string()))
        }
    }
}

pub fn data_dir() -> Result<PathBuf, ConfigError> {
    data_dir_with(|k| std::env::var(k))
}

fn data_dir_with(get_var: impl Fn(&str) -> Result<String, std::env::VarError>) -> Result<PathBuf, ConfigError> {
    let base = get_var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|_| {
            get_var("HOME")
                .map(|h| PathBuf::from(h).join(".local").join("share"))
                .map_err(|_| ConfigError::HomeDirNotFound)
        })?;
    Ok(base.join("kiku"))
}

fn config_dir() -> Result<PathBuf, ConfigError> {
    let base = std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|_| dirs_fallback_config_home())?;
    Ok(base.join("kiku"))
}

fn dirs_fallback_config_home() -> Result<PathBuf, ConfigError> {
    Ok(home_dir()?.join(".config"))
}

fn home_dir() -> Result<PathBuf, ConfigError> {
    std::env::var("HOME")
        .map(PathBuf::from)
        .map_err(|_| ConfigError::HomeDirNotFound)
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Invalid channel ID: {0}")]
    InvalidChannelId(String),

    #[error("Failed to read config at {0}: {1}")]
    ReadFailed(PathBuf, std::io::Error),

    #[error("Failed to parse config at {0}: {1}")]
    ParseFailed(PathBuf, serde_json::Error),

    #[error("HOME environment variable not set")]
    HomeDirNotFound,

    #[error("Invalid config: {0}")]
    InvalidValue(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_allows_all_channels() {
        let config = Config::default();
        assert!(config.is_channel_allowed("C1234567890"));
    }

    #[test]
    fn allowlist_rejects_unlisted_channel() {
        let config = Config {
            channels: vec!["C111".to_string(), "C222".to_string()],
            ..Default::default()
        };
        assert!(config.is_channel_allowed("C111"));
        assert!(!config.is_channel_allowed("C999"));
    }

    #[test]
    fn validate_channel_id_accepts_valid() {
        assert!(Config::validate_channel_id("C1234567890").is_ok());
        assert!(Config::validate_channel_id("CABC123").is_ok());
    }

    #[test]
    fn validate_channel_id_rejects_invalid() {
        assert!(Config::validate_channel_id("D1234").is_err());
        assert!(Config::validate_channel_id("C").is_err());
        assert!(Config::validate_channel_id("").is_err());
        assert!(Config::validate_channel_id("C123-456").is_err());
    }

    #[test]
    fn data_dir_respects_xdg() {
        let dir = data_dir_with(|key| match key {
            "XDG_DATA_HOME" => Ok("/tmp/test-xdg".into()),
            _ => Err(std::env::VarError::NotPresent),
        }).unwrap();
        assert_eq!(dir, PathBuf::from("/tmp/test-xdg/kiku"));
    }

    #[test]
    fn data_dir_falls_back_to_home() {
        let dir = data_dir_with(|key| match key {
            "HOME" => Ok("/home/testuser".into()),
            _ => Err(std::env::VarError::NotPresent),
        }).unwrap();
        assert_eq!(dir, PathBuf::from("/home/testuser/.local/share/kiku"));
    }
}
