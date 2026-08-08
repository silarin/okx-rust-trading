use super::types::BotConfig;

const MILLIS_PER_SECOND: u64 = 1_000;

pub fn timeout_ms_to_secs(timeout_ms: u64) -> u64 {
    timeout_ms.div_ceil(MILLIS_PER_SECOND)
}

pub fn masked_okx_api_key(config: &BotConfig) -> String {
    config
        .okx
        .as_ref()
        .map(|okx| okx.api_key.as_str())
        .map(str::trim)
        .filter(|api_key| !api_key.is_empty())
        .map(mask)
        .unwrap_or_else(|| "unconfigured".to_owned())
}

pub fn masked_okx_account_id(config: &BotConfig) -> String {
    config
        .okx
        .as_ref()
        .map(|okx| okx.account_id.as_str())
        .map(str::trim)
        .filter(|account_id| !account_id.is_empty())
        .map(mask)
        .unwrap_or_else(|| "unconfigured".to_owned())
}

fn mask(value: &str) -> String {
    let char_count = value.chars().count();
    if char_count <= 4 {
        return "*".repeat(char_count);
    }
    let prefix: String = value.chars().take(2).collect();
    let suffix: String = value.chars().skip(char_count.saturating_sub(2)).collect();
    format!("{prefix}{}{suffix}", "*".repeat(char_count - 4))
}
