use std::{
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, bail, ensure};

use crate::{
    config::{
        types::RequestedTradingInstrument, validation::validate_requested_trading_instrument,
    },
    okx::{
        trading_instrument::ValidatedTradingInstrument,
        types::{OkxAccountConfig, OkxTradeFeeRate},
    },
};

/// Strict operator intent used as the identity of one capability request.
///
/// Account diagnostics are deliberately absent: product identity is only the
/// configured instrument, instrument type, and trade mode.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestedCapability {
    trading_instrument: RequestedTradingInstrument,
}

impl RequestedCapability {
    pub(crate) fn from_trading_instrument(requested: &RequestedTradingInstrument) -> Result<Self> {
        validate_requested_trading_instrument(requested)?;
        Ok(Self {
            trading_instrument: requested.clone(),
        })
    }

    pub(crate) fn trading_instrument(&self) -> &RequestedTradingInstrument {
        &self.trading_instrument
    }
}

/// Parsed OKX account-level metadata retained only for sanitized diagnostics
/// and evidence-generation change detection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AccountLevelDiagnostic {
    One,
    Two,
    Three,
    Four,
}

impl AccountLevelDiagnostic {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "1" => Ok(Self::One),
            "2" => Ok(Self::Two),
            "3" => Ok(Self::Three),
            "4" => Ok(Self::Four),
            _ => bail!("OKX account configuration acctLv is missing, malformed, or undocumented"),
        }
    }

    pub(crate) const fn as_okx(self) -> &'static str {
        match self {
            Self::One => "1",
            Self::Two => "2",
            Self::Three => "3",
            Self::Four => "4",
        }
    }
}

/// One timestamped observation of diagnostic account metadata.
#[derive(Clone, Debug)]
pub(crate) struct AccountLevelDiagnosticSnapshot {
    value: AccountLevelDiagnostic,
    observed_at: Instant,
}

impl AccountLevelDiagnosticSnapshot {
    pub(crate) fn observe(account: &OkxAccountConfig) -> Result<Self> {
        Self::observed_at(account, Instant::now())
    }

    fn observed_at(account: &OkxAccountConfig, observed_at: Instant) -> Result<Self> {
        Ok(Self {
            value: AccountLevelDiagnostic::parse(&account.account_level)?,
            observed_at,
        })
    }

    pub(crate) const fn value(&self) -> AccountLevelDiagnostic {
        self.value
    }

    pub(crate) fn ensure_fresh(&self, maximum_age: Duration) -> Result<()> {
        ensure!(
            self.observed_at.elapsed() <= maximum_age,
            "OKX account-level diagnostic snapshot became stale before capability validation completed"
        );
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn stale_for_test(account: &OkxAccountConfig, age: Duration) -> Result<Self> {
        let observed_at = Instant::now()
            .checked_sub(age)
            .ok_or_else(|| anyhow::anyhow!("test account diagnostic age is out of range"))?;
        Self::observed_at(account, observed_at)
    }
}

/// The implemented product contexts are closed and non-convertible. Phase 1
/// intentionally contains only the existing cash-SPOT context.
#[derive(Clone, Debug)]
enum ValidatedProductContext {
    CashSpot(Arc<ValidatedTradingInstrument>),
}

/// One immutable, bounded generation of agreeing capability evidence.
///
/// The diagnostic account level is retained beside, never inside, requested
/// or validated product identity.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedCapabilityGeneration {
    requested: RequestedCapability,
    product: ValidatedProductContext,
    account_level: AccountLevelDiagnosticSnapshot,
    fee: OkxTradeFeeRate,
}

impl ValidatedCapabilityGeneration {
    pub(crate) fn cash_spot(
        requested: RequestedCapability,
        instrument: Arc<ValidatedTradingInstrument>,
        account_level: AccountLevelDiagnosticSnapshot,
        fee: OkxTradeFeeRate,
        maximum_age: Duration,
    ) -> Result<Self> {
        account_level.ensure_fresh(maximum_age)?;
        let requested_instrument = requested.trading_instrument();
        ensure!(
            requested_instrument.instrument.as_str() == instrument.inst_id()
                && requested_instrument.inst_type.as_okx() == instrument.inst_type().as_okx()
                && requested_instrument.td_mode.as_okx() == instrument.td_mode().as_okx(),
            "validated cash-SPOT context contradicts the requested capability identity"
        );
        fee.ensure_spot(instrument.inst_id())?;
        ensure!(
            fee.group_id == instrument.fee_group_id()?,
            "OKX fee group {} contradicts validated instrument groupId {} for {}",
            fee.group_id,
            instrument.fee_group_id()?,
            instrument.inst_id()
        );
        fee.normalized_maker_cost_rate()?;
        fee.normalized_taker_cost_rate()?;
        Ok(Self {
            requested,
            product: ValidatedProductContext::CashSpot(instrument),
            account_level,
            fee,
        })
    }

    pub(crate) fn requested(&self) -> &RequestedCapability {
        &self.requested
    }

    pub(crate) fn cash_spot_context(&self) -> Arc<ValidatedTradingInstrument> {
        match &self.product {
            ValidatedProductContext::CashSpot(instrument) => Arc::clone(instrument),
        }
    }

    pub(crate) fn account_level_diagnostic(&self) -> &AccountLevelDiagnosticSnapshot {
        &self.account_level
    }

    pub(crate) fn fee(&self) -> &OkxTradeFeeRate {
        &self.fee
    }
}

impl Deref for ValidatedCapabilityGeneration {
    type Target = ValidatedTradingInstrument;

    fn deref(&self) -> &Self::Target {
        match &self.product {
            ValidatedProductContext::CashSpot(instrument) => instrument,
        }
    }
}

#[cfg(test)]
#[path = "capability_tests.rs"]
mod tests;
