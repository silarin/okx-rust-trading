//! Fail-closed shutdown reconciliation for `OkxEmaAtrMakerTrend`.

use anyhow::{Result, bail, ensure};
use tracing::info;

use super::OkxEmaAtrMakerTrendRunner;
use crate::okx::client::OkxClient;

impl OkxEmaAtrMakerTrendRunner {
    pub async fn shutdown(&mut self, client: &impl OkxClient) -> Result<()> {
        self.ensure_initialized(client).await?;
        self.refresh_tracked_orders(client).await?;
        self.cancel_shutdown_entry_order(client).await?;
        self.reconcile_shutdown_position(client).await?;
        info!(
            strategy_id = %self.instance_id,
            instrument = %self.instrument_id,
            has_position = self.exchange()?.position.is_some(),
            has_take_profit = self.exchange()?.take_profit_order.is_some(),
            has_stop_loss = self.exchange()?.stop_loss_order.is_some(),
            has_stop_loss_exit = self.exchange()?.stop_loss_exit_order.is_some(),
            "completed OKX 1m maker trend shutdown reconciliation"
        );
        Ok(())
    }

    async fn reconcile_shutdown_position(&mut self, client: &impl OkxClient) -> Result<()> {
        if self.exchange()?.position.is_none() {
            self.shutdown_without_position(client).await?;
        } else {
            self.shutdown_with_position(client).await?;
        }
        Ok(())
    }

    async fn cancel_shutdown_entry_order(&mut self, client: &impl OkxClient) -> Result<()> {
        let Some(entry_order) = self.exchange()?.entry_order.clone() else {
            return Ok(());
        };
        self.cancel_tracked_entry_order(client, &entry_order.client_order_id)
            .await?;
        self.refresh_entry_order(client).await?;
        if self
            .regular_order_is_live(client, &entry_order.client_order_id)
            .await?
        {
            bail!(
                "shutdown left live OKX entry order {}; final state is ambiguous",
                entry_order.client_order_id
            );
        }
        if self.exchange()?.position.is_some() {
            return Ok(());
        }
        if let Some(current_order) = self.exchange_mut()?.entry_order.as_ref()
            && current_order.client_order_id == entry_order.client_order_id
        {
            self.exchange_mut()?.entry_order = None;
        }
        Ok(())
    }

    async fn shutdown_without_position(&mut self, client: &impl OkxClient) -> Result<()> {
        if let Some(take_profit_order) = self.exchange()?.take_profit_order.clone() {
            self.cancel_take_profit_order(client).await?;
            if self
                .regular_order_is_live(client, &take_profit_order.client_order_id)
                .await?
            {
                bail!(
                    "shutdown left live OKX take-profit order {}; final state is ambiguous",
                    take_profit_order.client_order_id
                );
            }
            self.exchange_mut()?.take_profit_order = None;
        }

        if let Some(stop_loss_order) = self.exchange()?.stop_loss_order.clone() {
            self.cancel_stop_loss_order(client).await?;
            self.refresh_stop_loss_order(client).await?;
            if self.exchange()?.stop_loss_order.is_some() {
                bail!(
                    "shutdown left live OKX stop-loss algo {}; final state is ambiguous",
                    stop_loss_order.algo_id
                );
            }
        }

        if let Some(stop_loss_exit_order) = self.exchange()?.stop_loss_exit_order.clone() {
            self.cancel_stop_loss_exit_order(client).await?;
            if self
                .regular_order_is_live(client, &stop_loss_exit_order.client_order_id)
                .await?
            {
                bail!(
                    "shutdown left live OKX stop-loss exit order {}; final state is ambiguous",
                    stop_loss_exit_order.client_order_id
                );
            }
            self.exchange_mut()?.stop_loss_exit_order = None;
        }
        self.clear_position_state();
        Ok(())
    }

    async fn shutdown_with_position(&mut self, client: &impl OkxClient) -> Result<()> {
        self.refresh_take_profit_order(client).await?;
        self.refresh_stop_loss_order(client).await?;
        self.refresh_stop_loss_exit_order(client).await?;
        self.evaluate_stop_loss(client).await?;
        self.reconcile_shutdown_pending_stop_loss(client).await?;
        if self.exchange()?.position.is_none() {
            self.shutdown_without_position(client).await?;
            return Ok(());
        }
        self.ensure_take_profit_order(client).await?;
        self.ensure_stop_loss_order(client).await?;

        let state = self.exchange()?;
        ensure!(
            state.stop_loss_order.is_some() || state.stop_loss_exit_order.is_some(),
            "shutdown found an open OKX strategy position without stop-loss protection; final state is ambiguous"
        );
        ensure!(
            state.take_profit_order.is_some() || state.stop_loss_exit_order.is_some(),
            "shutdown found an open OKX strategy position without take-profit or active exit protection; final state is ambiguous"
        );
        Ok(())
    }

    async fn reconcile_shutdown_pending_stop_loss(
        &mut self,
        client: &impl OkxClient,
    ) -> Result<()> {
        let Some(state) = self.exchange().ok() else {
            return Ok(());
        };
        if state.position.is_none()
            || state.stop_loss_pending.is_none()
            || state.stop_loss_order.is_some()
            || state.stop_loss_exit_order.is_some()
        {
            return Ok(());
        }

        if let Some(take_profit_order) = state.take_profit_order.clone() {
            if self
                .regular_order_is_live(client, &take_profit_order.client_order_id)
                .await?
            {
                self.cancel_take_profit_order(client).await?;
                bail!(
                    "shutdown found pending OKX stop-loss without market exit while take-profit order {} is still live; final state is ambiguous",
                    take_profit_order.client_order_id
                );
            }
            self.refresh_take_profit_order(client).await?;
        }

        let state = self.exchange()?;
        if state.position.is_none()
            || state.stop_loss_order.is_some()
            || state.stop_loss_exit_order.is_some()
        {
            return Ok(());
        }
        self.evaluate_stop_loss(client).await
    }

    async fn regular_order_is_live(
        &self,
        client: &impl OkxClient,
        client_order_id: &str,
    ) -> Result<bool> {
        Ok(client
            .order(&self.instrument_id, client_order_id)
            .await?
            .is_some_and(|order| order.is_live()))
    }
}
