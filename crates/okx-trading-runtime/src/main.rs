#![forbid(unsafe_code)]

use anyhow::Result;

fn main() -> Result<()> {
    okx_trading_runtime::run_with_args_blocking(std::env::args().skip(1))
}
