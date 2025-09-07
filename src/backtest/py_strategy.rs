//! Optional Python strategy bindings for backtests (feature `python`).
//! This is a lightweight stub to keep tooling (rustfmt) happy when the feature is disabled.
//!
//! Real IPC/PyProc strategy for backtests lives in `backtest::engine::py_strategy_adapter`.
//! When enabling the `python` feature, you can add pyo3-based bindings here.

#[cfg(feature = "python")]
mod real {
    // Place real python-backed strategy types here if needed in the future.
}
