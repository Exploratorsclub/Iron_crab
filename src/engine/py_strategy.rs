// Optionales Python-Strategie-Backend (ohne pyo3-asyncio, minimale Latenz-agnostische Variante)
#[cfg(feature = "python")]
#[allow(clippy::all)]
pub mod py {
    use anyhow::Result;
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use pyo3::prelude::*;
    use std::sync::Arc;

    use crate::engine::{EngineContext, Strategy};
    use crate::types::TradeIntent;

    pub struct PyStrategy {
        name: String,
        // Stored for potential introspection/debug; currently unused
        _module_path: String,
        _class_name: String,
        _params: serde_json::Value,
        py_obj: Mutex<PyObject>,
    }

    impl PyStrategy {
        pub async fn new(
            name: String,
            module_path: String,
            class_name: String,
            params: serde_json::Value,
        ) -> Result<Self> {
            // Synchronous init unter GIL (einmalig)
            let py_obj = Python::with_gil(|py| -> PyResult<PyObject> {
                // Use non-deprecated bound import API
                let m = PyModule::import_bound(py, &module_path)?;
                let cls = m.getattr(&class_name)?;
                // Serialize params; map JSON errors into a Python ValueError
                let params_str = serde_json::to_string(&params).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "failed to serialize params to JSON: {e}"
                    ))
                })?;
                // Call class constructor with JSON params; convert to owned PyObject
                let inst = cls.call1((params_str,))?.unbind();
                Ok(inst.into())
            })
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

            Ok(Self {
                name,
                _module_path: module_path,
                _class_name: class_name,
                _params: params,
                py_obj: Mutex::new(py_obj),
            })
        }
    }

    #[async_trait]
    impl Strategy for PyStrategy {
        fn name(&self) -> &str {
            &self.name
        }

        async fn on_tick(&self, _ctx: Arc<EngineContext>) -> Result<Vec<TradeIntent>> {
            let obj = self.py_obj.lock().clone();
            let intents = Python::with_gil(|py| -> PyResult<Vec<TradeIntent>> {
                let out = obj.call_method0(py, "on_tick")?;
                let s: String = out.extract(py)?;
                let intents: Vec<TradeIntent> = serde_json::from_str(&s).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("bad json: {e}"))
                })?;
                Ok(intents)
            })
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            Ok(intents)
        }
    }
}
