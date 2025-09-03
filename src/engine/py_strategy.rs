
// Optionales Python-Strategie-Backend (ohne pyo3-asyncio, minimale Latenz-agnostische Variante)
#[cfg(feature = "python")]
pub mod py {
    use std::sync::Arc;
    use anyhow::Result;
    use async_trait::async_trait;
    use pyo3::prelude::*;

    use crate::types::TradeIntent;
    use super::{EngineContext, Strategy};

    pub struct PyStrategy {
        name: String,
        module_path: String,
        class_name: String,
        params: serde_json::Value,
        py_obj: PyObject,
    }

    impl PyStrategy {
        pub async fn new(name: String, module_path: String, class_name: String, params: serde_json::Value) -> Result<Self> {
            // Synchronous init unter GIL (einmalig)
            let py_obj = Python::with_gil(|py| -> PyResult<PyObject> {
                let m = PyModule::import(py, &module_path)?;
                let cls = m.getattr(&class_name)?;
                let inst = cls.call1((serde_json::to_string(&params)?,))?;
                Ok(inst.into())
            })?;

            Ok(Self { name, module_path, class_name, params, py_obj })
        }
    }

    #[async_trait]
    impl Strategy for PyStrategy {
        fn name(&self) -> &str { &self.name }

        async fn on_tick(&self, _ctx: Arc<EngineContext>) -> Result<Vec<TradeIntent>> {
            let obj = self.py_obj.clone();
            let intents = Python::with_gil(|py| -> PyResult<Vec<TradeIntent>> {
                let out = obj.call_method0(py, "on_tick")?;
                let s: String = out.extract(py)?;
                let intents: Vec<TradeIntent> = serde_json::from_str(&s)
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("bad json: {e}")))?;
                Ok(intents)
            })?;
            Ok(intents)
        }
    }
}
