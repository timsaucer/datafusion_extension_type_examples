use datafusion::logical_expr::ScalarUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use df_extension_types::string_to_uuid::StringToUuid;
use df_extension_types::uuid_to_string::UuidToString;
use df_extension_types::uuid_version::UuidVersion;
use paste::paste;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::sync::Arc;

macro_rules! export_rust_scalar_udf {
    ($inner_name:ident, $exported_name:literal) => {
        paste! {
            #[pyclass(name = $exported_name)]
            #[derive(Debug, Clone)]
            pub(crate) struct [<$inner_name UDF>] {
                inner: $inner_name,
            }

            #[pymethods]
            impl [<$inner_name UDF>] {
                #[new]
                fn new() -> Self {
                    Self {
                        inner: $inner_name::default(),
                    }
                }

                fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
                    let name = cr"datafusion_scalar_udf".into();

                    let func = Arc::new(ScalarUDF::new_from_impl(self.inner.clone()));
                    let provider = FFI_ScalarUDF::from(func);

                    PyCapsule::new(py, provider, Some(name))
                }
            }
        }
    };
}

export_rust_scalar_udf!(StringToUuid, "StringToUuidUDF");
export_rust_scalar_udf!(UuidVersion, "UuidVersionUDF");
export_rust_scalar_udf!(UuidToString, "UuidToStringUDF");

#[pymodule]
fn df_extension_types_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<StringToUuidUDF>()?;
    m.add_class::<UuidVersionUDF>()?;
    m.add_class::<UuidToStringUDF>()?;
    Ok(())
}
