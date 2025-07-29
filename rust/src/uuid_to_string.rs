use arrow::array::StringViewArray;
use arrow::datatypes::{Field, FieldRef};
use arrow_schema::extension::CanonicalExtensionType;
use datafusion::arrow::array::FixedSizeBinaryArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, ScalarValue};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct UuidToString {
    signature: Signature,
}

impl Default for UuidToString {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::FixedSizeBinary(16)]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for UuidToString {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uuid_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        unimplemented!()
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> datafusion::common::Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            return exec_err!("Incorrect number of arguments for uuid_version");
        }

        let input_field = &args.arg_fields[0];
        let Ok(CanonicalExtensionType::Uuid(_)) = input_field.try_canonical_extension_type() else {
            return exec_err!("Input field must contain the UUID canonical extension type");
        };

        Ok(Arc::new(Field::new(self.name(), DataType::Utf8View, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let input = &args.args[0];

        Ok(match input {
            ColumnarValue::Array(arr) => {
                let arr = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or(
                    exec_datafusion_err!("Unable to downcast UUID canonical extension array"),
                )?;

                let output = arr
                    .iter()
                    .map(|value| {
                        value
                            .map(|uuid| {
                                Uuid::from_slice(uuid)
                                    .map_err(|err| exec_datafusion_err!("{err}"))
                                    .map(|v| v.to_string())
                            })
                            .transpose()
                    })
                    .collect::<DataFusionResult<Vec<_>>>()?;

                let output_array = Arc::new(StringViewArray::from(output));

                ColumnarValue::Array(output_array)
            }
            ColumnarValue::Scalar(sv) => match sv {
                ScalarValue::FixedSizeBinary(16, data) => {
                    let data = data
                        .as_ref()
                        .map(|uuid| {
                            Uuid::from_slice(uuid)
                                .map_err(|err| exec_datafusion_err!("{err}"))
                                .map(|v| v.to_string())
                        })
                        .transpose()?;

                    ColumnarValue::Scalar(ScalarValue::Utf8View(data))
                }
                _ => return exec_err!("Invalid data type {}", sv.data_type()),
            },
        })
    }
}
