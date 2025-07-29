use crate::string_to_uuid::StringToUuid;
use arrow::array::UInt32Array;
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
pub struct UuidVersion {
    signature: Signature,
}

impl Default for UuidVersion {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    vec![
                        DataType::FixedSizeBinary(16),
                        DataType::Utf8,
                        DataType::Utf8View,
                        DataType::LargeUtf8,
                    ],
                ),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for UuidVersion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "uuid_version"
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
        if &DataType::FixedSizeBinary(16) == input_field.data_type() {
            let Ok(CanonicalExtensionType::Uuid(_)) = input_field.try_canonical_extension_type()
            else {
                return exec_err!("Input field must contain the UUID canonical extension type");
            };
        }

        Ok(Arc::new(Field::new(self.name(), DataType::UInt32, true)))
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let input = match &args.args[0].data_type() {
            DataType::FixedSizeBinary(16) => args.args.pop().ok_or(exec_datafusion_err!(
                "Expected one input column to uuid_version"
            ))?,
            _ => {
                let string_to_uuid = StringToUuid::default();
                string_to_uuid.invoke_with_args(args)?
            }
        };

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
                                    .map(|v| v.get_version_num() as u32)
                            })
                            .transpose()
                    })
                    .collect::<DataFusionResult<Vec<_>>>()?;

                let output_array = Arc::new(UInt32Array::from(output));

                ColumnarValue::Array(output_array)
            }
            ColumnarValue::Scalar(sv) => match sv {
                ScalarValue::FixedSizeBinary(16, data) => {
                    let version = data
                        .as_ref()
                        .map(|uuid| {
                            Uuid::from_slice(uuid)
                                .map_err(|err| exec_datafusion_err!("{err}"))
                                .map(|v| v.get_version_num() as u32)
                        })
                        .transpose()?;

                    ColumnarValue::Scalar(ScalarValue::UInt32(version))
                }
                _ => return exec_err!("Invalid data type {}", sv.data_type()),
            },
        })
    }
}
