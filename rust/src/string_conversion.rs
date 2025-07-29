use arrow::array::{LargeStringArray, StringViewArray};
use datafusion::arrow::array::{ArrayRef, FixedSizeBinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_datafusion_err, exec_err, ScalarValue};
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use std::any::Any;
use std::sync::Arc;
use arrow::datatypes::{Field, FieldRef};
use uuid::Uuid;

#[derive(Debug)]
pub struct StringToUuid {
    signature: Signature,
}

impl Default for StringToUuid {
    fn default() -> Self {
        Self {
            signature: Signature::new(TypeSignature::String(1), Volatility::Immutable),
        }
    }
}

macro_rules! define_from_string_array {
    ($fn_name:ident, $array_type:ty) => {
        fn $fn_name(arr: &ArrayRef) -> DataFusionResult<ArrayRef> {
            let arr = arr
                .as_any()
                .downcast_ref::<$array_type>()
                .ok_or(exec_datafusion_err!(
                    "Unable to downcast array as expected by type."
                ))?;

            let output = arr
                .iter()
                .map(|value| {
                    value
                        .map(|str_uuid| {
                            Uuid::try_parse(str_uuid)
                                .map_err(|err| exec_datafusion_err!("{err}"))
                                .map(|v| v.into_bytes())
                        })
                        .transpose()
                })
                .collect::<DataFusionResult<Vec<_>>>()?;

            let output_ref = output
                .iter()
                .map(|maybe_bytes| maybe_bytes.as_ref().map(|v| v.as_slice()))
                .collect::<Vec<_>>();

            Ok(Arc::new(FixedSizeBinaryArray::from(output_ref)))
        }
    };
}

define_from_string_array!(from_utf8_arr, StringArray);
define_from_string_array!(from_utf8view_arr, StringViewArray);
define_from_string_array!(from_large_utf8_arr, LargeStringArray);

impl ScalarUDFImpl for StringToUuid {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "string_to_uuid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        unimplemented!()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::common::Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            return exec_err!("Incorrect number of arguments for string_to_uuid");
        }
        match args.arg_fields[0].data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {},
            _ => return exec_err!("Incorrect data type for string_to_uuid"),
        }

        Ok(Arc::new(Field::new(self.name(), DataType::FixedSizeBinary(16), true).with_extension_type(arrow_schema::extension::Uuid)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let input = &args.args[0];

        Ok(match input {
            ColumnarValue::Array(arr) => match arr.data_type() {
                DataType::Utf8 => ColumnarValue::Array(from_utf8_arr(arr)?),
                DataType::LargeUtf8 => ColumnarValue::Array(from_large_utf8_arr(arr)?),
                DataType::Utf8View => ColumnarValue::Array(from_utf8view_arr(arr)?),
                _ => return exec_err!("Invalid data type {}", arr.data_type()),
            },
            ColumnarValue::Scalar(sv) => match sv {
                ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
                    let uuid = v
                        .as_ref()
                        .map(|v_str| {
                            Uuid::try_parse(v_str).map_err(|err| exec_datafusion_err!("{err}"))
                        })
                        .transpose()?
                        .map(|v| v.into_bytes().iter().cloned().collect());

                    ColumnarValue::Scalar(ScalarValue::FixedSizeBinary(16, uuid))
                }
                _ => return exec_err!("Invalid data type {}", sv.data_type()),
            },
        })
    }
}
