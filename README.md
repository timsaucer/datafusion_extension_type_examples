# DataFusion Extension Type Example

This repository is used to demonstrate how extension types can be used with
DataFusion, both in Rust and in Python. The implementation of scalar UDFs
is in Rust and can be found in the `rust` folder, along with an example. The
Python crate depends on the Rust crate and exposes these UDFs via PYO3.

## Python Example

To run the Python crate, you will need a few dependencies. This should produce
a minimal environment to run the example.

```shell
cd python
uv venv
uv pip install maturin pyarrow datafusion
source .venv/bin/activate
maturin develop
python examples/example_scalar_udf.py
```