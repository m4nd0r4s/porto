//! Emits pyo3's interpreter cfgs (`Py_3_12`, `Py_GIL_DISABLED`, ...) for this crate, so code
//! that differs between CPython versions (`py_compat`) can use `#[cfg(Py_3_12)]`.

fn main() {
    pyo3_build_config::use_pyo3_cfgs();
}
