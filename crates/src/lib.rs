use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyBool, PyFloat, PyInt};
use std::collections::HashSet;
use std::thread;
use std::time::{Duration, Instant};
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

struct LogItem {
    collection: PyObject,
    properties: Py<PyDict>,
    uuid: Option<PyObject>,
    vector: Option<Vec<f32>>,
}

#[pyclass]
struct RustBatchManager {
    sender: Sender<LogItem>,
    flush_callback: PyObject,
    worker_handle: Option<thread::JoinHandle<()>>,
    stop_signal: Sender<()>,
}

#[pymethods]
impl RustBatchManager {
    #[new]
    fn new(
        py: Python<'_>,
        callback: PyObject,
        batch_threshold: usize,
        flush_interval_ms: u64,
    ) -> Self {
        let (tx, rx) = bounded::<LogItem>(10000);
        let (stop_tx, stop_rx) = bounded(1);

        let worker_callback = callback.clone_ref(py);

        let handle = thread::spawn(move || {
            Self::worker_loop(rx, stop_rx, worker_callback, batch_threshold, flush_interval_ms);
        });

        RustBatchManager {
            sender: tx,
            flush_callback: callback,
            worker_handle: Some(handle),
            stop_signal: stop_tx,
        }
    }

    #[pyo3(signature = (collection, properties, uuid=None, vector=None))]
    fn add_object(
        &self,
        collection: PyObject,
        properties: Py<PyDict>,
        uuid: Option<PyObject>,
        vector: Option<Vec<f32>>,
    ) {
        let item = LogItem {
            collection,
            properties,
            uuid,
            vector,
        };

        match self.sender.try_send(item) {
            Ok(_) => {},
            Err(TrySendError::Full(_)) => {
                eprintln!("[RustCore] 🚨 Queue Full! Dropping log item.");
            },
            Err(TrySendError::Disconnected(_)) => {
                eprintln!("[RustCore] ❌ Channel disconnected.");
            }
        }
    }

    fn shutdown(&self) {
        let _ = self.stop_signal.send(());
    }
}

impl RustBatchManager {
    fn worker_loop(
        rx: Receiver<LogItem>,
        stop_rx: Receiver<()>,
        callback: PyObject,
        threshold: usize,
        interval_ms: u64,
    ) {
        let mut buffer = Vec::with_capacity(threshold);
        let mut last_flush = Instant::now();
        let flush_interval = Duration::from_millis(interval_ms);

        loop {
            if let Ok(_) = stop_rx.try_recv() {
                if !buffer.is_empty() {
                    Self::flush_buffer(&buffer, &callback);
                }
                break;
            }

            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(item) => buffer.push(item),
                Err(_) => {}
            }

            let time_since_flush = last_flush.elapsed();
            if buffer.len() >= threshold || (time_since_flush >= flush_interval && !buffer.is_empty()) {
                Self::flush_buffer(&buffer, &callback);
                buffer.clear();
                last_flush = Instant::now();
            }
        }
    }

    fn flush_buffer(buffer: &Vec<LogItem>, callback: &PyObject) {
        Python::with_gil(|py| {
            let py_list = PyList::empty_bound(py);

            for item in buffer {
                let dict = PyDict::new_bound(py);
                let _ = dict.set_item("collection", &item.collection);
                let _ = dict.set_item("properties", &item.properties);

                if let Some(uuid) = &item.uuid {
                    let _ = dict.set_item("uuid", uuid);
                } else {
                    let _ = dict.set_item("uuid", py.None());
                }

                if let Some(vec) = &item.vector {
                    let _ = dict.set_item("vector", vec);
                } else {
                    let _ = dict.set_item("vector", py.None());
                }

                let _ = py_list.append(dict);
            }

            if let Err(e) = callback.call1(py, (py_list,)) {
                eprintln!("[RustCore] ❌ Callback failed: {}", e);
                e.print_and_set_sys_last_vars(py);
            }
        });
    }
}


fn process_recursive(py: Python, value: &Bound<'_, PyAny>, sensitive_set: &HashSet<String>) -> PyResult<PyObject> {
    if let Ok(dict_obj) = value.downcast::<PyDict>() {
        let new_dict = PyDict::new_bound(py);
        for (k, v) in dict_obj {
            let k_str = k.to_string().to_lowercase();
            if sensitive_set.contains(&k_str) {
                new_dict.set_item(k, "[MASKED]")?;
            } else {
                new_dict.set_item(k, process_recursive(py, &v, sensitive_set)?)?;
            }
        }
        Ok(new_dict.into())
    } else if let Ok(list_obj) = value.downcast::<PyList>() {
        let new_list = PyList::empty_bound(py);
        for item in list_obj {
            new_list.append(process_recursive(py, &item, sensitive_set)?)?;
        }
        Ok(new_list.into())
    } else {
        if value.is_none()
           || value.is_instance_of::<PyBool>()
           || value.is_instance_of::<PyFloat>()
           || value.is_instance_of::<PyInt>()
           || value.is_instance_of::<PyString>() {
            // [수정] unbind()를 사용하여 PyObject로 변환
            Ok(value.clone().unbind())
        } else {
            match value.str() {
                Ok(s) => Ok(s.into()),
                Err(_) => Ok(PyString::new_bound(py, "[SERIALIZATION_ERROR]").into())
            }
        }
    }
}

#[pyfunction]
fn mask_and_serialize(py: Python, data: &Bound<'_, PyAny>, sensitive_keys: Vec<String>) -> PyResult<PyObject> {
    let sensitive_set: HashSet<String> = sensitive_keys.into_iter().map(|s| s.to_lowercase()).collect();
    process_recursive(py, data, &sensitive_set)
}

#[pymodule]
fn vectorwave_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustBatchManager>()?;
    m.add_function(wrap_pyfunction!(mask_and_serialize, m)?)?;
    Ok(())
}