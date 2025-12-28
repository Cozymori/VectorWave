use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyBool, PyFloat, PyInt};
use std::collections::HashSet;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::Mutex;
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
    #[allow(dead_code)]
    flush_callback: PyObject,
    worker_handle: Mutex<Option<thread::JoinHandle<()>>>,
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
            worker_handle: Mutex::new(Some(handle)),
            stop_signal: stop_tx,
        }
    }

    #[pyo3(signature = (collection, properties, uuid=None, vector=None))]
    fn add_object(&self, collection: PyObject, properties: Py<PyDict>, uuid: Option<PyObject>, vector: Option<Vec<f32>>) {
        let item = LogItem { collection, properties, uuid, vector };
        let _ = self.sender.try_send(item);
    }

    fn shutdown(&self, py: Python<'_>) {
        let _ = self.stop_signal.send(());
        py.allow_threads(|| {
            if let Ok(mut handle_guard) = self.worker_handle.lock() {
                if let Some(handle) = handle_guard.take() {
                    let _ = handle.join();
                }
            }
        });
    }
}

impl RustBatchManager {
    fn worker_loop(rx: Receiver<LogItem>, stop_rx: Receiver<()>, callback: PyObject, threshold: usize, interval_ms: u64) {
            let mut buffer = Vec::with_capacity(threshold);
            let mut last_flush = Instant::now();
            let flush_interval = Duration::from_millis(interval_ms);

            loop {
                crossbeam_channel::select! {
                    recv(stop_rx) -> msg => {
                        break;
                    }
                    recv(rx) -> msg => {
                        match msg {
                            Ok(item) => buffer.push(item),
                            Err(_) => break,
                        }
                    }
                    default(Duration::from_millis(100)) => {}
                }


                if stop_rx.is_empty() && (buffer.len() >= threshold || (last_flush.elapsed() >= flush_interval && !buffer.is_empty())) {

                     let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        Self::flush_buffer(&buffer, &callback);
                    }));
                    buffer.clear();
                    last_flush = Instant::now();
                }
            }
        }

    fn flush_buffer(buffer: &Vec<LogItem>, callback: &PyObject) {
        Python::with_gil(|py| {
            let py_list = PyList::empty(py);

            for item in buffer {
                let dict = PyDict::new(py);
                let mut set_field = |key: &str, val: PyObject| -> bool {
                    if let Err(_) = dict.set_item(key, val) { return false; }
                    true
                };

                if !set_field("collection", item.collection.clone_ref(py)) { continue; }
                if !set_field("properties", item.properties.to_object(py)) { continue; }
                let uuid_val = item.uuid.as_ref().map_or(py.None(), |u| u.clone_ref(py));
                if !set_field("uuid", uuid_val) { continue; }
                let vector_val = item.vector.as_ref().map_or(py.None(), |v| v.to_object(py));
                if !set_field("vector", vector_val) { continue; }

                if let Err(e) = py_list.append(dict) {
                     eprintln!("[RustCore] ⚠️ Failed to append item: {}", e);
                }
            }

            if let Err(e) = callback.call1(py, (py_list,)) {
                eprintln!("[RustCore] ⚠️ Flush failed: {}", e);
            }
        });
    }
}

fn process_recursive(py: Python, value: &Bound<'_, PyAny>, sensitive_set: &HashSet<String>) -> PyResult<PyObject> {
    if let Ok(dict_obj) = value.downcast::<PyDict>() {
        let new_dict = PyDict::new(py);
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
        let new_list = PyList::empty(py);
        for item in list_obj {
            new_list.append(process_recursive(py, &item, sensitive_set)?)?;
        }
        Ok(new_list.into())
    } else {
        if value.is_none() || value.is_instance_of::<PyBool>() || value.is_instance_of::<PyFloat>() || value.is_instance_of::<PyInt>() || value.is_instance_of::<PyString>() {
            Ok(value.clone().unbind())
        } else {
            match value.str() {
                Ok(s) => Ok(s.into()),
                Err(_) => Ok(PyString::new(py, "[SERIALIZATION_ERROR]").into())
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