use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::thread;
use std::time::{Duration, Instant};
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

struct LogItem {
    collection: Py<PyAny>,
    properties: Py<PyDict>,
    uuid: Option<Py<PyAny>>,
    vector: Option<Vec<f32>>,
}

#[pyclass]
struct RustBatchManager {
    sender: Sender<LogItem>,
    flush_callback: Py<PyAny>,
    worker_handle: Option<thread::JoinHandle<()>>,
    stop_signal: Sender<()>,
}

#[pymethods]
impl RustBatchManager {
    #[new]
    fn new(
        callback: Py<PyAny>,
        batch_threshold: usize,
        flush_interval_ms: u64,
    ) -> Self {
        let (tx, rx) = bounded::<LogItem>(10000);
        let (stop_tx, stop_rx) = bounded(1);

        let worker_callback = callback.clone();

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

    fn add_object(
        &self,
        collection: Py<PyAny>,
        properties: Py<PyDict>,
        uuid: Option<Py<PyAny>>,
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
        callback: Py<PyAny>,
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

    fn flush_buffer(buffer: &Vec<LogItem>, callback: &Py<PyAny>) {
        Python::with_gil(|py| {
            let py_list = PyList::empty(py);

            for item in buffer {
                let dict = PyDict::new(py);
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

#[pymodule]
fn vectorwave_core(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RustBatchManager>()?;
    Ok(())
}