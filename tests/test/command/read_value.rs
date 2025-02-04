use crate::test::{Test, TestError, TIMEOUT};
use std::{str::from_utf8, sync::Mutex};
use tokio::time::timeout;

use nu_protocol::{
    engine::{Call, Command, EngineState, Stack},
    Category, PipelineData, Record, ShellError, Signature, Span, Type, Value,
};
use respite::{RespPrimitive, RespValue};
use tokio::runtime::Handle;
use triomphe::Arc;

#[derive(Clone)]
pub struct ReadValueCommand(pub Arc<Mutex<Option<Test>>>);

fn primitive_to_value(resp: &RespPrimitive, internal_span: Span) -> Value {
    use RespPrimitive::*;
    match resp {
        Nil => Value::Nothing { internal_span },
        Integer(n) => Value::Int {
            val: *n,
            internal_span,
        },
        String(s) => {
            if let Ok(s) = from_utf8(s) {
                Value::String {
                    val: s.into(),
                    internal_span,
                }
            } else {
                Value::Binary {
                    val: s.to_vec(),
                    internal_span,
                }
            }
        }
    }
}

fn to_value(resp: &RespValue, internal_span: Span) -> Value {
    use RespValue::*;
    match resp {
        Nil => Value::Nothing { internal_span },
        String(s) => {
            if let Ok(s) = from_utf8(s) {
                Value::String {
                    val: s.into(),
                    internal_span,
                }
            } else {
                Value::Binary {
                    val: s.to_vec(),
                    internal_span,
                }
            }
        }
        Integer(n) => Value::Int {
            val: *n,
            internal_span,
        },
        Double(f) => Value::Float {
            val: **f,
            internal_span,
        },
        Verbatim(encoding, value) => {
            let mut record = Record::new();
            let encoding = from_utf8(encoding).unwrap().into();
            let value = from_utf8(value).unwrap().into();
            record.insert(
                "type",
                Value::String {
                    val: "verbatim".into(),
                    internal_span,
                },
            );
            record.insert(
                "encoding",
                Value::String {
                    val: encoding,
                    internal_span,
                },
            );
            record.insert(
                "value",
                Value::String {
                    val: value,
                    internal_span,
                },
            );
            Value::Record {
                val: record.into(),
                internal_span,
            }
        }
        Error(value) => {
            let mut record = Record::new();
            let value = from_utf8(value).unwrap().into();
            record.insert(
                "type",
                Value::String {
                    val: "error".into(),
                    internal_span,
                },
            );
            record.insert(
                "value",
                Value::String {
                    val: value,
                    internal_span,
                },
            );
            Value::Record {
                val: record.into(),
                internal_span,
            }
        }
        Array(values) => {
            let values = values
                .iter()
                .map(|value| to_value(value, internal_span))
                .collect();
            Value::List {
                vals: values,
                internal_span,
            }
        }
        Map(map) => {
            let mut value_record = Record::new();
            for (key, value) in map.iter() {
                let RespPrimitive::String(key) = key else {
                    todo!();
                };
                value_record.insert(from_utf8(key).unwrap(), to_value(value, internal_span));
            }
            let mut record = Record::new();
            record.insert(
                "type",
                Value::String {
                    val: "map".into(),
                    internal_span,
                },
            );
            record.insert(
                "value",
                Value::Record {
                    val: value_record.into(),
                    internal_span,
                },
            );
            Value::Record {
                val: record.into(),
                internal_span,
            }
        }
        Set(set) => {
            let values = set
                .iter()
                .map(|value| primitive_to_value(value, internal_span))
                .collect();
            let mut record = Record::new();
            record.insert(
                "type",
                Value::String {
                    val: "set".into(),
                    internal_span,
                },
            );
            record.insert(
                "value",
                Value::List {
                    vals: values,
                    internal_span,
                },
            );
            Value::Record {
                val: record.into(),
                internal_span,
            }
        }
        Push(values) => {
            let values = values
                .iter()
                .map(|value| to_value(value, internal_span))
                .collect();
            let mut record = Record::new();
            record.insert(
                "type",
                Value::String {
                    val: "push".into(),
                    internal_span,
                },
            );
            record.insert(
                "value",
                Value::List {
                    vals: values,
                    internal_span,
                },
            );
            Value::Record {
                val: record.into(),
                internal_span,
            }
        }
        _ => {
            let mut record = Record::new();
            record.insert(
                "type",
                Value::String {
                    val: "todo".into(),
                    internal_span,
                },
            );
            Value::Record {
                val: record.into(),
                internal_span,
            }
        }
    }
}

impl Command for ReadValueCommand {
    fn name(&self) -> &str {
        "read-value"
    }

    fn description(&self) -> &str {
        "read a value from the client"
    }

    fn signature(&self) -> Signature {
        Signature::build("ttl")
            .input_output_types(vec![(Type::Any, Type::Any)])
            .category(Category::Custom("bradis".into()))
    }

    fn run(
        &self,
        _state: &EngineState,
        _stack: &mut Stack,
        call: &Call,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let mut guard = self.0.lock().unwrap();
        let test = guard.as_mut().unwrap();
        let handle = Handle::current();
        let Ok(value) = handle.block_on(timeout(TIMEOUT, test.read_value())) else {
            return Err(TestError::Timeout(call.span()).into());
        };
        let value = to_value(&value?, call.span());
        drop(guard);

        Ok(PipelineData::Value(value, None))
    }
}
