use crate::test::{Test, TestError};
use std::sync::Mutex;

use nu_engine::CallExt;
use nu_protocol::{
    Category, PipelineData, ShellError, Signature, SyntaxShape, Type, Value,
    engine::{Call, Command, EngineState, Stack},
};
use tokio::runtime::Handle;
use triomphe::Arc;

#[derive(Clone)]
pub struct RunCommand(pub Arc<Mutex<Option<Test>>>);

impl Command for RunCommand {
    fn name(&self) -> &str {
        "run"
    }

    fn description(&self) -> &str {
        "run a redis command"
    }

    fn signature(&self) -> Signature {
        Signature::build("run")
            .input_output_types(vec![(Type::Any, Type::Any)])
            .rest(
                "args",
                SyntaxShape::OneOf(vec![SyntaxShape::String, SyntaxShape::Binary]),
                "command arguments",
            )
            .category(Category::Custom("bradis".into()))
    }

    fn run(
        &self,
        state: &EngineState,
        stack: &mut Stack,
        call: &Call,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let args: Vec<Value> = call.rest(state, stack, 0)?;

        let mut guard = self.0.lock().unwrap();
        let test = guard.as_mut().unwrap();
        let writer = test.writer()?;

        let handle = Handle::current();
        handle
            .block_on(writer.write_array(args.len()))
            .map_err(TestError::from)?;
        for arg in args {
            match arg {
                Value::Binary { val, .. } => {
                    handle
                        .block_on(writer.write_blob_string(&val))
                        .map_err(TestError::from)?;
                }
                Value::String { val, .. } => {
                    handle
                        .block_on(writer.write_blob_string(val.as_bytes()))
                        .map_err(TestError::from)?;
                }
                _ => unreachable!(),
            }
        }

        drop(guard);

        Ok(PipelineData::Empty)
    }
}
