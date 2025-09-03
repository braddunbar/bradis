use crate::test::{TIMEOUT, Test, TestError};
use std::sync::Mutex;

use nu_engine::CallExt;
use nu_protocol::{
    Category, PipelineData, ShellError, Signature, SyntaxShape, Type, Value,
    engine::{Call, Command, EngineState, Stack},
};
use tokio::{runtime::Handle, time::timeout};
use triomphe::Arc;

#[derive(Clone)]
pub struct ClientClosedCommand(pub Arc<Mutex<Option<Test>>>);

impl Command for ClientClosedCommand {
    fn name(&self) -> &'static str {
        "client closed"
    }

    fn description(&self) -> &'static str {
        "is the client closed?"
    }

    fn signature(&self) -> Signature {
        Signature::build("client closed")
            .input_output_types(vec![(Type::Any, Type::Bool)])
            .required("index", SyntaxShape::Int, "index of the client")
            .category(Category::Custom("bradis".into()))
    }

    fn run(
        &self,
        state: &EngineState,
        stack: &mut Stack,
        call: &Call,
        _input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let index: usize = call.req(state, stack, 0)?;

        let mut guard = self.0.lock().unwrap();
        let test = guard.as_mut().unwrap();
        let client = test
            .clients
            .get_mut(&index)
            .ok_or(TestError::MissingClient)?;
        let handle = Handle::current();
        let Ok(value) = handle.block_on(timeout(TIMEOUT, client.reader.value())) else {
            return Err(TestError::Timeout(call.span()).into());
        };
        let closed = value.map_err(TestError::from)?.is_none();
        drop(guard);

        Ok(PipelineData::Value(
            Value::Bool {
                val: closed,
                internal_span: call.span(),
            },
            None,
        ))
    }
}
