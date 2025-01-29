use crate::test::Test;
use nu_protocol::{
    engine::{Call, Command, EngineState, Stack},
    Category, PipelineData, ShellError, Signature, Type, Value,
};
use std::sync::Mutex;
use triomphe::Arc;

#[derive(Clone)]
pub struct ClientIdCommand(pub Arc<Mutex<Option<Test>>>);

impl Command for ClientIdCommand {
    fn name(&self) -> &str {
        "client-id"
    }

    fn description(&self) -> &str {
        "get the client id"
    }

    fn signature(&self) -> Signature {
        Signature::build("client-id")
            .input_output_types(vec![(Type::Any, Type::String)])
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
        let id = test.client()?.id;
        drop(guard);

        Ok(PipelineData::Value(
            Value::String {
                val: format!("{}", id),
                internal_span: call.span(),
            },
            None,
        ))
    }
}
