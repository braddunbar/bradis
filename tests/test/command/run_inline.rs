use crate::test::{Test, TestError};
use std::sync::Mutex;

use nu_engine::{get_eval_block, CallExt};
use nu_protocol::{
    engine::{Call, Closure, Command, EngineState, Stack},
    Category, PipelineData, ShellError, Signature, SyntaxShape, Type,
};
use tokio::runtime::Handle;
use triomphe::Arc;

#[derive(Clone)]
pub struct RunInlineCommand(pub Arc<Mutex<Option<Test>>>);

impl Command for RunInlineCommand {
    fn name(&self) -> &str {
        "run-inline"
    }

    fn description(&self) -> &str {
        "run an inline redis command"
    }

    fn signature(&self) -> Signature {
        Signature::build("run")
            .input_output_types(vec![(Type::Any, Type::Any)])
            .required("line", SyntaxShape::String, "the inline command to run")
            .optional(
                "assertion block",
                SyntaxShape::Closure(None),
                "the assertion to run",
            )
            .category(Category::Custom("bradis".into()))
    }

    fn run(
        &self,
        state: &EngineState,
        stack: &mut Stack,
        call: &Call,
        input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let line: String = call.req(state, stack, 0)?;
        let body: Option<Closure> = call.opt(state, stack, 1)?;

        let mut guard = self.0.lock().unwrap();
        let test = guard.as_mut().unwrap();
        let client = test.client()?;
        let writer = client
            .writer
            .as_mut()
            .ok_or(TestError::WriterDisconnected)?;

        let handle = Handle::current();
        handle
            .block_on(writer.write_inline(line.as_bytes()))
            .unwrap();
        drop(guard);

        if let Some(closure) = body {
            let eval_block = get_eval_block(state);
            let block = state.get_block(closure.block_id);
            eval_block(state, stack, block, input)?;
        }

        Ok(PipelineData::Empty)
    }
}
