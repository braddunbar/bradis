use crate::test::Test;
use std::sync::Mutex;

use nu_engine::{CallExt, get_eval_block};
use nu_protocol::{
    Category, PipelineData, ShellError, Signature, SyntaxShape, Type,
    engine::{Call, Closure, Command, EngineState, Stack},
};
use tokio::runtime::Handle;
use triomphe::Arc;

#[derive(Clone)]
pub struct ClientCommand(pub Arc<Mutex<Option<Test>>>);

impl Command for ClientCommand {
    fn name(&self) -> &'static str {
        "client"
    }

    fn description(&self) -> &'static str {
        "use a particular client"
    }

    fn signature(&self) -> Signature {
        Signature::build("client")
            .input_output_types(vec![(Type::Any, Type::Any)])
            .required("index", SyntaxShape::Int, "index of the client")
            .required("body", SyntaxShape::Closure(None), "body to execute")
            .category(Category::Custom("bradis".into()))
    }

    fn run(
        &self,
        state: &EngineState,
        stack: &mut Stack,
        call: &Call,
        input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        let index: usize = call.req(state, stack, 0)?;
        let block: Closure = call.req(state, stack, 1)?;

        let mut guard = self.0.lock().unwrap();
        let test = guard.as_mut().unwrap();
        let current = test.current;
        test.current = index;
        let handle = Handle::current();
        handle.block_on(test.connect())?;
        drop(guard);

        let eval_block = get_eval_block(state);
        let block = state.get_block(block.block_id);
        let result = eval_block(state, stack, block, input)?;

        let mut guard = self.0.lock().unwrap();
        let test = guard.as_mut().unwrap();
        test.current = current;
        drop(guard);

        Ok(result)
    }
}
