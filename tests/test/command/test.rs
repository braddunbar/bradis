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
pub struct TestCommand(pub Arc<Mutex<Option<Test>>>);

impl Command for TestCommand {
    fn name(&self) -> &str {
        "test"
    }

    fn description(&self) -> &str {
        "define a redis test"
    }

    fn signature(&self) -> Signature {
        Signature::build("test")
            .input_output_types(vec![(Type::Any, Type::Any)])
            .required("name", SyntaxShape::String, "name of the test")
            .required("body", SyntaxShape::Closure(None), "body of the test")
            .category(Category::Custom("bradis".into()))
    }

    fn run(
        &self,
        state: &EngineState,
        stack: &mut Stack,
        call: &Call,
        input: PipelineData,
    ) -> Result<PipelineData, ShellError> {
        call.req::<String>(state, stack, 0)?;
        let block: Closure = call.req(state, stack, 1)?;

        let mut test = Test::default();
        Handle::current().block_on(test.connect()).unwrap();
        *self.0.lock().unwrap() = Some(test);

        let eval_block = get_eval_block(state);
        let block = state.get_block(block.block_id);
        eval_block(state, stack, block, input)?;

        Ok(PipelineData::Empty)
    }
}
