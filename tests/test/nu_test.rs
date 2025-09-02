use crate::test::{TestClient, TestError, TestResult, command::*};
use std::{env::current_dir, sync::Mutex};

use bradis::{Addr, Server};
use hashbrown::HashMap;
use miette::{Diagnostic, MietteError, SourceSpan, SpanContents};
use nu_cli::Print;
use nu_cmd_lang::create_default_context;
use nu_command::add_shell_command_context;
use nu_engine::eval_block;
use nu_parser::parse;
use nu_protocol::{
    CompileError, ParseError, PipelineData, ShellError,
    debugger::WithoutDebug,
    engine::{EngineState, Stack, StateWorkingSet, VirtualPath},
};
use nu_std::load_standard_library;
use respite::{RespValue, RespWriter};
use thiserror::Error;
use tokio::io::{DuplexStream, WriteHalf, duplex};
use triomphe::Arc;

impl From<TestError> for ShellError {
    fn from(value: TestError) -> Self {
        ShellError::GenericError {
            error: format!("{}", value),
            msg: format!("{}", value),
            span: match value {
                TestError::Timeout(span) => Some(span),
                _ => None,
            },
            help: None,
            inner: Vec::new(),
        }
    }
}

#[derive(Diagnostic, Error)]
#[error("Nu suite failure")]
pub struct SuiteError {
    #[source_code]
    pub state: Wrap,

    #[diagnostic_source]
    pub error: NuError,
}

impl std::fmt::Debug for SuiteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

// TODO: Clean this up.
pub struct Wrap(EngineState);

impl miette::SourceCode for Wrap {
    fn read_span<'a>(
        &'a self,
        span: &SourceSpan,
        context_lines_before: usize,
        context_lines_after: usize,
    ) -> Result<Box<dyn SpanContents<'a> + 'a>, MietteError> {
        for cached_file in self.0.files() {
            let (filename, start, end) = (
                &cached_file.name,
                cached_file.covered_span.start,
                cached_file.covered_span.end,
            );
            if span.offset() >= start && span.offset() + span.len() <= end {
                let our_span = cached_file.covered_span;
                // We need to move to a local span because we're only reading
                // the specific file contents via self.get_span_contents.
                let local_span = (span.offset() - start, span.len()).into();
                let span_contents = self.0.get_span_contents(our_span);
                let span_contents = span_contents.read_span(
                    &local_span,
                    context_lines_before,
                    context_lines_after,
                )?;
                let content_span = span_contents.span();
                // Back to "global" indexing
                let retranslated = (content_span.offset() + start, content_span.len()).into();

                let data = span_contents.data();
                return Ok(Box::new(miette::MietteSpanContents::new_named(
                    (**filename).to_owned(),
                    data,
                    retranslated,
                    span_contents.line(),
                    span_contents.column(),
                    span_contents.line_count(),
                )));
            }
        }
        Err(miette::MietteError::OutOfBounds)
    }
}

#[derive(Debug, Diagnostic, Error)]
pub enum NuError {
    #[error(transparent)]
    #[diagnostic_source]
    Compile(
        #[from]
        #[diagnostic_source]
        CompileError,
    ),

    #[error(transparent)]
    #[diagnostic_source]
    Parse(
        #[from]
        #[diagnostic_source]
        ParseError,
    ),

    #[error(transparent)]
    #[diagnostic_source]
    Shell(
        #[from]
        #[diagnostic_source]
        ShellError,
    ),
}

#[allow(clippy::result_large_err)]
pub fn run(name: &str, source_code: &str) -> Result<(), SuiteError> {
    let mut state = add_shell_command_context(create_default_context());
    if let Err(error) = run_inner(&mut state, name, source_code) {
        return Err(SuiteError {
            error,
            state: Wrap(state),
        });
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn run_inner(state: &mut EngineState, name: &str, source: &str) -> Result<(), NuError> {
    let mut stack = Stack::new();
    stack.set_cwd(current_dir().unwrap())?;
    state.merge_env(&mut stack)?;
    load_standard_library(state).unwrap();

    let mut working_set = StateWorkingSet::new(state);
    let test = Arc::new(Mutex::new(None));
    working_set.add_decl(Box::new(ClientCommand(test.clone())));
    working_set.add_decl(Box::new(ClientClosedCommand(test.clone())));
    working_set.add_decl(Box::new(ClientIdCommand(test.clone())));
    working_set.add_decl(Box::new(ReadValueCommand(test.clone())));
    working_set.add_decl(Box::new(RunCommand(test.clone())));
    working_set.add_decl(Box::new(RunInlineCommand(test.clone())));
    working_set.add_decl(Box::new(TestCommand(test.clone())));
    working_set.add_decl(Box::new(Print));
    let file_id = working_set.add_file("bradis".into(), include_bytes!("../bradis.nu"));
    _ = working_set.add_virtual_path("bradis".into(), VirtualPath::File(file_id));
    let block = parse(&mut working_set, Some(name), source.as_bytes(), false);

    if let Some(error) = working_set.parse_errors.first() {
        return Err(error.clone().into());
    }

    if let Some(error) = working_set.compile_errors.first() {
        return Err(error.clone().into());
    }

    state.merge_delta(working_set.render())?;
    eval_block::<WithoutDebug>(state, &mut stack, &block, PipelineData::Empty)?;
    Ok(())
}

pub struct Test {
    pub clients: HashMap<usize, TestClient>,
    pub current: usize,
    pub server: Server,
}

impl Default for Test {
    fn default() -> Self {
        Self {
            clients: HashMap::new(),
            current: 1,
            server: Server::default(),
        }
    }
}

impl Test {
    pub fn client(&mut self) -> TestResult<&mut TestClient> {
        self.clients
            .get_mut(&self.current)
            .ok_or(TestError::MissingClient)
    }

    pub async fn connect(&mut self) -> TestResult<()> {
        let index = self.current;
        if self.clients.contains_key(&index) {
            return Ok(());
        }
        let (remote, local) = duplex(2usize.pow(8));
        let addr = Addr {
            local: format!("127.0.0.1:{index}").parse().unwrap(),
            peer: format!("1.2.3.4:{index}").parse().unwrap(),
        };
        self.server.connect(local, Some(addr));
        let client = TestClient::connect(remote).await?;
        self.clients.insert(self.current, client);
        Ok(())
    }

    pub fn writer(&mut self) -> TestResult<&mut RespWriter<WriteHalf<DuplexStream>>> {
        self.client()?
            .writer
            .as_mut()
            .ok_or(TestError::WriterDisconnected)
    }

    pub async fn read_value(&mut self) -> TestResult<RespValue> {
        let reader = &mut self.client()?.reader;
        let value = reader.value();
        value.await?.ok_or(TestError::ReaderClosed)
    }
}
