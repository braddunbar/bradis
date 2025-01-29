use crate::test::{TestError, TestResult};
use respite::{RespReader, RespWriter};
use tokio::io::{DuplexStream, ReadHalf, WriteHalf};

#[derive(Debug)]
pub struct TestClient {
    pub id: i64,
    pub reader: RespReader<ReadHalf<DuplexStream>>,
    pub writer: Option<RespWriter<WriteHalf<DuplexStream>>>,
}

impl TestClient {
    pub async fn connect(stream: DuplexStream) -> TestResult<Self> {
        let (reader, writer) = tokio::io::split(stream);
        let mut writer = RespWriter::new(writer);
        let mut reader = RespReader::new(reader, Default::default());

        writer.write_inline(b"client id").await?;
        let value = reader.value().await?.ok_or(TestError::ReaderClosed)?;
        let id = value.integer().ok_or(TestError::UnexpectedValue(value))?;

        Ok(Self {
            id,
            reader,
            writer: Some(writer),
        })
    }
}
