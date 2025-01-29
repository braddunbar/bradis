use crate::{
    bytes::parse,
    client::ClientId,
    command::{self, Arity, Command, CommandKind, Keys},
    db::DBIndex,
    epoch,
    reply::ReplyError,
};
use bytes::Bytes;
use ordered_float::NotNan;
use std::{collections::VecDeque, iter::StepBy, net::SocketAddr, ops::Range};
use tokio::time::Duration;

#[derive(Clone, Debug)]
pub struct Request {
    arguments: VecDeque<Bytes>,
    pub command: &'static Command,
    next: usize,
}

impl Default for Request {
    fn default() -> Self {
        Request {
            arguments: VecDeque::new(),
            command: &command::UNKNOWN,
            next: 1,
        }
    }
}

impl Request {
    fn set_command(&mut self) {
        self.command = self
            .get(0)
            .map_or(&command::UNKNOWN, |argument| argument[..].into());
    }

    pub fn kind(&self) -> CommandKind {
        self.command.kind
    }

    pub fn next(&self) -> usize {
        self.next
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        let argument = self.arguments.pop_front();
        self.set_command();
        argument
    }

    pub fn push_front(&mut self, argument: Bytes) {
        self.arguments.push_front(argument);
        self.set_command();
    }

    pub fn reset(&mut self, next: usize) {
        self.next = next;
    }

    pub fn clear(&mut self) {
        self.next = 0;
        self.arguments.clear();
        self.command = &command::UNKNOWN;
    }

    pub fn drain(&mut self) -> impl Iterator<Item = Bytes> + '_ {
        self.arguments.drain(..)
    }

    pub fn push_back(&mut self, argument: Bytes) {
        self.arguments.push_back(argument);
        if self.len() == 1 {
            self.set_command();
            self.next = 1;
        }
    }

    pub fn len(&self) -> usize {
        self.arguments.len()
    }

    pub fn remaining(&self) -> usize {
        self.arguments.len() - self.next
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn get(&self, index: usize) -> Option<Bytes> {
        self.arguments.get(index).cloned()
    }

    pub fn iter(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.arguments.iter().skip(self.next).cloned()
    }

    /// Assert that the number of remaining arguments is a factor of 2.
    pub fn assert_pairs(&self) -> Result<(), ReplyError> {
        if self.remaining() % 2 == 0 {
            Ok(())
        } else {
            Err(self.wrong_arguments())
        }
    }

    pub fn is_valid(&self) -> bool {
        use Arity::*;
        match self.command.arity {
            Exact(arity) => self.len() == arity.into(),
            Minimum(arity) => self.len() >= arity.into(),
        }
    }

    pub fn peek(&mut self) -> Option<Bytes> {
        self.get(self.next)
    }

    pub fn pop(&mut self) -> Result<Bytes, ReplyError> {
        self.try_pop().ok_or_else(|| self.wrong_arguments())
    }

    pub fn try_pop(&mut self) -> Option<Bytes> {
        if let Some(argument) = self.peek() {
            self.next += 1;
            Some(argument)
        } else {
            None
        }
    }

    pub fn wrong_arguments(&self) -> ReplyError {
        ReplyError::WrongArguments(self.command)
    }

    pub fn unknown_subcommand(&self) -> ReplyError {
        let subcommand = self.get(1).unwrap_or_else(|| "".into());
        ReplyError::UnknownSubcommand(self.command, subcommand)
    }

    pub fn bit(&mut self) -> Result<bool, ReplyError> {
        match &self.pop()?[..] {
            b"0" => Ok(false),
            b"1" => Ok(true),
            _ => Err(ReplyError::BitArgument),
        }
    }

    pub fn bit_offset(&mut self) -> Result<usize, ReplyError> {
        self.usize().map_err(|_| ReplyError::BitOffset)
    }

    pub fn i64(&mut self) -> Result<i64, ReplyError> {
        parse(&self.pop()?).ok_or(ReplyError::Integer)
    }

    pub fn client_id(&mut self) -> Result<ClientId, ReplyError> {
        parse(&self.pop()?)
            .map(ClientId)
            .ok_or(ReplyError::InvalidClientId)
    }

    pub fn f64(&mut self) -> Result<f64, ReplyError> {
        parse(&self.pop()?).ok_or(ReplyError::Float)
    }

    pub fn u128(&mut self) -> Result<u128, ReplyError> {
        parse(&self.pop()?).ok_or(ReplyError::Integer)
    }

    pub fn finite_f64(&mut self) -> Result<f64, ReplyError> {
        let value = self.f64()?;
        if value.is_finite() {
            Ok(value)
        } else {
            Err(ReplyError::NanOrInfinity)
        }
    }

    pub fn usize(&mut self) -> Result<usize, ReplyError> {
        parse(&self.pop()?).ok_or(ReplyError::OffsetRange)
    }

    pub fn integer(&mut self) -> Result<usize, ReplyError> {
        self.usize().map_err(|_| ReplyError::Integer)
    }

    pub fn db_index(&mut self) -> Result<DBIndex, ReplyError> {
        let value = self.usize().map_err(|_| ReplyError::Integer)?;
        Ok(DBIndex(value))
    }

    pub fn addr(&mut self) -> Result<Option<SocketAddr>, ReplyError> {
        Ok(parse(&self.pop()?))
    }

    pub fn not_nan(&mut self) -> Result<NotNan<f64>, ReplyError> {
        let f = self.f64()?;
        NotNan::new(f).map_err(|_| ReplyError::Float)
    }

    pub fn timeout(&mut self) -> Result<Duration, ReplyError> {
        if self.is_empty() {
            return Err(self.wrong_arguments());
        }

        let timeout = self.f64().map_err(|_| ReplyError::InvalidTimeout)?;

        if timeout < 0_f64 {
            return Err(ReplyError::NegativeTimeout);
        }

        if !timeout.is_finite() {
            return Err(ReplyError::InfiniteTimeout);
        }

        Ok(Duration::from_secs_f64(timeout))
    }

    fn _ttl<const U: i128>(&mut self) -> Result<u128, ReplyError> {
        parse::<i128>(&self.pop()?)
            .and_then(|x| x.checked_mul(U))
            .and_then(|x| {
                let epoch = epoch().as_millis();
                let abs = x.unsigned_abs();
                if x < 0 {
                    epoch.checked_sub(abs)
                } else {
                    epoch.checked_add(abs)
                }
            })
            .ok_or(ReplyError::ExpireTime(self.command))
    }

    pub fn ttl(&mut self) -> Result<u128, ReplyError> {
        self._ttl::<1000>()
    }

    pub fn pttl(&mut self) -> Result<u128, ReplyError> {
        self._ttl::<1>()
    }

    fn _expiretime<const U: u128>(&mut self) -> Result<u128, ReplyError> {
        parse::<u128>(&self.pop()?)
            .and_then(|x| x.checked_mul(U))
            .ok_or(ReplyError::ExpireTime(self.command))
    }

    pub fn expiretime(&mut self) -> Result<u128, ReplyError> {
        self._expiretime::<1000>()
    }

    pub fn pexpiretime(&mut self) -> Result<u128, ReplyError> {
        self._expiretime::<1>()
    }

    pub fn numkeys(&mut self) -> Result<usize, ReplyError> {
        match self.i64()?.try_into() {
            Ok(n) if n > self.remaining() => Err(ReplyError::NumberOfKeys),
            Ok(n) => Ok(n),
            Err(_) => Err(ReplyError::NegativeKeys),
        }
    }

    /// Get an iterator with the index of all keys.
    pub fn keys(&self) -> Result<StepBy<Range<usize>>, ReplyError> {
        use Keys::*;
        let len = self.len();

        let keys = match self.command.keys {
            All => (1..len).step_by(1),
            Argument(index) => {
                let count: usize = self
                    .get(index)
                    .and_then(|bytes| parse(&bytes[..]))
                    .ok_or(ReplyError::InvalidCommandArguments)?;

                if len - index - 1 < count {
                    return Err(ReplyError::InvalidCommandArguments);
                }

                let start = index + 1;
                let end = start + count;

                (start..end).step_by(1)
            }
            Double => (1..3).step_by(1),
            Odd => (1..len).step_by(2),
            None => return Err(ReplyError::Nokeys),
            Single => (1..2).step_by(1),
            SkipOne => (2..len).step_by(1),
            Trailing => (1..len - 1).step_by(1),
        };

        Ok(keys)
    }
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, argument) in self.arguments.iter().enumerate() {
            if index != 0 {
                write!(f, " ")?;
            }
            write!(f, "\"")?;
            for byte in argument {
                match byte {
                    b'\'' => write!(f, "'")?,
                    b => write!(f, "{}", b.escape_ascii())?,
                }
            }
            write!(f, "\"")?;
        }
        Ok(())
    }
}
