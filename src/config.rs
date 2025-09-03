mod error;
mod key;

pub use error::ConfigError;
pub use key::ConfigKey;

use crate::{
    bytes::{lex, parse},
    reply::{Reply, ReplyError},
    store::Store,
};
use bytes::Bytes;
use logos::Logos;

/// An option accepting "yes" or "no".
#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum YesNoOption {
    #[regex(b"(?i:yes)")]
    Yes,

    #[regex(b"(?i:no)")]
    No,
}

// Convert a "yes" or "no" value into a boolean.
fn yes_no(value: &[u8]) -> Result<bool, ConfigError> {
    use YesNoOption::*;
    match lex(value) {
        Some(Yes) => Ok(true),
        Some(No) => Ok(false),
        None => Err(ConfigError::YesNo),
    }
}

// Wrapper value for easy conversion to a `Reply`.
pub struct YesNo(pub bool);

pub struct Config {
    pub key: ConfigKey,
    pub name: &'static str,
    pub getter: fn(&mut Store) -> Reply,
    pub setter: fn(&Bytes, &mut Store) -> Result<(), ConfigError>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config").field("name", &self.name).finish()
    }
}

pub static PROTOMAXBULKLEN: Config = Config {
    key: ConfigKey::ProtoMaxBulkLen,
    name: "proto-max-bulk-len",
    getter: get_proto_max_bulk_len,
    setter: set_proto_max_bulk_len,
};

fn get_proto_max_bulk_len(store: &mut Store) -> Reply {
    match i64::try_from(store.reader_config.blob_limit()) {
        Ok(value) => Reply::Bulk(value.into()),
        Err(_) => ReplyError::InvalidUsize.into(),
    }
}

fn set_proto_max_bulk_len(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.reader_config.set_blob_limit(memory(value)?);
    Ok(())
}

// TODO: This is new config…what should we do with it?
pub static PROTO_INLINE_MAX_SIZE: Config = Config {
    key: ConfigKey::ProtoInlineMaxSize,
    name: "proto-inline-max-size",
    getter: get_proto_inline_max_size,
    setter: set_proto_inline_max_size,
};

fn get_proto_inline_max_size(store: &mut Store) -> Reply {
    match i64::try_from(store.reader_config.inline_limit()) {
        Ok(value) => Reply::Bulk(value.into()),
        Err(_) => ReplyError::InvalidUsize.into(),
    }
}

fn set_proto_inline_max_size(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.reader_config.set_inline_limit(memory(value)?);
    Ok(())
}

pub static HASH_MAX_ZIPLIST_ENTRIES: Config = Config {
    key: ConfigKey::HashMaxZiplistEntries,
    name: "hash-max-ziplist-entries",
    getter: get_hash_max_listpack_entries,
    setter: set_hash_max_listpack_entries,
};

pub static HASH_MAX_LISTPACK_ENTRIES: Config = Config {
    key: ConfigKey::HashMaxListpackEntries,
    name: "hash-max-listpack-entries",
    getter: get_hash_max_listpack_entries,
    setter: set_hash_max_listpack_entries,
};

fn get_hash_max_listpack_entries(store: &mut Store) -> Reply {
    match i64::try_from(store.hash_max_listpack_entries) {
        Ok(value) => Reply::Bulk(value.into()),
        Err(_) => ReplyError::InvalidUsize.into(),
    }
}

fn set_hash_max_listpack_entries(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.hash_max_listpack_entries = memory(value)?;
    Ok(())
}

pub static HASH_MAX_ZIPLIST_VALUE: Config = Config {
    key: ConfigKey::HashMaxZiplistValue,
    name: "hash-max-ziplist-value",
    getter: get_hash_max_listpack_value,
    setter: set_hash_max_listpack_value,
};

pub static HASH_MAX_LISTPACK_VALUE: Config = Config {
    key: ConfigKey::HashMaxListpackValue,
    name: "hash-max-listpack-value",
    getter: get_hash_max_listpack_value,
    setter: set_hash_max_listpack_value,
};

fn get_hash_max_listpack_value(store: &mut Store) -> Reply {
    match i64::try_from(store.hash_max_listpack_value) {
        Ok(value) => Reply::Bulk(value.into()),
        Err(_) => ReplyError::InvalidUsize.into(),
    }
}

fn set_hash_max_listpack_value(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.hash_max_listpack_value = memory(value)?;
    Ok(())
}

pub static ZSET_MAX_ZIPLIST_ENTRIES: Config = Config {
    key: ConfigKey::ZsetMaxZiplistEntries,
    name: "zset-max-ziplist-entries",
    getter: get_zset_max_listpack_entries,
    setter: set_zset_max_listpack_entries,
};

pub static ZSET_MAX_LISTPACK_ENTRIES: Config = Config {
    key: ConfigKey::ZsetMaxListpackEntries,
    name: "zset-max-listpack-entries",
    getter: get_zset_max_listpack_entries,
    setter: set_zset_max_listpack_entries,
};

fn get_zset_max_listpack_entries(store: &mut Store) -> Reply {
    match i64::try_from(store.zset_max_listpack_entries) {
        Ok(value) => Reply::Bulk(value.into()),
        Err(_) => ReplyError::InvalidUsize.into(),
    }
}

fn set_zset_max_listpack_entries(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.zset_max_listpack_entries = memory(value)?;
    Ok(())
}

pub static ZSET_MAX_ZIPLIST_VALUE: Config = Config {
    key: ConfigKey::ZsetMaxZiplistValue,
    name: "zset-max-ziplist-value",
    getter: get_zset_max_listpack_value,
    setter: set_zset_max_listpack_value,
};

pub static ZSET_MAX_LISTPACK_VALUE: Config = Config {
    key: ConfigKey::ZsetMaxListpackValue,
    name: "zset-max-listpack-value",
    getter: get_zset_max_listpack_value,
    setter: set_zset_max_listpack_value,
};

fn get_zset_max_listpack_value(store: &mut Store) -> Reply {
    match i64::try_from(store.zset_max_listpack_value) {
        Ok(value) => Reply::Bulk(value.into()),
        Err(_) => ReplyError::InvalidUsize.into(),
    }
}

fn set_zset_max_listpack_value(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.zset_max_listpack_value = memory(value)?;
    Ok(())
}

pub static SET_MAX_INTSET_ENTRIES: Config = Config {
    key: ConfigKey::SetMaxIntsetEntries,
    name: "set-max-intset-entries",
    getter: get_set_max_intset_entries,
    setter: set_set_max_intset_entries,
};

fn get_set_max_intset_entries(store: &mut Store) -> Reply {
    match i64::try_from(store.set_config.max_intset_entries) {
        Ok(value) => Reply::Bulk(value.into()),
        Err(_) => ReplyError::InvalidUsize.into(),
    }
}

fn set_set_max_intset_entries(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.set_config.max_intset_entries = memory(value)?;
    Ok(())
}

pub static SET_MAX_LISTPACK_ENTRIES: Config = Config {
    key: ConfigKey::SetMaxListpackEntries,
    name: "set-max-listpack-entries",
    getter: get_set_max_listpack_entries,
    setter: set_set_max_listpack_entries,
};

fn get_set_max_listpack_entries(store: &mut Store) -> Reply {
    match i64::try_from(store.set_config.max_listpack_entries) {
        Ok(value) => Reply::Bulk(value.into()),
        Err(_) => ReplyError::InvalidUsize.into(),
    }
}

fn set_set_max_listpack_entries(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.set_config.max_listpack_entries = memory(value)?;
    Ok(())
}

pub static SET_MAX_LISTPACK_VALUE: Config = Config {
    key: ConfigKey::SetMaxListpackValue,
    name: "set-max-listpack-value",
    getter: get_set_max_listpack_value,
    setter: set_set_max_listpack_value,
};

fn get_set_max_listpack_value(store: &mut Store) -> Reply {
    match i64::try_from(store.set_config.max_listpack_value) {
        Ok(value) => Reply::Bulk(value.into()),
        Err(_) => ReplyError::InvalidUsize.into(),
    }
}

fn set_set_max_listpack_value(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.set_config.max_listpack_value = memory(value)?;
    Ok(())
}

pub static LAZY_EXPIRE: Config = Config {
    key: ConfigKey::LazyExpire,
    name: "lazyfree-lazy-expire",
    getter: get_lazy_expire,
    setter: set_lazy_expire,
};

fn get_lazy_expire(store: &mut Store) -> Reply {
    YesNo(store.lazy_expire).into()
}

fn set_lazy_expire(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.lazy_expire = yes_no(&value[..])?;
    Ok(())
}

pub static LAZY_USER_DEL: Config = Config {
    key: ConfigKey::LazyUserDel,
    name: "lazyfree-lazy-user-del",
    getter: get_lazy_user_del,
    setter: set_lazy_user_del,
};

fn get_lazy_user_del(store: &mut Store) -> Reply {
    YesNo(store.lazy_user_del).into()
}

fn set_lazy_user_del(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.lazy_user_del = yes_no(&value[..])?;
    Ok(())
}

pub static LAZY_USER_FLUSH: Config = Config {
    key: ConfigKey::LazyUserFlush,
    name: "lazyfree-lazy-user-flush",
    getter: get_lazy_user_flush,
    setter: set_lazy_user_flush,
};

fn get_lazy_user_flush(store: &mut Store) -> Reply {
    YesNo(store.lazy_user_flush).into()
}

fn set_lazy_user_flush(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.lazy_user_flush = yes_no(&value[..])?;
    Ok(())
}

pub static LIST_MAX_LISTPACK_SIZE: Config = Config {
    key: ConfigKey::ListMaxListpackSize,
    name: "list-max-listpack-size",
    getter: get_list_max_listpack_size,
    setter: set_list_max_listpack_size,
};

pub static LIST_MAX_ZIPLIST_SIZE: Config = Config {
    key: ConfigKey::ListMaxZiplistSize,
    name: "list-max-ziplist-size",
    getter: get_list_max_listpack_size,
    setter: set_list_max_listpack_size,
};

fn get_list_max_listpack_size(store: &mut Store) -> Reply {
    store.list_max_listpack_size.into()
}

fn set_list_max_listpack_size(value: &Bytes, store: &mut Store) -> Result<(), ConfigError> {
    store.list_max_listpack_size = parse(value).ok_or(ConfigError::Integer)?;
    Ok(())
}

pub static UNKNOWN: Config = Config {
    key: ConfigKey::Unknown,
    name: "unknown",
    getter: get_unknown,
    setter: set_unknown,
};

fn get_unknown(_: &mut Store) -> Reply {
    Reply::Nil
}

fn set_unknown(_: &Bytes, _: &mut Store) -> Result<(), ConfigError> {
    Ok(())
}

fn memory(value: &[u8]) -> Result<usize, ConfigError> {
    let result = match value {
        [digits @ .., b'k' | b'K'] => parse(digits).map(|v: usize| v * 1000),
        [digits @ .., b'k' | b'K', b'b' | b'B'] => parse(digits).map(|v: usize| v * 1024),
        [digits @ .., b'm' | b'M'] => parse(digits).map(|v: usize| v * 1000 * 1000),
        [digits @ .., b'm' | b'M', b'b' | b'B'] => parse(digits).map(|v: usize| v * 1024 * 1024),
        [digits @ .., b'g' | b'G'] => parse(digits).map(|v: usize| v * 1000 * 1000 * 1000),
        [digits @ .., b'g' | b'G', b'b' | b'B'] => {
            parse(digits).map(|v: usize| v * 1024 * 1024 * 1024)
        }
        digits => parse(digits),
    };

    result.ok_or(ConfigError::Memory)
}
