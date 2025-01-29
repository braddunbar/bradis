use crate::config::*;
use logos::Logos;

#[derive(Clone, Copy, Debug, Eq, Hash, Logos, PartialEq)]
pub enum ConfigKey {
    #[regex(b"(?i:hash-max-listpack-entries)")]
    HashMaxListpackEntries,

    #[regex(b"(?i:hash-max-listpack-value)")]
    HashMaxListpackValue,

    #[regex(b"(?i:hash-max-ziplist-entries)")]
    HashMaxZiplistEntries,

    #[regex(b"(?i:hash-max-ziplist-value)")]
    HashMaxZiplistValue,

    #[regex(b"(?i:lazyfree-lazy-expire)")]
    LazyExpire,

    #[regex(b"(?i:lazyfree-lazy-user-del)")]
    LazyUserDel,

    #[regex(b"(?i:lazyfree-lazy-user-flush)")]
    LazyUserFlush,

    #[regex(b"(?i:list-max-listpack-size)")]
    ListMaxListpackSize,

    #[regex(b"(?i:list-max-ziplist-size)")]
    ListMaxZiplistSize,

    #[regex(b"(?i:proto-max-bulk-len)")]
    ProtoMaxBulkLen,

    #[regex(b"(?i:proto-inline-max-size)")]
    ProtoInlineMaxSize,

    #[regex(b"(?i:set-max-intset-entries)")]
    SetMaxIntsetEntries,

    #[regex(b"(?i:set-max-listpack-entries)")]
    SetMaxListpackEntries,

    #[regex(b"(?i:set-max-listpack-value)")]
    SetMaxListpackValue,

    #[regex(b"(?i:zset-max-listpack-entries)")]
    ZsetMaxListpackEntries,

    #[regex(b"(?i:zset-max-listpack-value)")]
    ZsetMaxListpackValue,

    #[regex(b"(?i:zset-max-ziplist-entries)")]
    ZsetMaxZiplistEntries,

    #[regex(b"(?i:zset-max-ziplist-value)")]
    ZsetMaxZiplistValue,

    Unknown,
}

impl ConfigKey {
    pub fn config(self) -> &'static Config {
        use ConfigKey::*;
        match self {
            HashMaxListpackEntries => &HASH_MAX_LISTPACK_ENTRIES,
            HashMaxListpackValue => &HASH_MAX_LISTPACK_VALUE,
            HashMaxZiplistEntries => &HASH_MAX_ZIPLIST_ENTRIES,
            HashMaxZiplistValue => &HASH_MAX_ZIPLIST_VALUE,
            LazyExpire => &LAZY_EXPIRE,
            LazyUserDel => &LAZY_USER_DEL,
            LazyUserFlush => &LAZY_USER_FLUSH,
            ListMaxListpackSize => &LIST_MAX_LISTPACK_SIZE,
            ListMaxZiplistSize => &LIST_MAX_ZIPLIST_SIZE,
            ProtoMaxBulkLen => &PROTOMAXBULKLEN,
            ProtoInlineMaxSize => &PROTO_INLINE_MAX_SIZE,
            SetMaxIntsetEntries => &SET_MAX_INTSET_ENTRIES,
            SetMaxListpackEntries => &SET_MAX_LISTPACK_ENTRIES,
            SetMaxListpackValue => &SET_MAX_LISTPACK_VALUE,
            ZsetMaxListpackEntries => &ZSET_MAX_LISTPACK_ENTRIES,
            ZsetMaxListpackValue => &ZSET_MAX_LISTPACK_VALUE,
            ZsetMaxZiplistEntries => &ZSET_MAX_ZIPLIST_ENTRIES,
            ZsetMaxZiplistValue => &ZSET_MAX_ZIPLIST_VALUE,
            Unknown => &UNKNOWN,
        }
    }
}
