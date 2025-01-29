mod index;
mod key_ref;
mod raw;
mod value;

pub use index::DBIndex;
pub use key_ref::KeyRef;
pub use raw::{Raw, RawSlice, RawSliceRef};
pub use value::{
    list_is_valid, ArrayString, Edge, Extreme, Hash, HashKey, HashValue, Insertion, List, Set,
    SetRef, SetValue, SortedSet, SortedSetRef, SortedSetValue, StringSlice, StringValue, Value,
    ValueError,
};

use crate::epoch;
use hashbrown::{hash_map::EntryRef, DefaultHashBuilder, HashMap};

/// A Redis database, storing all the values and their expiration times.
#[derive(Debug, Clone)]
pub struct DB {
    /// A map containing all key value pairs in this database.
    objects: HashMap<StringValue, Value>,

    /// A map containing the expiration time of all volatile keys in this database.
    expires: HashMap<StringValue, u128>,
}

impl Default for DB {
    fn default() -> Self {
        DB {
            objects: HashMap::new(),
            expires: HashMap::new(),
        }
    }
}

impl DB {
    /// Get the value for `key`, unless it has expired.
    pub fn get<Q>(&self, key: &Q) -> Option<&Value>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        if self.is_expired(key) {
            None
        } else {
            self.objects.get(key)
        }
    }

    /// Does `key` exist in this database?
    pub fn exists<Q>(&self, key: &Q) -> bool
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Get the mutable value for `key`, unless it has expired.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut Value>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        if self.is_expired(key) {
            self.remove(key);
            None
        } else {
            self.objects.get_mut(key)
        }
    }

    /// Get the mutable value for many keys.
    pub fn get_many_mut<const N: usize, Q>(&mut self, keys: [&Q; N]) -> [Option<&mut Value>; N]
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        // TODO: Check expirations…?
        self.objects.get_many_mut(keys)
    }

    /// Get an entry ref for a `key`.
    pub fn entry_ref<'a, Q>(
        &'a mut self,
        key: &'a Q,
    ) -> EntryRef<'a, 'a, StringValue, Q, Value, DefaultHashBuilder>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        if self.is_expired(key) {
            self.remove(key);
        }
        self.objects.entry_ref(key)
    }

    /// Set the expiration time for `key`. Return `true` if the key exists, otherwise `false`.
    pub fn expire<'a, Q>(&mut self, key: &'a Q, at: u128) -> bool
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        StringValue: From<&'a Q>,
    {
        if let EntryRef::Occupied(mut entry) = self.expires.entry_ref(key) {
            if epoch().as_millis() >= *entry.get() {
                entry.remove();
                self.objects.remove(key);
                false
            } else {
                entry.insert(at);
                true
            }
        } else if let Some((key, _)) = self.objects.get_key_value(key) {
            self.expires.insert(key.clone(), at);
            true
        } else {
            false
        }
    }

    /// Remove the expiration for `key`. Return `true` if it exists.
    pub fn persist<Q>(&mut self, key: &Q) -> bool
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.expires.remove(key).is_some()
    }

    /// Insert `key` `value` pair, optionally keeping the current expiration.
    fn insert<'a, Q, V>(&mut self, key: &'a Q, value: V, keepttl: bool) -> Option<Value>
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        StringValue: From<&'a Q>,
        V: Into<Value>,
    {
        let expired = self.is_expired(key);
        if !keepttl || expired {
            self.persist(key);
        }
        let value = match self.objects.entry_ref(key) {
            EntryRef::Occupied(mut entry) => Some(entry.insert(value.into())),
            EntryRef::Vacant(entry) => {
                entry.insert(value.into());
                None
            }
        };
        if expired {
            None
        } else {
            value
        }
    }

    /// Set the `value` of `key`, removing the expiration time.
    pub fn set<'a, Q, V>(&mut self, key: &'a Q, value: V) -> Option<Value>
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        StringValue: From<&'a Q>,
        V: Into<Value>,
    {
        self.insert(key, value, false)
    }

    /// Set the `value` of `key`, keeping the expiration time.
    pub fn overwrite<'a, Q, V>(&mut self, key: &'a Q, value: V) -> Option<Value>
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        StringValue: From<&'a Q>,
        V: Into<Value>,
    {
        self.insert(key, value, true)
    }

    /// Set the `value` of `key`, with an expiration time.
    pub fn setex<'a, Q, V>(&mut self, key: &'a Q, value: V, at: u128) -> Option<Value>
    where
        Q: KeyRef<StringValue> + ?Sized + 'a,
        StringValue: From<&'a Q>,
        V: Into<Value>,
    {
        if at <= epoch().as_millis() {
            // TODO: Should this also remove the previous value?
            return None;
        }
        match self.objects.entry_ref(key) {
            EntryRef::Occupied(mut entry) => {
                self.expires.insert(entry.key().clone(), at);
                Some(entry.insert(value.into()))
            }
            EntryRef::Vacant(entry) => {
                let occupied = entry.insert_entry(value.into());
                self.expires.insert(occupied.key().clone(), at);
                None
            }
        }
    }

    /// Remove `key` from this database.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<Value>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        let expired = self.is_expired(key);
        self.persist(key);
        let value = self.objects.remove(key);
        if expired {
            None
        } else {
            value
        }
    }

    /// Return the time until `key` expires in milliseconds.
    pub fn ttl(&self, key: impl AsRef<[u8]>) -> Option<u128> {
        let x = self.expires.get(key.as_ref())?;
        if epoch().as_millis() >= *x {
            None
        } else {
            Some(*x - epoch().as_millis())
        }
    }

    /// Return the expiration time for `key` in milliseconds.
    pub fn expires_at(&self, key: impl AsRef<[u8]>) -> Option<u128> {
        // TODO: Check if already expired…?
        self.expires.get(key.as_ref()).copied()
    }

    /// Is `key` expired?
    fn is_expired<Q>(&self, key: &Q) -> bool
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        match self.expires.get(key) {
            Some(x) => epoch().as_millis() >= *x,
            None => false,
        }
    }

    /// Iterate over all keys in this database.
    pub fn keys(&self) -> impl Iterator<Item = StringValue> + '_ {
        self.objects.keys().filter_map(move |key| {
            if self.is_expired(key) {
                None
            } else {
                Some(key.clone())
            }
        })
    }

    /// The number of values in this database.
    pub fn size(&self) -> usize {
        self.objects.len()
    }

    /// Get a reference to a hash value. Return an error if the type is wrong.
    pub fn get_hash<Q>(&self, key: &Q) -> Result<Option<&Hash>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get(key).map(Value::as_hash).transpose()
    }

    /// Get a mutable reference to a hash value. Return an error if the type is wrong.
    pub fn mut_hash<Q>(&mut self, key: &Q) -> Result<Option<&mut Hash>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get_mut(key).map(Value::mut_hash).transpose()
    }

    /// Get a mutable reference to a hash value. Insert it if it doesn't exist. Return an error if
    /// the type is wrong.
    pub fn hash_or_default<'a, Q>(&'a mut self, key: &'a Q) -> Result<&'a mut Hash, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
        StringValue: From<&'a Q>,
    {
        self.entry_ref(key).or_insert_with(Value::hash).mut_hash()
    }

    /// Get a reference to a list value. Return an error if the type is wrong.
    pub fn get_list<Q>(&self, key: &Q) -> Result<Option<&List>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get(key).map(Value::as_list).transpose()
    }

    /// Get a mutable reference to a list value. Return an error if the type is wrong.
    pub fn mut_list<Q>(&mut self, key: &Q) -> Result<Option<&mut List>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get_mut(key).map(Value::mut_list).transpose()
    }

    /// Get a mutable reference to a list value. Insert it if it doesn't exist. Return an error if
    /// the type is wrong.
    pub fn list_or_default<'a, Q>(&'a mut self, key: &'a Q) -> Result<&'a mut List, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
        StringValue: From<&'a Q>,
    {
        self.entry_ref(key).or_insert_with(Value::list).mut_list()
    }

    /// Get a reference to a set value. Return an error if the type is wrong.
    pub fn get_set<Q>(&self, key: &Q) -> Result<Option<&Set>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get(key).map(Value::as_set).transpose()
    }

    /// Get a mutable reference to a set value. Return an error if the type is wrong.
    pub fn mut_set<Q>(&mut self, key: &Q) -> Result<Option<&mut Set>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get_mut(key).map(Value::mut_set).transpose()
    }

    /// Get a mutable reference to a set value. Insert it if it doesn't exist. Return an error if
    /// the type is wrong.
    pub fn set_or_default<'a, Q>(&'a mut self, key: &'a Q) -> Result<&'a mut Set, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
        StringValue: From<&'a Q>,
    {
        self.entry_ref(key).or_insert_with(Value::set).mut_set()
    }

    /// Get a reference to a sorted set value. Return an error if the type is wrong.
    pub fn get_sorted_set<Q>(&self, key: &Q) -> Result<Option<&SortedSet>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get(key).map(Value::as_sorted_set).transpose()
    }

    /// Get a mutable reference to a sorted set value. Return an error if the type is wrong.
    pub fn mut_sorted_set<Q>(&mut self, key: &Q) -> Result<Option<&mut SortedSet>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get_mut(key).map(Value::mut_sorted_set).transpose()
    }

    /// Get a mutable reference to a sorted set value. Insert it if it doesn't exist. Return an
    /// error if the type is wrong.
    pub fn sorted_set_or_default<'a, Q>(
        &'a mut self,
        key: &'a Q,
    ) -> Result<&'a mut SortedSet, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
        StringValue: From<&'a Q>,
    {
        self.entry_ref(key)
            .or_insert_with(Value::sorted_set)
            .mut_sorted_set()
    }

    /// Get a reference to a string value. Return an error if the type is wrong.
    pub fn get_string<Q>(&self, key: &Q) -> Result<Option<&StringValue>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get(key).map(Value::as_string).transpose()
    }

    /// Get a mutable reference to a string value. Return an error if the type is wrong.
    pub fn mut_string<Q>(&mut self, key: &Q) -> Result<Option<&mut StringValue>, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
    {
        self.get_mut(key).map(Value::mut_string).transpose()
    }

    /// Get a mutable reference to a string value. Insert it if it doesn't exist. Return an error
    /// if the type is wrong.
    pub fn string_or_default<'a, Q>(
        &'a mut self,
        key: &'a Q,
    ) -> Result<&'a mut StringValue, ValueError>
    where
        Q: KeyRef<StringValue> + ?Sized,
        StringValue: From<&'a Q>,
    {
        self.entry_ref(key)
            .or_insert_with(Value::string)
            .mut_string()
    }
}

#[cfg(test)]
#[cfg(not(miri))]
mod tests {
    use super::*;

    #[test]
    fn set() {
        let mut db = DB::default();
        assert_eq!(db.set(b"a", "x"), None);
        assert_eq!(db.get(b"a"), Some(&"x".into()));
        assert_eq!(db.set(b"a", "y"), Some("x".into()));
    }

    #[test]
    fn set_expired() {
        let mut db = DB::default();
        assert_eq!(db.set(b"a", "x"), None);
        db.expire(b"a", epoch().as_millis() - 10_000);
        assert_eq!(db.set(b"a", "y"), None);
    }

    #[test]
    fn setex() {
        let mut db = DB::default();
        assert_eq!(db.setex(b"a", "x", epoch().as_millis() + 10_000), None);
        assert_eq!(db.get(b"a"), Some(&"x".into()));
        assert!((9995..10_006).contains(&db.ttl("a").unwrap()));
    }

    #[test]
    fn keys() {
        let mut db = DB::default();
        db.setex(b"a", "x", epoch().as_millis());
        db.setex(b"b", "x", epoch().as_millis() + 10_000);
        db.set(b"c", "x");
        let keys: Vec<_> = db.keys().collect();
        assert!(keys.contains(&"b".into()));
        assert!(keys.contains(&"c".into()));
    }

    #[test]
    fn remove_expired_returns_none() {
        let mut db = DB::default();
        db.set(b"x", "1");
        db.expire(b"x", epoch().as_millis() - 10_000);
        assert_eq!(db.remove(b"x"), None);
    }
}
