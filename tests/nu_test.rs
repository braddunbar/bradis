#[cfg(not(miri))]
mod test;

macro_rules! nu_test {
    ($name:ident, $file:expr) => {
        #[test]
        #[cfg(not(miri))]
        fn $name() -> miette::Result<()> {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let _guard = runtime.enter();
            test::run($file, include_str!($file))?;
            Ok(())
        }
    };
}

nu_test!(bitops, "bitops.nu");
nu_test!(client, "client.nu");
nu_test!(config, "config.nu");
nu_test!(db, "db.nu");
nu_test!(eval, "eval.nu");
nu_test!(expire, "expire.nu");
nu_test!(hash, "hash.nu");
nu_test!(keys, "keys.nu");
nu_test!(list, "list.nu");
nu_test!(multi, "multi.nu");
nu_test!(protocol, "protocol.nu");
nu_test!(pubsub, "pubsub.nu");
nu_test!(server, "server.nu");
nu_test!(set, "set.nu");
nu_test!(sorted_set, "sorted_set.nu");
nu_test!(store, "store.nu");
nu_test!(string, "string.nu");
