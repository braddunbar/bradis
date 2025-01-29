use bradis *
use std/assert

test "client: list" {
  let id1 = client 1 { client-id }
  let id2 = client 2 { client-id }

  run client list
  assert str contains (read-string) $"id=($id1)"
  run client list
  assert str contains (read-string) $"id=($id2)"

  run client list id $id1 $id2
  assert str contains (read-string) $"id=($id1)"
  run client list id $id1 $id2
  assert str contains (read-string) $"id=($id2)"

  run client list id $id1
  assert str contains (read-string) $"id=($id1)"
  run client list id $id1
  assert not (read-string | str contains $"id=($id2)")

  run client list id $id2
  assert str contains (read-string) $"id=($id2)"
  run client list id $id2
  assert not (read-string | str contains $"id=($id1)")
}

test "client info" {
  run client getname; nil
  assert equal "" (client info 1 name)
  run client setname x; ok
  run client setname "x x"; err "ERR Client names cannot contain spaces, newlines or special characters."
  run client getname; str x
  assert equal x (client info 1 name)
  run client setname ""; ok
  run client getname; nil
  assert equal "" (client info 1 name)
}

test "client info: pubsub" {
  client 2 {
    client info await 1 sub "0"
    client info await 1 psub "0"
  }

  run subscribe x y z
  array [subscribe x 1]
  array [subscribe y 2]
  array [subscribe z 3]

  client 2 {
    client info await 1 sub "3"
    client info await 1 psub "0"
  }

  run psubscribe x* y* z*
  array [psubscribe x* 4]
  array [psubscribe y* 5]
  array [psubscribe z* 6]

  client 2 {
    client info await 1 sub "3"
    client info await 1 psub "3"
  }

  run unsubscribe x
  array [unsubscribe x 5]
  run punsubscribe x*
  array [punsubscribe x* 4]

  client 2 {
    client info await 1 sub "2"
    client info await 1 psub "2"
  }

  run unsubscribe
  run punsubscribe

  client 2 {
    client info await 1 sub "0"
    client info await 1 psub "0"
  }
}

test "client info: multi" {
  client 2 { assert equal "-1" (client info 1 multi) }
  run multi; ok
  client 2 { assert equal "0" (client info 1 multi) }
  run get x; str QUEUED
  client 2 { assert equal "1" (client info 1 multi) }
  run get y; str QUEUED
  client 2 { assert equal "2" (client info 1 multi) }
  run exec; array [null null]
  client 2 { assert equal "-1" (client info 1 multi) }
}

test "client info: multi error" {
  client 2 { assert equal "-1" (client info 1 multi) }
  run multi; ok
  client 2 { assert equal "0" (client info 1 multi) }
  run get x; str QUEUED
  client 2 { assert equal "1" (client info 1 multi) }
  run set; err "ERR wrong number of arguments for 'set' command"
  client 2 { assert equal "1" (client info 1 multi) }
  run exec; err "EXECABORT Transaction discarded because of previous errors."
  client 2 { assert equal "-1" (client info 1 multi) }
}

test "client info: db" {
  assert equal "0" (client info 1 db)
  run select 5; ok
  assert equal "5" (client info 1 db)
  assert equal "1.2.3.4:1" (client info 1 addr)
  assert equal "127.0.0.1:1" (client info 1 laddr)
}

test "client kill" {
  let id = client-id
  client 2 { run client kill id $id; int 1 }
  assert (client closed 1)
}

test "client kill: laddr" {
  run get x; nil
  client 2 { run client kill laddr "127.0.0.1:1"; int 1 }
  assert (client closed 1)
}

test "client kill: addr" {
  run get x; nil
  client 2 { run client kill addr "1.2.3.4:1"; int 1 }
  assert (client closed 1)
}

test "client kill: addr - old syntax" {
  run get x; nil
  client 2 { run client kill "1.2.3.4:1"; int 1 }
  assert (client closed 1)
}

test "client kill: skipme no" {
  let id = client-id
  run client kill id $id skipme no; int 1
  assert (client closed 1)
}

test "client kill: skipme yes" {
  let id = client-id
  run client kill id $id skipme yes; int 0
  run get x; nil
}

test "client kill: skipme default" {
  let id = client-id
  run client kill id $id; int 0
  run get x; nil
}

test "client kill: blocking" {
  let id = client-id
  run blpop l 0
  client 2 {
    await-flag 1 b
    run client kill id $id; int 1
  }
  assert (client closed 1)
}

test "client: help" {
  discard hello 3
  run client help
  assert str contains (read-string) CLIENT
}

test "client reply off" {
  run client reply off
  run set x 1
  run set x 2
  run set x 3
  run get x
  run client reply on; ok
  run get x; str 3
}

test "client reply skip" {
  run client reply skip
  run incr x
  run incr x; int 2
  run client reply skip
  run incr x
  run incr x; int 4
}

test "client reply skip: multiple replies" {
  run rpush x 1 2 3; int 3
  run client reply skip
  run lrange x 0 "-1"
  run llen x; int 3
  run client reply skip
  run lrange x 0 "-1"
  run llen x; int 3
}

test "client id" {
  let id = client-id | into int
  run client id; int $id
}

test "select within multi" {
  run select 5; ok
  run set x 5; ok
  run select 6; ok
  run set x 6; ok
  run multi; ok
  run select 5; str QUEUED
  run get x; str QUEUED
  run select 6; str QUEUED
  run get x; str QUEUED
  run exec; array [OK "5" OK "6"]
}

test "quit within multi" {
  run multi; ok
  run quit; ok
  assert (client closed 1)
}

test "quit" {
  run quit; ok
  assert (client closed 1)
}

test "unblock" {
  let id = client-id
  run blpop l 0
  client 2 {
    await-flag 1 b
    run client unblock $id; int 1
    noflag 1 b
  }
  nil
  run ping; str PONG
}

test "unblock failed" {
  let id = client-id
  client 2 { run client unblock $id; int 0 }
}

test "unblock timeout" {
  let id = client-id
  run blpop l 0
  client 2 {
    await-flag 1 b
    run client unblock $id timeout; int 1
  }
  nil
  run ping; str PONG
}

test "unblock error" {
  let id = client-id
  run blpop l 0
  client 2 {
    await-flag 1 b
    run client unblock $id error; int 1
  }
  err "UNBLOCKED client unblocked via CLIENT UNBLOCK"
  run ping; str PONG
}

test "unblock syntax" {
  let id = client-id
  client 2 { run client unblock $id foo; err "ERR syntax error" }
}

test "command help" {
  discard hello 3
  run command help
  assert str contains (read-string) COMMAND
}

test "command list" {
  run command list
  let value = read-value
  assert ("get" in $value)
  assert ("set" in $value)
}

test "command list pattern" {
  run command list filterby pattern com*
  assert equal (read-value) [command]
  run command list filterby pattern app*
  assert equal (read-value) [append]
  run command list filterby pattern lr*
  assert equal (read-value) [lrange lrem]
}

test "info" {
  run discard info
}

test "reset clears name" {
  run client setname foo; ok
  run reset; str RESET
  run client getname; nil
}

test "reset discards multi" {
  run watch x; ok
  run set x 1; ok
  run multi; ok
  run set x 2; str QUEUED
  run reset; str RESET
  run exec; err "ERR EXEC without MULTI"

  # Make sure the queue is cleared and no keys are watched
  run multi; ok
  run get x; str QUEUED
  run exec; array ["1"]
}

test "reset reply mode on" {
  run client reply off
  run set x 1
  run reset; str RESET
  run get x; str 1
}

test "reset selects db 0" {
  run set x 0; ok
  run select 1; ok
  run set x 1; ok
  run reset; str RESET
  run get x; str 0
  assert equal "0" (client info 1 db)
}

test "reset clears pubsub mode" {
  run subscribe x; array [subscribe x 1]
  run psubscribe x*; array [psubscribe x* 2]
  run reset; str RESET
  run get x; nil
  run unsubscribe; array [unsubscribe null 0]
  run punsubscribe; array [punsubscribe null 0]
}

test "reset resets resp version" {
  discard hello 3
  run subscribe x; push [subscribe x 1]
  run reset; str RESET
  run subscribe x; array [subscribe x 1]
}

test "multi/exec flag" {
  client 2 { noflag 1 x }
  run multi; ok
  run get x; str QUEUED
  client 2 { flag 1 x }
  run exec; array [null]
  client 2 { noflag 1 x }
}

test "client info: cmd" {
  run get x; nil
  client 2 { assert equal get (client info 1 cmd) }
  run set x 1; ok
  client 2 { assert equal set (client info 1 cmd) }
}

test "client info: resp" {
  client 2 { assert equal "2" (client info 1 resp) }
  discard hello 3
  client 2 { assert equal "3" (client info 1 resp) }
  discard hello 2
  client 2 { assert equal "2" (client info 1 resp) }
}

test "client info: section" {
  run info
  let value = read-value
  assert str contains $value "#Server"
  assert str contains $value "#Persistence"

  run info server
  let value = read-value
  assert str contains $value "#Server"

  run info server
  let value = read-value
  assert not ($value | str contains "#Persistence")

  run info server stats
  let value = read-value
  assert str contains $value "#Server"
  assert str contains $value "#Stats"

  run info server stats
  let value = read-value
  assert not ($value | str contains "#Persistence")
}

test "dirty flag" {
  client 2 { noflag 1 d }
  run watch x; ok
  run set x 1; ok
  client 2 { flag 1 d }
  run multi; ok
  run get x; str QUEUED
  run exec; nil
  client 2 { noflag 1 d }
}

test "monitor flag" {
  client 2 { noflag 1 O }
  client 1 { run monitor; ok }
  client 2 { flag 1 O }
  client 1 { run reset; str RESET }
  client 2 { noflag 1 O }
}

test "echo with crlf" {
  run echo "a\r\nb"
  str "a\r\nb"
}

test "blocking flag" {
  run blpop x 0

  client 2 {
    await-flag 1 b
    run rpush x 1; int 1
    noflag 1 b
  }

  array [x "1"]
}

test "client: wrong arguments" {
  run client; err "ERR wrong number of arguments for 'client' command"
}

test "client getname: wrong arguments" {
  run client getname invalid; err "ERR Unknown subcommand or wrong number of arguments for 'getname'. Try CLIENT HELP."
}

test "client unblock: wrong arguments" {
  run client unblock; err "ERR Unknown subcommand or wrong number of arguments for 'unblock'. Try CLIENT HELP."
  run client unblock 1 timeout invalid; err "ERR Unknown subcommand or wrong number of arguments for 'unblock'. Try CLIENT HELP."
}

test "client info: wrong arguments" {
  run client info invalid; err "ERR Unknown subcommand or wrong number of arguments for 'info'. Try CLIENT HELP."
}

test "client id: wrong arguments" {
  run client id invalid; err "ERR Unknown subcommand or wrong number of arguments for 'id'. Try CLIENT HELP."
}

test "client help: wrong arguments" {
  run client help invalid; err "ERR Unknown subcommand or wrong number of arguments for 'help'. Try CLIENT HELP."
}

test "client reply: wrong arguments" {
  run client reply; err "ERR Unknown subcommand or wrong number of arguments for 'reply'. Try CLIENT HELP."
  run client reply foo bar; err "ERR Unknown subcommand or wrong number of arguments for 'reply'. Try CLIENT HELP."
}

test "client setname: wrong arguments" {
  run client setname; err "ERR Unknown subcommand or wrong number of arguments for 'setname'. Try CLIENT HELP."
}

test "command help: wrong arguments" {
  run command help invalid; err "ERR Unknown subcommand or wrong number of arguments for 'help'. Try COMMAND HELP."
}

test "command count: wrong arguments" {
  run command count invalid; err "ERR Unknown subcommand or wrong number of arguments for 'count'. Try COMMAND HELP."
}

test "command getkeys: wrong arguments" {
  run command getkeys; err "ERR Unknown subcommand or wrong number of arguments for 'getkeys'. Try COMMAND HELP."
}

test "select: wrong arguments" {
  run select; err "ERR wrong number of arguments for 'select' command"
  run select 2 3; err "ERR wrong number of arguments for 'select' command"
}

test "watch: wrong arguments" {
  run watch; err "ERR wrong number of arguments for 'watch' command"
}

test "unwatch: wrong arguments" {
  run unwatch 2; err "ERR wrong number of arguments for 'unwatch' command"
}

test "command getkeys" {
  run command getkeys get k x; err "ERR Invalid number of arguments specified for command"
  run command getkeys invalidcommand; err "ERR Invalid command specified"
  run command getkeys append k x; array [k]
  run command getkeys bitcount k 1; array [k]
  run command getkeys bitfield k SET i8 "#0" 100; array [k]
  run command getkeys bitop AND k1 k2 k3; array [k1 k2 k3]
  run command getkeys bitpos k 1; array [k]
  run command getkeys blmove k1 k2 left right 0; array [k1 k2]
  run command getkeys blmpop 1 1 k1 left count 5; array [k1]
  run command getkeys blmpop 1 2 k1 k2 left count 5; array [k1 k2]
  run command getkeys blmpop 1 3 k1 k2 k3 left count 5; array [k1 k2 k3]
  run command getkeys blpop k1 k2 0; array [k1 k2]
  run command getkeys brpop k1 0; array [k1]
  run command getkeys brpop k1 k2 0; array [k1 k2]
  run command getkeys brpop k1 k2 k3 0; array [k1 k2 k3]
  run command getkeys brpoplpush k1 k2 0; array [k1 k2]
  run command getkeys client id; err "The command has no key arguments"
  run command getkeys command getkeys get a; err "The command has no key arguments"
  run command getkeys dbsize; err "The command has no key arguments"
  run command getkeys decr k; array [k]
  run command getkeys decrby k 1; array [k]
  run command getkeys del k1 k2 k3; array [k1 k2 k3]
  run command getkeys discard; err "The command has no key arguments"
  run command getkeys echo foo; err "The command has no key arguments"
  run command getkeys exec; err "The command has no key arguments"
  run command getkeys exists k1 k2; array [k1 k2]
  run command getkeys expire k 100; array [k]
  run command getkeys expireat k 100; array [k]
  run command getkeys flushall; err "The command has no key arguments"
  run command getkeys flushdb; err "The command has no key arguments"
  run command getkeys get k; array [k]
  run command getkeys getdel k; array [k]
  run command getkeys getex k; array [k]
  run command getkeys getbit k 100; array [k]
  run command getkeys getrange k 7 12; array [k]
  run command getkeys getset k v; array [k]
  run command getkeys hdel k f1 f2; array [k]
  run command getkeys hello; err "The command has no key arguments"
  run command getkeys hexists k f; array [k]
  run command getkeys hget k f; array [k]
  run command getkeys hgetall k; array [k]
  run command getkeys hincrby k f 1; array [k]
  run command getkeys hincrbyfloat k f 1.5; array [k]
  run command getkeys hkeys k; array [k]
  run command getkeys hlen k; array [k]
  run command getkeys hmget k f1 f2; array [k]
  run command getkeys hset k f1 v1 f2 v2; array [k]
  run command getkeys hsetnx k f v; array [k]
  run command getkeys hmset k f1 v1 f2 v2; array [k]
  run command getkeys hstrlen k f; array [k]
  run command getkeys hvals k; array [k]
  run command getkeys incr k; array [k]
  run command getkeys incrby k 1; array [k]
  run command getkeys incrbyfloat k 1.5; array [k]
  run command getkeys keys *; err "The command has no key arguments"
  run command getkeys lindex k i; array [k]
  run command getkeys linsert k before p e; array [k]
  run command getkeys llen k; array [k]
  run command getkeys lmove k1 k2 left right; array [k1 k2]
  run command getkeys lmpop 2 k1 k2 left; array [k1 k2]
  run command getkeys lpop k 3; array [k]
  run command getkeys lpush k e1 e2; array [k]
  run command getkeys lpushx k e1 e2; array [k]
  run command getkeys lrange k 5 10; array [k]
  run command getkeys lrem k 5 e; array [k]
  run command getkeys lset k 5 e; array [k]
  run command getkeys ltrim k 5 10; array [k]
  run command getkeys mget k1 k2; array [k1 k2]
  run command getkeys monitor; err "The command has no key arguments"
  run command getkeys move k db; array [k]
  run command getkeys mset k1 v1 k2 v2; array [k1 k2]
  run command getkeys msetnx k1 v1 k2 v2; array [k1 k2]
  run command getkeys multi; err "The command has no key arguments"
  run command getkeys persist k; array [k]
  run command getkeys pexpire k 5000; array [k]
  run command getkeys pexpireat k 5000; array [k]
  run command getkeys ping; err "The command has no key arguments"
  run command getkeys psetex k 150 v; array [k]
  run command getkeys psubscribe foo.*; err "The command has no key arguments"
  run command getkeys pttl k; array [k]
  run command getkeys publish foo bar; err "The command has no key arguments"
  run command getkeys pubsub numpat; err "The command has no key arguments"
  run command getkeys punsubscribe foo.*; err "The command has no key arguments"
  run command getkeys quit; err "The command has no key arguments"
  run command getkeys rpop k 1; array [k]
  run command getkeys rpoplpush k1 k2; array [k1 k2]
  run command getkeys rpush k e1 e2; array [k]
  run command getkeys rpushx k e1 e2; array [k]
  run command getkeys sadd k m1 m2; array [k]
  run command getkeys scard k; array [k]
  run command getkeys select 1; err "The command has no key arguments"
  run command getkeys set k v nx; array [k]
  run command getkeys setbit k 5 v; array [k]
  run command getkeys setex k 5 v; array [k]
  run command getkeys setnx k v; array [k]
  run command getkeys setrange k 5 v; array [k]
  run command getkeys sismember k m; array [k]
  run command getkeys smembers k; array [k]
  run command getkeys spop k 5; array [k]
  run command getkeys srem k m1 m2; array [k]
  run command getkeys strlen k; array [k]
  run command getkeys subscribe foo; err "The command has no key arguments"
  run command getkeys swapdb 1 2; err "The command has no key arguments"
  run command getkeys ttl k; array [k]
  run command getkeys type k; array [k]
  run command getkeys unlink k1 k2; array [k1 k2]
  run command getkeys unsubscribe foo; err "The command has no key arguments"
  run command getkeys unwatch; err "The command has no key arguments"
  run command getkeys watch k1 k2; array [k1 k2]
  run command getkeys zadd k1 s m; array [k1]
  run command getkeys zcard k1; array [k1]
  run command getkeys zrangebyscore k1 min max; array [k1]
  run command getkeys zrank k1 member; array [k1]
  run command getkeys zrem k1 k2 k3; array [k1 k2 k3]
}

test "monitor: getkeys" {
  run monitor; ok
  run get x; err "ERR Replica can't interact with the keyspace"
  run set x 1; err "ERR Replica can't interact with the keyspace"
  run ping; str PONG
}

test "monitor" {
  run monitor; ok
  client 2 {
    run set x 1; ok
    run get x; str 1
  }
  assert (read-value | str ends-with '"set" "x" "1"')
  assert (read-value | str ends-with '"get" "x"')
  run quit; ok
  assert (client closed 1)
}

test "monitor: reset" {
  run monitor; ok
  client 2 {
    run set x 1; ok
    run get x; str 1
  }
  assert (read-value | str ends-with '"set" "x" "1"')
  assert (read-value | str ends-with '"get" "x"')
  run reset; str RESET
  run get x; str 1
}

test "monitor: no read commands" {
  run monitor; ok
  client 2 {
    run command getkeys set x 1
    run command getkeys get x
  }
  assert (read-value | str ends-with '"command" "getkeys" "set" "x" "1"')
  assert (read-value | str ends-with '"command" "getkeys" "get" "x"')
}
