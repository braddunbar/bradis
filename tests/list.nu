use bradis *

const sizes = ["-2", "-1", "0", "2", "4"]

def multiple-sizes [name: string body: closure] {
  for size in $sizes {
    test $"($name) ($size)" {
      run config set list-max-listpack-size $size; ok
      do $body
    }
  }
}

test "list: wrong arguments" {
  run blmpop; err "ERR wrong number of arguments for 'blmpop' command"
  run blmpop 0; err "ERR wrong number of arguments for 'blmpop' command"
  run blmpop 0 1; err "ERR wrong number of arguments for 'blmpop' command"
  run blmpop 0 1 x; err "ERR wrong number of arguments for 'blmpop' command"
  run rpush 2; err "ERR wrong number of arguments for 'rpush' command"
  run rpushx 2; err "ERR wrong number of arguments for 'rpushx' command"
  run ltrim 2 3; err "ERR wrong number of arguments for 'ltrim' command"
  run ltrim 2 3 4 5; err "ERR wrong number of arguments for 'ltrim' command"
  run lrem 2 3; err "ERR wrong number of arguments for 'lrem' command"
  run lrem 2 3 4 5; err "ERR wrong number of arguments for 'lrem' command"
  run lset 2 3; err "ERR wrong number of arguments for 'lset' command"
  run lset 2 3 4 5; err "ERR wrong number of arguments for 'lset' command"
  run lrange 2 3; err "ERR wrong number of arguments for 'lrange' command"
  run lrange 2 3 4 5; err "ERR wrong number of arguments for 'lrange' command"
  run lpush 2; err "ERR wrong number of arguments for 'lpush' command"
  run lpushx 2; err "ERR wrong number of arguments for 'lpushx' command"
  run lmove 2 3 4; err "ERR wrong number of arguments for 'lmove' command"
  run lmove 2 3 4 5 6; err "ERR wrong number of arguments for 'lmove' command"
  run lmpop; err "ERR wrong number of arguments for 'lmpop' command"
  run lpop; err "ERR wrong number of arguments for 'lpop' command"
  run lpos; err "ERR wrong number of arguments for 'lpos' command"
  run lpos x; err "ERR wrong number of arguments for 'lpos' command"
  run llen; err "ERR wrong number of arguments for 'llen' command"
  run llen 2 3; err "ERR wrong number of arguments for 'llen' command"
  run lindex 2; err "ERR wrong number of arguments for 'lindex' command"
  run lindex 2 3 4; err "ERR wrong number of arguments for 'lindex' command"
  run linsert 2 3 4; err "ERR wrong number of arguments for 'linsert' command"
  run linsert 2 3 4 5 6; err "ERR wrong number of arguments for 'linsert' command"
  run rpop; err "ERR wrong number of arguments for 'rpop' command"
  run rpoplpush 2; err "ERR wrong number of arguments for 'rpoplpush' command"
  run rpoplpush 2 3 4; err "ERR wrong number of arguments for 'rpoplpush' command"
}

multiple-sizes "lpush/lpop/llen" {
  run lpush l x y; int 2
  run llen l; int 2
  run lpop l; str y
  run llen l; int 1
  run lpop l; str x
  run llen l; int 0
  run hset l x 1; int 1
}

multiple-sizes "lmpop" {
  run rpush l1 x y z; int 3
  run rpush l2 x y z; int 3
  run lmpop 2 l1 l2 left; array [l1 [x]]
}

multiple-sizes "lmpop: multiple counts" {
  run rpush l1 x y z; int 3
  run rpush l2 x y z; int 3
  run lmpop 2 l1 l2 left count 1 count 2; err "ERR syntax error"
}

multiple-sizes "lmpop: right" {
  run rpush l1 x y z; int 3
  run rpush l2 x y z; int 3
  run lmpop 2 l1 l2 right; array [l1 [z]]
}

multiple-sizes "lmpop: count" {
  run rpush l1 x y z; int 3
  run rpush l2 x y z; int 3
  run lmpop 2 l1 l2 left count 2; array [l1 [x y]]
}

multiple-sizes "lmpop: count right" {
  run rpush l1 x y z; int 3
  run rpush l2 x y z; int 3
  run lmpop 2 l1 l2 right count 2; array [l1 [z y]]
}

multiple-sizes "lmpop: wrongtype" {
  run set l1 x; ok
  run rpush l2 x y z; int 3
  run lmpop 2 l1 l2 right count 2; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

multiple-sizes "lmpop: numkeys zero" {
  run rpush l1 x y z; int 3
  run rpush l2 x y z; int 3
  run lmpop 2 l1 l2 right count 0; err "ERR count should be greater than 0"
}

multiple-sizes "lmpop: count zero" {
  run set l1 x; ok
  run rpush l2 x y z; int 3
  run lmpop 0 right count 2; err "ERR numkeys should be greater than 0"
}

multiple-sizes "lmpop: missing edge" {
  run rpush l1 x y z; int 3
  run rpush l2 x y z; int 3
  run lmpop 2 l1 l2; err "ERR syntax error"
}

multiple-sizes "blmpop: invalid" {
  run blmpop 0 0 l1 left; err "ERR numkeys should be greater than 0"
  run blmpop 0 2 l1 left; err "ERR syntax error"
  run blmpop "-2" 1 l1 left; err "ERR timeout is negative"
}

multiple-sizes "blmpop: exec" {
  run rpush l a b c d; int 4
  run multi; ok
  run blmpop 0 1 l left count 2; str QUEUED
  run blmpop 0 1 l left count 2; str QUEUED
  run blmpop 0 1 l left count 2; str QUEUED
  run exec; array [[l [a b]] [l [c d]] null]
  run type l; str none
}

multiple-sizes "blmpop: exec" {
  run blmpop 1 1 x left count 2

  client 2 {
    await-flag 1 b
    run rpush x a b c; int 3
  }

  array [x [a b]]
  run lrange x 0 "-1"; array [c]
}

multiple-sizes "blmpop: right" {
  run blmpop 1 1 x right count 2

  client 2 {
    await-flag 1 b
    run rpush x a b c; int 3
  }

  array [x [c b]]
  run lrange x 0 "-1"; array [a]
}

multiple-sizes "blmpop: left less than count" {
  run blmpop 1 1 x left count 5

  client 2 {
    await-flag 1 b
    run rpush x a b c; int 3
  }

  array [x [a b c]];
  run lrange x 0 "-1"; array []
  run type x; str none
}

multiple-sizes "blmpop: right multiple keys" {
  run blmpop 1 2 x y right count 2

  client 2 {
    await-flag 1 b
    run rpush y a b c; int 3
  }

  array [y [c b]]
  run lrange y 0 "-1"; array [a]
}

multiple-sizes "blmpop: timeout" {
  run blmpop 0.001 1 x left count 5; nil
}

test "blmove: wrong arguments" {
  run blmove a b c d; err "ERR wrong number of arguments for 'blmove' command"
  run blmove a b c d e f; err "ERR wrong number of arguments for 'blmove' command"
}

test "brpoplpush wrong arguments" {
  run brpoplpush a b; err "ERR wrong number of arguments for 'brpoplpush' command"
  run brpoplpush a b c d; err "ERR wrong number of arguments for 'brpoplpush' command"
}

test "blmove: do not create destination" {
  run blmove source destination left right 0.001; nil
  run type source; str none
  run type destination; str none
}

multiple-sizes "blmove: left right" {
  run rpush destination x; int 1
  run blmove source destination left right 0

  client 2 {
    await-flag 1 b
    run rpush source a b c; int 3
  }

  str a
  run lrange source 0 "-1"; array [b c]
  run lrange destination 0 "-1"; array [x a]
}

multiple-sizes "blmove: right left" {
  run rpush destination x; int 1
  run blmove source destination right left 0

  client 2 {
    await-flag 1 b
    run rpush source a b c; int 3
  }

  str c
  run lrange source 0 "-1"; array [a b]
  run lrange destination 0 "-1"; array [c x]
}

multiple-sizes "brpoplpush" {
  run rpush destination x; int 1
  run brpoplpush source destination 0

  client 2 {
    await-flag 1 b
    run rpush source a b c; int 3
  }

  str c
  run lrange source 0 "-1"; array [a b]
  run lrange destination 0 "-1"; array [c x]
}

multiple-sizes "blmove: left left" {
  run rpush destination x; int 1
  run blmove source destination left left 0

  client 2 {
    await-flag 1 b
    run rpush source a b c; int 3
  }

  str a
  run lrange source 0 "-1"; array [b c]
  run lrange destination 0 "-1"; array [a x]
}

multiple-sizes "blmove: right right" {
  run rpush destination x; int 1
  run blmove source destination right right 0

  client 2 {
    await-flag 1 b
    run rpush source a b c; int 3
  }

  str c
  run lrange source 0 "-1"; array [a b]
  run lrange destination 0 "-1"; array [x c]
}

multiple-sizes "blmove: wrongtype after blocking" {
  run blmove source destination right right 0

  client 2 {
    await-flag 1 b
    run blmove source destination right right 0
  }

  client 3 {
    await-flag 2 b
    run blpop source 0
  }

  client 4 {
    await-flag 3 b
    run set destination x; ok
    run rpush source a b; int 2
  }

  client 1 { err "WRONGTYPE Operation against a key holding the wrong kind of value" }
  client 2 { err "WRONGTYPE Operation against a key holding the wrong kind of value" }
  client 3 { array [source a] }
  client 4 { run lrange source 0 "-1"; array [b] }
}

multiple-sizes "blmove: empty destination" {
  run blmove source destination right right 0

  client 2 {
    await-flag 1 b
    run rpush source a b c; int 3
  }

  str c
  run lrange source 0 "-1"; array [a b]
  run lrange destination 0 "-1"; array [c]
}

multiple-sizes "blmove: remove key" {
  run rpush source x; int 1
  run blmove source destination left right 0; str x
  run type source; str none
}

multiple-sizes "blmove: wrongtype destination waiting" {
  run set destination x; ok
  run blmove source destination left right 0

  client 2 {
    await-flag 1 b
    run rpush source a b c; int 3
  }

  err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

multiple-sizes "blmove: wrongtype destination immediate" {
  run rpush source a b c; int 3
  run set destination x; ok
  run blmove source destination left right 0; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

multiple-sizes "blmove: wrongtype source" {
  run set source x; ok
  run blmove source destination left right 0; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

multiple-sizes "blmove: after blpop" {
  client 1 { run blpop source 0 }

  client 2 {
    await-flag 1 b
    run blmove source destination left right 0
  }

  client 3 {
    await-flag 2 b
    run rpush source a b
  }

  client 1 { array [source a] }
  client 2 { str b }
}

multiple-sizes "blpop: touch" {
  run blpop x 0

  client 2 {
    await-flag 1 b
    touch x { run rpush x a; int 1 }
  }

  array [x a]
}

multiple-sizes "blmpop: touch" {
  run blmpop 0 1 x left count 2

  client 2 {
    await-flag 1 b
    touch x { run rpush x a b c; int 3 }
  }

  array [x [a b]]
  run lrange x 0 "-1"; array [c]
}

multiple-sizes "blmove: touch source" {
  run blmove x y left right 0

  client 2 {
    await-flag 1 b
    touch x { run rpush x a b c; int 3 }
  }

  str a
  run lrange x 0 "-1"; array [b c]
}

multiple-sizes "blmove: touch destination" {
  run blmove x y left right 0

  client 2 {
    await-flag 1 b
    touch y { run rpush x a b c; int 3 }
  }

  str a
  run lrange x 0 "-1"; array [b c]
}

multiple-sizes "blmove: empty after blpop" {
  run blpop source 0

  client 2 {
    await-flag 1 b
    run blmove source destination left right 0
  }

  client 3 {
    await-flag 2 b
    run rpush source a; int 1
    run type destination; str none
  }

  array [source a]

  client 3 {
    noflag 1 b
    flag 2 b
    run rpush source b; int 1
  }

  client 2 { str b }
}

multiple-sizes "lmove: missing source" {
  run lmove source destination left right; nil
  run multi; ok
  run blmove source destination left right 0; str QUEUED
  run exec; array [null]
}

multiple-sizes "lmove: wrongtype source" {
  run set source x; ok
  run lmove source destination left right; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run multi; ok
  run blmove source destination left right 0; str QUEUED
  run exec; array [{type: error, value: "WRONGTYPE Operation against a key holding the wrong kind of value"}]
}

multiple-sizes "lmove: wrongtype destination queued" {
  run set destination x; ok
  run lmove source destination left right; nil
  run multi; ok
  run blmove source destination left right 0; str QUEUED
  run exec; array [null]
}

multiple-sizes "lmove: left right" {
  run rpush source a b c; int 3
  run rpush destination x y z; int 3
  run lmove source destination left right; str a
  run lrange source 0 "-1"; array [b c]
  run lrange destination 0 "-1"; array [x y z a]
}

multiple-sizes "lmove: remove empty" {
  run rpush source a; int 1
  run lmove source destination left right; str a
  run type source; str none
  run lrange destination 0 "-1"; array [a]
}

multiple-sizes "lmove: same key, left right" {
  run rpush x a b c; int 3
  run lmove x x left right; str a
  run lrange x 0 "-1"; array [b c a]
}

multiple-sizes "lmove: same key, right left" {
  run rpush x a b c; int 3
  run lmove x x right left; str c
  run lrange x 0 "-1"; array [c a b]
}

multiple-sizes "lmove: same key, left left" {
  run rpush x a b c; int 3
  run lmove x x left left; str a
  run lrange x 0 "-1"; array [a b c]
}

multiple-sizes "lmove: same key, right right" {
  run rpush x a b c; int 3
  run lmove x x right right; str c
  run lrange x 0 "-1"; array [a b c]
}

multiple-sizes "blmove: wrong destination type" {
  run blmove x y left right 0

  client 2 {
    await-flag 1 b
    run set y a; ok
    run rpush x b; int 1
  }

  err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run get y; str a
  run lrange x 0 "-1"; array [b]
}

multiple-sizes "blmove: multi left right" {
  run rpush source a b c; int 3
  run rpush destination x y z; int 3
  run multi; ok
  run blmove source destination left right 0; str QUEUED
  run exec; array [a]
  run lrange source 0 "-1"; array [b c]
  run lrange destination 0 "-1"; array [x y z a]
}

multiple-sizes "lmove: right left" {
  run rpush source a b c; int 3
  run rpush destination x y z; int 3
  run lmove source destination right left; str c
  run lrange source 0 "-1"; array [a b]
  run lrange destination 0 "-1"; array [c x y z]
}

multiple-sizes "blmove: multi right left" {
  run rpush source a b c; int 3
  run rpush destination x y z; int 3
  run multi; ok
  run blmove source destination right left 0; str QUEUED
  run exec; array [c]
  run lrange source 0 "-1"; array [a b]
  run lrange destination 0 "-1"; array [c x y z]
}

multiple-sizes "lmove: left left" {
  run rpush source a b c; int 3
  run rpush destination x y z; int 3
  run lmove source destination left left; str a
  run lrange source 0 "-1"; array [b c]
  run lrange destination 0 "-1"; array [a x y z]
}

multiple-sizes "lmove: right right" {
  run rpush source a b c; int 3
  run rpush destination x y z; int 3
  run lmove source destination right right; str c
  run lrange source 0 "-1"; array [a b]
  run lrange destination 0 "-1"; array [x y z c]
}

multiple-sizes "blmove: multi right right" {
  run rpush source a b c; int 3
  run rpush destination x y z; int 3
  run multi; ok
  run blmove source destination right right 0; str QUEUED
  run exec; array [c]
  run lrange source 0 "-1"; array [a b]
  run lrange destination 0 "-1"; array [x y z c]
}

multiple-sizes "lpush: touch watched keys" {
  touch x { run lpush x 1; int 1 }
}

multiple-sizes "lpop: touch watched keys" {
  run lpush x 1; int 1
  touch x { run lpop x; str 1 }
}

multiple-sizes "lpop: count" {
  run rpush x 1 2 3 4; int 4
  run lpop x 3; array ["1" "2" "3"];
  run lpop x 3; array ["4"]
  run lpop x 3; nil
}

multiple-sizes "lpop: invalid count" {
  run rpush x 1 2 3 4; int 4
  run lpop x invalid; err "ERR value is not an integer or out of range"
}

multiple-sizes "lpop: zero count" {
  run lpop x 0; nil
  run rpush x 1 2 3 4; int 4
  run lpop x 0; array []
}

multiple-sizes "rpop: zero count" {
  run rpush x 1 2 3 4; int 4
  run rpop x 3; array ["4" "3" "2"]
  run rpop x 3; array ["1"]
}

multiple-sizes "rpop: invalid count" {
  run rpush x 1 2 3 4; int 4
  run rpop x invalid; err "ERR value is not an integer or out of range"
}

multiple-sizes "rpop: invalid count" {
  run rpush x 1 2 3 4; int 4
  run rpop x 0; array []
}

multiple-sizes "lpop: does not touch empty keys" {
  notouch x { run lpop x; nil }
}

multiple-sizes "lpushx: touch watched keys" {
  run lpush x 1; int 1
  touch x { run lpushx x 2; int 2 }
}

multiple-sizes "lpushx" {
  run lpushx l x; int 0
  run lpush l x; int 1
  run lpushx l x; int 2
  run lrange l 0 "-1"; array [x x]
}

multiple-sizes "rpushx" {
  run rpushx l x; int 0
  run rpush l x; int 1
  run rpushx l x; int 2
  run lrange l 0 "-1"; array [x x]
}

multiple-sizes "rpushx: touch watched keys" {
  run lpush x 1 2 3; int 3
  touch x { run rpushx x 4; int 4 }
}

multiple-sizes "linsert: touch watched keys" {
  run lpush x 1 2 3; int 3
  touch x { run linsert x before 2 4; int 4 }
}

multiple-sizes "linsert: missing" {
  run linsert list before pivot x; int 0
  run linsert list after pivot x; int 0
  run linsert list invalid pivot x; err "ERR syntax error"
  run type list; str none
}

multiple-sizes "linsert: pivot not found" {
  run rpush list x y; int 2
  run linsert list before a b; int -1
  run lrange list 0 "-1"; array [x y]
}

multiple-sizes "linsert: before" {
  run rpush list x y; int 2
  run linsert list before y a; int 3
  run lrange list 0 "-1"; array [x a y]
}

multiple-sizes "linsert: before first" {
  run rpush list x y; int 2
  run linsert list before x a; int 3
  run lrange list 0 "-1"; array [a x y]
}

multiple-sizes "linsert: after" {
  run rpush list x y; int 2
  run linsert list after x a; int 3
  run lrange list 0 "-1"; array [x a y]
}

multiple-sizes "linsert: after last" {
  run rpush list x y; int 2
  run linsert list after y a; int 3
  run lrange list 0 "-1"; array [x y a]
}

test "linsert: pack full" {
  run config set list-max-listpack-size 3; ok
  run rpush list a b c; int 3
  run object encoding list; str listpack
  run linsert list after c d; int 4
  run object encoding list; str quicklist
  run lrange list 0 "-1"; array [a b c d]
}

multiple-sizes "lset: lindex" {
  run lpush l x y; int 2
  run lindex l 2; nil
  run lindex l 1; str x
  run lset l 1 z; ok
  run lset l 5 z; err "ERR index out of range"
  run lindex l 1; str z
  run lpop l; str y
  run lpop l; str z
}

multiple-sizes "lset: more" {
  run rpush l 0 1 2 3 4 5 6 7; int 8
  run lset l 1 11; ok
  run lset l 5 55; ok
  run lset l 3 33; ok
  run lset l 7 77; ok
  run lrange l 0 "-1"; array ["0" "11" "2" "33" "4" "55" "6" "77"]
}

multiple-sizes "lset: last element" {
  run lpush x 1; int 1
  run lset x 0 2; ok
  run lrange x 0 "-1"; array ["2"]
}

multiple-sizes "lset: touch watched keys" {
  run lpush x 1; int 1
  touch x { run lset x 0 2; ok }
}

multiple-sizes "lset: lindex negative" {
  run lpush l x y; int 2
  run lindex l "-1"; str x
  run lindex l "-2"; str y
  run lindex l "-3"; nil
}

multiple-sizes "lset: empty" {
  run lset l 5 z; err "ERR no such key"
}

multiple-sizes "lindex: empty" {
  run lindex l 0; nil
}

multiple-sizes "llen: empty" {
  run llen l; int 0
}

multiple-sizes "lrange" {
  run lpush l x y z; int 3
  run lrange empty 0 1; array []
  run lrange l 0 3; array [z y x]
  run lrange l 0 "-1"; array [z y x]
  run lrange l 0 "-2"; array [z y]
  run lrange l 1 "-2"; array [y]
  run lrange l 2 "-2"; array []
}

multiple-sizes "rpush" {
  run rpush l x y z; int 3
  run lrange l 0 "-1"; array [x y z]
}

multiple-sizes "rpush: touch watched keys" {
  run lpush x 1 2 3; int 3
  touch x { run rpush x 4; int 4 }
}

multiple-sizes "rpop: x" {
  run rpop empty; nil
  run rpush l x y z; int 3
  run rpop l; str z
  run lrange l 0 "-1"; array [x y]
}

multiple-sizes "rpop: touch watched keys" {
  run lpush x 1 2 3; int 3
  touch x { run rpop x; str 1 }
}

multiple-sizes "rpoplpush" {
  run rpush source x y; int 2
  run rpoplpush source destination; str y
  run lrange source 0 "-1"; array [x]
  run lrange destination 0 "-1"; array [y]
}

multiple-sizes "rpoplpush: empty nil" {
  run rpoplpush a b; nil
}

multiple-sizes "rpoplpush: touch watched source" {
  run lpush x 1 2 3; int 3
  touch x { run rpoplpush x y; str 1 }
}

multiple-sizes "rpoplpush: touch watched destination" {
  run lpush x 1 2 3; int 3
  touch y { run rpoplpush x y; str 1 }
}

multiple-sizes "rpoplpush: empty" {
  run rpoplpush source destination; nil
}

multiple-sizes "rpoplpush: existing destination" {
  run rpush source x y; int 2
  run rpush destination a; int 1
  run rpoplpush source destination; str y
  run lrange source 0 "-1"; array [x]
  run lrange destination 0 "-1"; array [y a]
}

multiple-sizes "rpop: remove" {
  run rpush l x; int 1
  run rpop l; str x
  run type l; str none
}

multiple-sizes "lpop: remove" {
  run lpush l x; int 1
  run lpop l; str x
  run type l; str none
}

multiple-sizes "lmpop: remove" {
  run lpush l x; int 1
  run lmpop 1 l left; array [l [x]]
  run type l; str none
}

multiple-sizes "ltrim" {
  run rpush l a b c x y z; int 6
  run ltrim l 1 "-2"; ok
  run lrange l 0 "-1"; array [b c x y]
}

test "ltrim: convert" {
  run config set list-max-listpack-size 8; ok
  run rpush l a b c d e f g h i; int 9
  run object encoding l; str quicklist
  run ltrim l 0 8; ok
  run object encoding l; str quicklist
  run ltrim l 0 6; ok
  run object encoding l; str quicklist
  run ltrim l 0 4; ok
  run object encoding l; str quicklist
  run ltrim l 0 3; ok
  run object encoding l; str listpack
}

multiple-sizes "ltrim: touch watched keys" {
  run lpush x 1 2 3; int 3
  touch x { run ltrim x 0 1; ok }
}

multiple-sizes "ltrim: do not touch without change" {
  run lpush x 1 2 3; int 3
  notouch x { run ltrim x 0 5; ok }
}

multiple-sizes "ltrim: empty" {
  run ltrim empty 1 "-2"; ok
  run type empty; str none
}

multiple-sizes "lrem: positive" {
  run rpush l a a b c a a; int 6
  run lrem l 3 a; int 3
  run lrange l 0 "-1"; array [b c a]
}

multiple-sizes "lrem: touch watched keys" {
  run lpush x 1 2 3; int 3
  touch x { run lrem x 1 1; int 1 }
}

multiple-sizes "lrem: do not touch when nothing is removed" {
  run lpush x 1 2 3; int 3
  run watch x; ok
  run lrem x 1 4; int 0
  run multi; ok
  run lindex x 1; str QUEUED
  run exec; array ["2"]
}

multiple-sizes "lrem: negative" {
  run rpush l a a b c a a; int 6
  run lrem l "-3" a; int 3
  run lrange l 0 "-1"; array [a b c]
}

multiple-sizes "lrem: zero" {
  run rpush l a a b c a a; int 6
  run lrem l 0 a; int 4
  run lrange l 0 "-1"; array [b c]
}

multiple-sizes "lrem: empty" {
  run lrem empty "-3" a; int 0
}

multiple-sizes "blpop: multi" {
  run lpush l x y z; int 3
  run multi; ok
  run blpop a b l 10; str QUEUED
  run blpop a b l 10; str QUEUED
  run blpop a b l 10; str QUEUED
  run exec; array [[l z] [l y] [l x]]
  run type l; str none
}

multiple-sizes "brpop: multi" {
  run lpush l x y z; int 3
  run multi; ok
  run brpop a b l 10; str QUEUED
  run brpop a b l 10; str QUEUED
  run brpop a b l 10; str QUEUED
  run exec; array [[l x] [l y] [l z]]
  run type l; str none
}

multiple-sizes "blpop: touch watched keys" {
  run lpush x 1; int 1
  run watch x; ok

  client 2 {
    run multi; ok
    run blpop x 0; str QUEUED
    run exec; array [[x "1"]]
  }

  run multi; ok
  run lpush x 2; str QUEUED
  run exec; nil
}

multiple-sizes "brpop: touch watched keys" {
  run lpush x 1; int 1
  run watch x; ok

  client 2 {
    run multi; ok
    run brpop x 0; str QUEUED
    run exec; array [[x "1"]]
  }

  run multi; ok
  run lpush x 2; str QUEUED
  run exec; nil
}

multiple-sizes "blpop: nil" {
  run multi; ok
  run blpop l 0; str QUEUED
  run exec; array [null]
}

multiple-sizes "blpop: multi wrongtype" {
  run set l x; ok
  run lpush m x; int 1
  run multi; ok
  run blpop l m 0; str QUEUED
  run exec; [{type: error value: "WRONGTYPE Operation against a key holding the wrong kind of value"}];
}

multiple-sizes "blpop: wrong arguments" {
  run blpop x; err "ERR wrong number of arguments for 'blpop' command"
}

multiple-sizes "brpop: wrong arguments" {
  run brpop x; err "ERR wrong number of arguments for 'brpop' command"
}

multiple-sizes "blpop: timeout" {
  run set x 1; ok
  run blpop l "0.01"; nil
  run get x; str 1
  run get y; nil
}

multiple-sizes "brpop: timeout" {
  run set x 1; ok
  run brpop l "0.01"; nil
  run get x; str 1
  run get y; nil
}

multiple-sizes "blpop: existing item" {
  run rpush x a b c; int 3
  run blpop x "0.5"; array [x a]
}

multiple-sizes "brpop: existing item" {
  run rpush x a b c; int 3
  run brpop x "0.5"; array [x c]
}

multiple-sizes "blpop: invalid timeout" {
  run blpop x invalid; err "ERR timeout is not a float or out of range"
  run blpop x "-1"; err "ERR timeout is negative"
  run blpop x "inf"; err "ERR timeout is not finite"
}

multiple-sizes "blpop: lpush single" {
  run blpop x 1

  client 2 {
    await-flag 1 b
    run lpush x 1; int 1
  }

  array [x "1"]
  run type x; str none
}

multiple-sizes "brpop: lpush multiple" {
  run brpop x 1

  client 2 {
    await-flag 1 b
    run lpush x 1 2 3; int 3
  }

  array [x "1"]
}

multiple-sizes "blpop: lpush multiple" {
  run blpop x 1

  client 2 {
    await-flag 1 b
    run lpush x 1 2 3; int 3
  }

  array [x "3"]
}

multiple-sizes "blpop: multiple keys" {
  run blpop x y 1

  client 2 {
    await-flag 1 b
    run lpush x 1 2 3; int 3
  }

  array [x "3"]
}

multiple-sizes "blmove: trigger blpop" {
  run blmove a b left right 0

  client 2 {
    await-flag 1 b
    run blpop b 0
  }

  client 3 {
    await-flag 2 b
    run rpush a 1 2 3; int 3
  }

  str 1
  client 2 { array [b "1"] }
  run lrange a 0 "-1"; array ["2" "3"]
}

multiple-sizes "lpos" {
  run lpos missing x foo; err "ERR syntax error"
  run lpos missing x maxlen invalid; err "ERR value is not an integer or out of range"
  run lpos missing x count invalid; err "ERR value is not an integer or out of range"
  run lpos missing x rank invalid; err "ERR value is not an integer or out of range"
  run lpos missing x; nil
  run lpos missing x count 0; array []
  run rpush l a b c a b c a b c a b c; int 12;
  run lpos l a; int 0
  run lpos l b; int 1
  run lpos l c; int 2
  run lpos l missing; nil
  run lpos l a rank 1; int 0
  run lpos l a rank 2; int 3
  run lpos l a rank 3; int 6
  run lpos l a rank 4; int 9
  run lpos l a rank 5; nil
  run lpos l a rank "-1"; int 9
  run lpos l a rank "-2"; int 6
  run lpos l a rank "-3"; int 3
  run lpos l a rank "-4"; int 0
  run lpos l a rank "-5"; nil
  run lpos l a count 0; array [0, 3, 6, 9]
  run lpos l a count 0 rank 1; array [0, 3, 6, 9]
  run lpos l a count 0 rank 2; array [3, 6, 9]
  run lpos l a count 0 rank 3; array [6, 9]
  run lpos l a count 0 rank 4; array [9]
  run lpos l a count 0 rank 5; array []
  run lpos l a count 0 rank "-1"; array [9, 6, 3, 0]
  run lpos l a count 0 rank "-2"; array [6, 3, 0]
  run lpos l a count 0 rank "-3"; array [3, 0]
  run lpos l a count 0 rank "-4"; array [0]
  run lpos l a count 0 rank "-5"; array []
  run lpos l a count 1; array [0]
  run lpos l a count 2; array [0, 3]
  run lpos l a count 3; array [0, 3, 6]
  run lpos l a count 4; array [0, 3, 6, 9]
  run lpos l a count 5; array [0, 3, 6, 9]
  run lpos l a count 1 rank 2; array [3]
  run lpos l a count 2 rank 2; array [3, 6]
  run lpos l a count 3 rank 2; array [3, 6, 9]
  run lpos l a count 4 rank 2; array [3, 6, 9]
  run lpos l a count 1 rank "-2"; array [6]
  run lpos l a count 2 rank "-2"; array [6, 3]
  run lpos l a count 3 rank "-2"; array [6, 3, 0]
  run lpos l a count 4 rank "-2"; array [6, 3, 0]
  run lpos l a maxlen 1; int 0
  run lpos l a maxlen 1 rank 2; nil
  run lpos l a maxlen 2 rank 2; nil
  run lpos l a maxlen 3 rank 2; nil
  run lpos l a maxlen 4 rank 2; int 3
  run lpos l a maxlen 2 rank "-1"; nil
  run lpos l a maxlen 3 rank "-1"; int 9
}

test "wrongtype" {
  run set a x; ok
  run llen a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run lindex a 1; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run lpop a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run lpos a x; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run lpush a x; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run lrange a 0 "-1"; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run lset a 0 x; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run rpop a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run rpush a x; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run ltrim a 0 "-1"; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run lpushx a 0; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run rpoplpush a b; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "feed blockers after multi" {
  run blpop key 0

  client 2 {
    run multi; ok
    run rpush key a b c; str QUEUED
    run lpop key; str QUEUED
    run exec; array [3 a]
  }

  array [key b]
  run PING; str PONG
  run lrange key 0 "-1"; [c]
}
