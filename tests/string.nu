use bradis *

test "set something" {
  run set x foo; ok
}

test "get/set" {
  run get x; nil
  run set x foo; ok
  run get x; str foo
}

test "wrong arguments" {
  run set x; err "ERR wrong number of arguments for 'set' command"
}

test "getdel" {
  run getdel x; nil
  run set x foo; ok
  run getdel x; str foo
  run get x; nil
}

test "getex wrongtype" {
  run rpush x foo; int 1
  run getex x
  err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "append encoding" {
  discard hello 3
  run set a b; ok
  run append a c; int 2
  run get a; str bc
  run append b c; int 1
  run get b; str c

  # Array
  run set a x; ok
  run object encoding a; str embstr
  run append a y; int 2
  run get a; str xy
  run object encoding a; str embstr

  # Raw
  run set a "xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx"; ok
  run object encoding a; str raw
  run append a y; int 55
  run get a; str "xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxxy"
  run object encoding a; str raw

  # Int
  run set a 12345; ok
  run object encoding a; str int
  run append a 6; int 6
  run get a; str 123456
  run object encoding a; str int
  run append a x; int 7
  run get a; str 123456x
  run object encoding a; str embstr

  # Float
  run del a; int 1
  run incrbyfloat a 1.2; float 1.2
  run object encoding a; str float
  run append a y; int 4
  run get a; str 1.2y
  run object encoding a; str embstr
}

test "append: dirty" {
  dirty 1 { run append a b; int 1 }
}

test "incr: dirty" {
  dirty 1 { run incr x; int 1 }
}

test "incrby: dirty" {
  dirty 1 { run incrby x 1; int 1 }
}

test "incrbyflat: dirty" {
  discard hello 3
  dirty 1 { run incrbyfloat x 1; float 1.0 }
}

test "decr: dirty" {
  dirty 1 { run decr x; int -1 }
}

test "decrby: dirty" {
  dirty 1 { run decrby x 1; int -1 }
}

test "getdel: dirty" {
  dirty 1 { run set x 1; ok }
  dirty 1 { run getdel x; str 1 }
}

test "getdel: only delete strings" {
  dirty 1 { run sadd s 1; int 1 }
  dirty 0 { run getdel s; err "WRONGTYPE Operation against a key holding the wrong kind of value" }
  run scard s; int 1
}

test "getex: dirty" {
  dirty 1 { run set x 1; ok }
  dirty 0 { run getex x; str 1 }
  dirty 1 { run getex x ex 10; str 1 }
}

test "getdel: dirty missing" {
  dirty 0 { run getdel x; nil }
}

test "getset: dirty" {
  dirty 1 { run getset a b; nil }
  dirty 1 { run getset a c; str b }
}

test "mset: dirty" {
  dirty 2 { run mset a 1 b 2; ok }
}

test "msetnx: dirty" {
  dirty 1 { run set x 1; ok }
  dirty 0 { run msetnx x 1 y 2; int 0 }
  dirty 2 { run msetnx a 1 b 2; int 1 }
}

test "psetex: dirty" {
  dirty 1 { run psetex a 22000 b; ok }
}

test "setex: dirty" {
  dirty 1 { run setex a 22 b; ok }
}

test "setnx: dirty" {
  dirty 1 { run setnx a 1; int 1 }
  dirty 0 { run setnx a 2; int 0 }
}

test "setrange: dirty" {
  dirty 1 { run setrange a 1 test; int 5 }
}

test "append: max len" {
  run append a foo; int 3
  run config set proto-max-bulk-len 10; ok

  # Append to existing
  run append a xxxxxxxx
  err "ERR string exceeds maximum allowed size (proto-max-bulk-len)"

  # Append to empty with inline command
  run-inline "append b xxxxxxxxxxx"
  err "ERR string exceeds maximum allowed size (proto-max-bulk-len)"

  # Append to empty
  run append b xxxxxxxxxxx
  err "ERR Protocol Error: invalid blob length"
}

test "append: empty" {
  run append missing ""; int 0
  run strlen missing; int 0
  run type missing; str string
}

test "set: empty" {
  run set x ""; ok
  run strlen x; int 0
  run type x; str string
}

test "append: touch watched keys" {
  touch x { run append x 1; int 1 }
}

test "get value" {
  run get a; nil
  run get b; nil
}

test "getrange" {
  run set a abcdefghi; ok
  run getrange a 3 7; str defgh
}

test "getrange: empty range" {
  run set a abcdefghi; ok
  run getrange a 2 1; str ""
}

test "getrange: missing key" {
  run getrange a 2 1; str ""
}

test "getset" {
  run getset a b; nil
  run getset a c; str b
  run get a; str c
}

test "getset: touch watched keys" {
  touch x { run getset x 1; nil }
}

test "getset: remove expiration" {
  run set a x; ok
  run expire a 10; int 1
  run getset a y; str x
  run ttl a; int -1
}

test "set" {
  run set a b; ok
  run get a; str b
}

test "set: dirty" {
  dirty 1 { run set a b; ok }
}

test "set: touch watched keys" {
  touch x { run set x 1; ok }
}

test "set: hash" {
  run hset a b c; int 1
  run set a b; ok
  run get a; str b
}

test "set: nx" {
  run set a 1 xx nx; err "ERR syntax error"
  run set a 1 nx nx; ok
  run set a 2 nx; nil
  run get a; str 1
}

test "set: xx" {
  run set a 1 nx xx; err "ERR syntax error"
  run set a 1 xx xx; nil
  run get a; nil
  run set a 2; ok
  run set a 1 xx; ok
  run get a; str 1
}

test "set: ex" {
  run set a 1 ex 10; ok
  run get a; str 1
  ttl a 10
}

test "set: exat" {
  let t = ((date now) + 10sec | into int) // 10 ** 9
  run set a 1 exat ($t | into string); ok
  run get a; str 1
  run expiretime a; int $t
}

test "set: px" {
  run set a 1 px 20000; ok
  run get a; str 1
  ttl a 20
}

test "set: pxat" {
  let t = ((date now) + 10sec | into int) // 10 ** 6
  run set a 1 pxat ($t | into string); ok
  run get a; str 1
  run pexpiretime a; int $t
}

test "set: keepttl" {
  run set a 1 px 20000; ok
  run set a 2 keepttl; ok
  run get a; str 2
  ttl a 20
}

test "set: remove expiration" {
  run set a x; ok
  run expire a 10; int 1
  run set a y; ok
  run ttl a; int -1
}

test "set: get wrongtype" {
  run rpush x 1; int 1
  run set x 2 get; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run lrange x 0 "-1"; array ["1"]
}

test "set: get wrongtype" {
  run rpush x 1; int 1
  run set x 2 get; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run lrange x 0 "-1"; array ["1"]
}

test "set: get nil" {
  run set x 2 get; nil
  run get x; str 2
}

test "set: get previous" {
  run set x 1; ok
  run set x 2 get; str 1
  run get x; str 2
}

test "set: invalid" {
  let s = ((date now) + 10sec | into int) // 10 ** 9 | into string
  let ms = ((date now) + 10sec | into int) // 10 ** 6 | into string

  # EX
  run set x 2 exat $s ex 5; err "ERR syntax error"
  run set x 2 keepttl ex 5; err "ERR syntax error"
  run set x 2 px 5000 ex 5; err "ERR syntax error"
  run set x 2 pxat $ms ex 5; err "ERR syntax error"

  # EXAT
  run set x 2 ex 5 exat $s; err "ERR syntax error"
  run set x 2 keepttl exat $s; err "ERR syntax error"
  run set x 2 px 1000 exat $s; err "ERR syntax error"
  run set x 2 pxat $ms exat $s; err "ERR syntax error"

  # KEEPTTL
  run set x 2 ex 5 keepttl; err "ERR syntax error"
  run set x 2 exat $s keepttl; err "ERR syntax error"
  run set x 2 px 1000 keepttl; err "ERR syntax error"
  run set x 2 pxat $ms keepttl; err "ERR syntax error"

  # PX
  run set x 2 ex 5 px 5000; err "ERR syntax error"
  run set x 2 exat $s px 5000; err "ERR syntax error"
  run set x 2 keepttl px 6000; err "ERR syntax error"
  run set x 2 pxat $ms px 6000; err "ERR syntax error"

  # PXAT
  run set x 2 ex 5 pxat $ms; err "ERR syntax error"
  run set x 2 exat $s pxat $ms; err "ERR syntax error"
  run set x 2 keepttl pxat $ms; err "ERR syntax error"
  run set x 2 px 6000 pxat $ms; err "ERR syntax error"
}

test "strlen: wrong arguments" {
  run strlen; err "ERR wrong number of arguments for 'strlen' command"
  run strlen 2 3; err "ERR wrong number of arguments for 'strlen' command"
}

test "setrange: wrong arguments" {
  run setrange 2 3; err "ERR wrong number of arguments for 'setrange' command"
  run setrange 2 3 4 5; err "ERR wrong number of arguments for 'setrange' command"
}

test "setex: wrong arguments" {
  run setex 2 3; err "ERR wrong number of arguments for 'setex' command"
  run setex 2 3 4 5; err "ERR wrong number of arguments for 'setex' command"
}

test "setnx: wrong arguments" {
  run setnx 2; err "ERR wrong number of arguments for 'setnx' command"
  run setnx 2 3 4; err "ERR wrong number of arguments for 'setnx' command"
}

test "set: wrong arguments" {
  run set 2; err "ERR wrong number of arguments for 'set' command"
}

test "mget: wrong arguments" {
  run mget; err "ERR wrong number of arguments for 'mget' command"
}

test "mset: wrong arguments" {
  run mset 2; err "ERR wrong number of arguments for 'mset' command"
  run mset 2 3 4; err "ERR wrong number of arguments for 'mset' command"
}

test "msetnx: wrong arguments" {
  run msetnx 2; err "ERR wrong number of arguments for 'msetnx' command"
}

test "append: wrong arguments" {
  run append; err "ERR wrong number of arguments for 'append' command"
  run append 2; err "ERR wrong number of arguments for 'append' command"
  run append 2 3 4; err "ERR wrong number of arguments for 'append' command"
}

test "decr: wrong arguments" {
  run decr; err "ERR wrong number of arguments for 'decr' command"
  run decr 2 3; err "ERR wrong number of arguments for 'decr' command"
}

test "decrby: wrong arguments" {
  run decrby 2; err "ERR wrong number of arguments for 'decrby' command"
  run decrby 2 3 4; err "ERR wrong number of arguments for 'decrby' command"
}

test "incr: wrong arguments" {
  run incr; err "ERR wrong number of arguments for 'incr' command"
  run incr 2 3; err "ERR wrong number of arguments for 'incr' command"
}

test "incrby: wrong arguments" {
  run incrby 2; err "ERR wrong number of arguments for 'incrby' command"
  run incrby 2 3 4; err "ERR wrong number of arguments for 'incrby' command"
}

test "incrbyfloat: wrong arguments" {
  run incrbyfloat 2; err "ERR wrong number of arguments for 'incrbyfloat' command"
  run incrbyfloat 2 3 4 5; err "ERR wrong number of arguments for 'incrbyfloat' command"
}

test "getrange: wrong arguments" {
  run getrange 2 3; err "ERR wrong number of arguments for 'getrange' command"
}

test "getset: wrong arguments" {
  run getset 2; err "ERR wrong number of arguments for 'getset' command"
}

test "psetex: wrong arguments" {
  run psetex 2 3; err "ERR wrong number of arguments for 'psetex' command"
  run psetex 2 3 4 5; err "ERR wrong number of arguments for 'psetex' command"
}

test "get: wrong arguments" {
  run get; err "ERR wrong number of arguments for 'get' command"
  run get 2 3; err "ERR wrong number of arguments for 'get' command"
}

test "getex: wrong arguments" {
  run getex; err "ERR wrong number of arguments for 'getex' command"
}

test "getdel: wrong arguments" {
  run getdel; err "ERR wrong number of arguments for 'getdel' command"
  run getdel 2 3; err "ERR wrong number of arguments for 'getdel' command"
}

test "setex" {
  run setex a 12 b; ok
  run get a; str b
  ttl a 12
}

test "setex: touch watched keys" {
  touch x { run setex x 12 1; ok }
}

test "psetex" {
  run psetex a 22000 b; ok
  run get a; str b
  ttl a 22
}

test "psetex: touch watched keys" {
  touch x { run psetex x 12000 1; ok }
}

test "psetex: hash" {
  run hset a b c; int 1
  run psetex a 22000 b; ok
  run get a; str b
  ttl a 22
}

test "setnx" {
  run setnx a 1; int 1
  run setnx a 2; int 0
  run get a; str 1
}

test "setnx: hash" {
  run hset a b c; int 1
  run setnx a 1; int 0
  run hget a b; str c
}

test "setnx: touch watched keys" {
  touch x { run setnx x 1; int 1 }
}

test "strlen" {
  run set a abcde; ok
  run strlen a; int 5
}

test "strlen: int encoding" {
  run set a 12345; ok
  run object encoding a; str int
  run strlen a; int 5
}

test "strlen: empty" {
  run strlen a; int 0
}

test "setrange" {
  discard hello 3

  # Array
  run set a abcdefghi; ok
  run setrange a 1 test; int 9
  run get a; str atestfghi
  run object encoding a; str embstr

  # Integer
  run set a 123456789; ok
  run object encoding a; str int
  run setrange a 2 test; int 9
  run get a; str 12test789
  run object encoding a; str embstr
  run setrange a 2 3456; int 9
  run get a; str 123456789
  run object encoding a; str embstr

  # Float
  run set a 123456789; ok
  run setrange a 3 test; int 9
  run get a; str 123test89

  # Raw
  run set a 123456789123456789123456789; ok
  run setrange a 4 test; int 27
  run get a; str 1234test9123456789123456789
}

test "setrange: check len" {
  let max = 512 * 1024 * 1024
  run setrange a ($max + 1 | into string) test; err "ERR string exceeds maximum allowed size (proto-max-bulk-len)"
  run config set proto-max-bulk-len "1kb"; ok
  run setrange a 1025 test; err "ERR string exceeds maximum allowed size (proto-max-bulk-len)"
}

test "setrange: touch watched keys" {
  touch x { run setrange x 1 test; int 5 }
}

test "setrange: keep expiration" {
  run set a 123456789; ok
  run expire a 10; int 1
  run setrange a 1 test; int 9
  run get a; str 1test6789
  ttl a 10
}

test "setrange: slightly longer" {
  run set a 123; ok
  run setrange a 1 test; int 5
  run get a; str 1test
}

test "setrange: offset out of range" {
  run setrange a "-1" test; err "ERR offset is out of range"
}

test "setrange: expired" {
  let s = ((date now) - 10sec | into int) // 10 ** 9
  run set a 123456789; ok
  run expireat a ($s | into string); int 1
  run setrange a 1 test; int 5
  run get a; str "\u{0}test"
}

test "setrange: missing" {
  run setrange a 3 test; int 7
  run get a; str "\u{0}\u{0}\u{0}test"
}

test "incr" {
  run incr a; int 1
  run incr a; int 2
  run incr a; int 3
  run get a; str 3
}

test "incr: append" {
  run incr a; int 1
  run incr a; int 2
  run incr a; int 3
  run append a 0; int 2
  run get a; str 30
  run incr a; int 31
}

test "incr: touch watched keys" {
  touch x { run incr x; int 1 }
}

test "incr: string " {
  run set a x; ok
  run incr a; err "ERR value is not an integer or out of range"
}

test "incr: only exact numbers" {
  for x in ["01", "-01", "00", " 1", "1 ", "- 1 "] {
    run set a $x; ok
    run incr a; err "ERR value is not an integer or out of range"
  }
}

test "incr: overflow" {
  run set a ($I64MAX | into string); ok
  run incr a; err "ERR increment or decrement would overflow"
}

test "incrby" {
  run incrby a 10; int 10
  run incrby a 20; int 30
  run get a; str 30
}

test "incrby: touch watched keys" {
  touch x { run incrby x 1; int 1 }
}

test "incrbyfloat" {
  discard hello 3
  run incrbyfloat a 10.5; float 10.5
  run incrbyfloat a 12.5; float 23.0
  run set a x; ok
  run incrbyfloat a 1.2; err "ERR value is not a valid float"
  run incrbyfloat b "inf"; err "ERR increment would produce NaN or Infinity"
  run incrbyfloat b "-inf"; err "ERR increment would produce NaN or Infinity"
  run incrbyfloat b "nan"; err "ERR increment would produce NaN or Infinity"
}

test "incrbyfloat: touch watched keys" {
  discard hello 3
  touch x { run incrbyfloat x 1.5; float 1.5 }
}

test "decrby" {
  run decrby a 10; int -10
  run decrby a 20; int -30
  run get a; str "-30"
}

test "decrby: negative overflow" {
  run set x ($I64MIN | into string); ok
  run decrby x 1; err "ERR increment or decrement would overflow"
}

test "decrby: touch watched keys" {
  touch x { run decrby x 1; int -1 }
}

test "decr" {
  run decr a; int -1
  run decr a; int -2
  run decr a; int -3
  run get a; str "-3"
}

test "decr: touch watched keys" {
  touch x { run decr x; int -1 }
}

test "decr: overflow" {
  run set a ($I64MIN | into string); ok
  run decr a; err "ERR increment or decrement would overflow"
}

test "mget" {
  run set a a; ok
  run set b 2; ok
  run mget a b c; array ["a", "2", null]
}

test "mget: wrongtype" {
  run hset a b c; int 1
  run set b 2; ok
  run mget a b c; array [null, "2", null]
}

test "mset" {
  run mset a b c 1; ok
  run get a; str b
  run get c; str 1
}

test "mset: check arguments before setting" {
  run mset a 1 b; err "ERR wrong number of arguments for 'mset' command"
  run get a; nil
  run get b; nil
}

test "msetnx: check arguments before setting" {
  run msetnx a 1 b; err "ERR wrong number of arguments for 'msetnx' command"
  run get a; nil
  run get b; nil
}

test "mset: touch watched keys" {
  touch x { run mset x 1; ok }
}

test "mset: hash" {
  run hset a b c; int 1
  run mset a b c 1; ok
  run get a; str b
  run get c; str 1
}

test "mset: remove expiration" {
  run set a x; ok
  run expire a 100; int 1
  run mset a y; ok
  run ttl a; int -1
}

test "msetnx" {
  run msetnx a b c 1; int 1
  run mget a b c; array ["b", null, "1"]
}

test "msetnx: touch watched keys" {
  touch x { run msetnx x 1; int 1 }
}

test "msetnx: hash" {
  run hset a b c; int 1
  run msetnx a b c 1; int 0
  run hget a b; str c
  run get c; nil
}

test "msetnx: existing" {
  run set a x; ok
  run msetnx a b c 1; int 0
  run mget a b c; array ["x", null, null]
}

test "getdel" {
  run getdel x; nil
  run set x 1; ok
  run getdel x; str 1
  run get x; nil
}

test "getdel: touch watched keys" {
  run set x 1; ok
  touch x { run getdel x; str 1 }
}

test "getex" {
  run set x 1; ok
  run getex x; str 1
  run ttl x; int -1
}

test "getex: persist" {
  run set x 1 ex 10; ok
  run getex x persist; str 1
  run ttl x; int -1
}

test "getex: ex" {
  run set x 1; ok
  run getex x ex 10; str 1
  ttl x 10
}

test "getex: exat" {
  let t = ((date now) + 10sec | into int) // 10 ** 9
  run set x 1; ok
  run getex x exat ($t | into string); str 1
  run expiretime x; int $t
}

test "getex: px" {
  run set x 1; ok
  run getex x px 10000; str 1
  ttl x 10
}

test "getex: pxat" {
  let t = ((date now) + 10sec | into int) // 10 ** 6
  run set x 1; ok
  run getex x pxat ($t | into string); str 1
  run pexpiretime x; int $t
}

test "getex: nil" {
  run getex x; nil
}

test "getex: wrongtype" {
  run rpush x 1; int 1
  run getex x; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "getex: delete" {
  run set x 1; ok
  run getex x ex "-10"; str 1
  run get x; nil

  run set x 1; ok
  run getex x px "-10000"; str 1
  run get x; nil
}

test "getex: invalid" {
  let s = ((date now) + 10sec | into int) // 10 ** 9 | into string
  let ms = ((date now) + 10sec | into int) // 10 ** 6 | into string

  # EX
  run getex x ex 10 exat $s; err "ERR syntax error"
  run getex x ex 10 persist; err "ERR syntax error"
  run getex x ex 10 px 10000; err "ERR syntax error"
  run getex x ex 10 pxat $ms; err "ERR syntax error"

  # EXAT
  run getex x exat $s ex 10; err "ERR syntax error"
  run getex x exat $s persist; err "ERR syntax error"
  run getex x exat $s px 1000; err "ERR syntax error"
  run getex x exat $s pxat $ms; err "ERR syntax error"

  # PERSIST
  run getex x persist ex 10; err "ERR syntax error"
  run getex x persist exat $s; err "ERR syntax error"
  run getex x persist px 1000; err "ERR syntax error"
  run getex x persist pxat $ms; err "ERR syntax error"

  # PX
  run getex x px 1000 ex 10; err "ERR syntax error"
  run getex x px 1000 exat $s; err "ERR syntax error"
  run getex x px 1000 persist; err "ERR syntax error"
  run getex x px 1000 pxat $ms; err "ERR syntax error"

  # PXAT
  run getex x pxat $ms ex 10; err "ERR syntax error"
  run getex x pxat $ms exat $s; err "ERR syntax error"
  run getex x pxat $ms persist; err "ERR syntax error"
  run getex x pxat $ms px 1000; err "ERR syntax error"

  # Overflow
  let max = $U128MAX | into string
  run getex x ex $max; err "ERR invalid expire time in getex command"
  run getex x px $max; err "ERR invalid expire time in getex command"
}

test "append: wrongtype" {
  run hset a b c; int 1
  run append a b; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "decr: wrongtype" {
  run hset a b c; int 1
  run decr a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "decrby: wrongtype" {
  run hset a b c; int 1
  run decrby a 1; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "incr: wrongtype" {
  run hset a b c; int 1
  run incr a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "incrby: wrongtype" {
  run hset a b c; int 1
  run incrby a 1; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "incrbyfloat: wrongtype" {
  run hset a b c; int 1
  run incrbyfloat a 1; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "get: wrongtype" {
  run hset a b c; int 1
  run get a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "getrange: wrongtype" {
  run hset a b c; int 1
  run getrange a 1 4; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "getset: wrongtype" {
  run hset a b c; int 1
  run getset a 1; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "strlen: wrongtype" {
  run hset a b c; int 1
  run strlen a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "encode integers and embedded strings" {
  run setnx x x; int 1
  run object encoding x; str embstr

  run set x 1; ok
  run object encoding x; str int

  run set x "-1"; ok
  run object encoding x; str int

  run set x x; ok
  run object encoding x; str embstr

  run setex x 100 x; ok
  run object encoding x; str embstr

  run psetex x 100000 x; ok
  run object encoding x; str embstr

  run getset x x; str x
  run object encoding x; str embstr

  run mset x x; ok
  run object encoding x; str embstr

  run msetnx y x; int 1
  run object encoding y; str embstr
}
