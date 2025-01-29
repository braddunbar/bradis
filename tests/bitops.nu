use bradis *

test "getbit: wrong arguments" {
  run getbit 2; err "ERR wrong number of arguments for 'getbit' command"
}

test "setbit: wrong arguments" {
  run setbit 2 3; err "ERR wrong number of arguments for 'setbit' command"
  run setbit 2 3 4 5; err "ERR wrong number of arguments for 'setbit' command"
}

test "bitcount" {
  run bitcount a; int 0
  run set a ac; ok
  run bitcount a; int 7
  run bitcount a 1 1; int 4
  run bitcount a 0 0; int 3
}

test "bitcount: first byte" {
  let b = 0x[ff]
  run set a $b; ok
  run bitcount a 0 0 byte; int 8
}

test "bitcount: bit" {
  run setbit x 2 1; int 0
  run setbit x 7 1; int 0
  run setbit x 14 1; int 0
  run setbit x 20 1; int 0

  # All bits
  run bitcount x; int 4
  run bitcount x 0 "-1"; int 4
  run bitcount x 0 "-1" bit; int 4

  # Exclude bits from first byte
  run bitcount x 2 "-1" bit; int 4
  run bitcount x 3 "-1" bit; int 3

  # Exclude bits from last byte
  run bitcount x 3 20 bit; int 3
  run bitcount x 3 19 bit; int 2

  # Just one byte
  run bitcount x 2 7 bit; int 2
  run bitcount x 3 7 bit; int 1

  # Just two bytes
  run bitcount x 3 14 bit; int 2
  run bitcount x 3 13 bit; int 1
  run bitcount x 11 21 bit; int 2
}

test "bitcount: one byte" {
  run setbit x 2 1; int 0
  run setbit x 6 1; int 0
  run setbit x 7 1; int 0

  run bitcount x 2 5 bit; int 1
  run bitcount x 3 5 bit; int 0
  run bitcount x 3 6 bit; int 1
}

test "bitcount: large value" {
  run setbit x 200 1; int 0
  run setbit x 300 1; int 0
  run setbit x 1000 1; int 0
  run setbit x 2000 1; int 0
  run setbit x 2001 1; int 0
  run setbit x 5000 1; int 0
  run setbit x 10000 1; int 0

  run bitcount x 200 20000 bit; int 7
  run bitcount x 200 5000 bit; int 6
  run bitcount x 300 2000 bit; int 3
}

test "bitpos" {
  # Wrong arguments
  run bitpos a; err "ERR wrong number of arguments for 'bitpos' command"

  # Too many arguments
  run bitpos a 1 0 5 1; err "ERR syntax error"

  # If no 1 exists, return -1.
  run bitpos a 1; int -1

  # Without an explicit end, searching for 0 returns 0.
  run bitpos a 0; int 0

  # In an empty key, searching for 0 returns 0.
  run bitpos a 0 1 5; int 0
  run bitpos a 0 1 5 bit; int 0

  # Check for valid arguments.
  run bitpos a 2; err "ERR The bit argument must be 1 or 0."
  run bitpos a 0 invalid; err "ERR value is not an integer or out of range"
  run bitpos a 0 1 invalid; err "ERR value is not an integer or out of range"
  run bitpos a 0 invalid invalid; err "ERR value is not an integer or out of range"

  # Search for 1 when none exists.
  let b = 0x[00]
  run set a $b; ok
  run bitpos a 1; int -1

  # Search for 1 in the first byte.
  let b = 0x[01]
  run set a $b; ok
  run bitpos a 1; int 7

  # Search for 1 in the second byte.
  let b = 0x[0002]
  run set a $b; ok
  run bitpos a 1; int 14

  # Search for missing 0 in one byte.
  let b = 0x[ff]
  run set a $b; ok
  run bitpos a 0; int 8

  # Search for 0 in the second byte.
  let b = 0x[fffc]
  run set a $b; ok
  run bitpos a 0; int 14

  # Search for 0 in a range with an explicit end.
  let b = 0x[00ffff00]
  run set a $b; ok
  run bitpos a 0 1 2; int -1

  # Search for 0 in a range without an explicit end.
  let b = 0x[00ffff]
  run set a $b; ok
  run bitpos a 0 1; int 24

  # Search for 0 in a range without an explicit end.
  let b = 0x[00fffff0]
  run set a $b; ok
  run bitpos a 0 1; int 28

  # Search for 1 in a range with an explicit end.
  let b = 0x[ff0000000f]
  run set a $b; ok
  run bitpos a 1 1 "-2"; int -1

  # Search for 1 in a range without an explicit end.
  let b = 0x[ff00000f]
  run set a $b; ok
  run bitpos a 1 1; int 28
}

test "bitpos: bit set" {
  run setbit x 10 1; int 0
  run setbit x 12 1; int 0
  run setbit x 20 1; int 0
  run setbit x 25 1; int 0
  run setbit x 29 1; int 0

  # In the first byte
  run bitpos x 1 11 30 bit; int 12

  # Excluded from first byte
  run bitpos x 1 13 30 bit; int 20

  # Only one byte
  run bitpos x 1 26 28 bit; int -1

  # In the last byte
  run bitpos x 1 6 13 bit; int 10
}

test "bitpos: bit clear" {
  let b = 0x[ffffffff]
  run set x $b; ok
  run setbit x 10 0; int 1
  run setbit x 12 0; int 1
  run setbit x 20 0; int 1
  run setbit x 25 0; int 1
  run setbit x 29 0; int 1

  # In the first byte
  run bitpos x 0 11 30 bit; int 12

  # Excluded from first byte
  run bitpos x 0 13 30 bit; int 20

  # Only one byte
  run bitpos x 0 26 28 bit; int -1

  # In the last byte
  run bitpos x 0 6 13 bit; int 10
}

test "bitpos: first byte" {
  let b = 0x[ff]
  run set x $b; ok
  run setbit x 1 0; int 1
  run bitpos x 0 0 0 byte; int 1
}

test "getbit" {
  run set a ac; ok
  run getbit a 7; int 1
  run getbit a 6; int 0
  run getbit a 15; int 1
  run getbit a 14; int 1
  run getbit a 875; int 0
  run getbit a "-875"; err "ERR bit offset is not an integer or out of range"
  run get a; str ac
}

test "setbit" {
  run set a ac; ok
  run setbit a 6 1; int 0
  run setbit a 6 2; err "ERR The bit argument must be 1 or 0."
  run setbit a "-6" 1; err "ERR bit offset is not an integer or out of range"
  run get a; str cc
  run setbit a 6 0; int 1
  run get a; str ac
}

test "setbit: touch watched keys" {
  touch x { run setbit x 6 1; int 0 }
}

test "bitop: shorter keys are zero padded" {
  let a = 0x[0102ffff]
  let b = 0x[0102ff]

  run set a $a; ok
  run set b $b; ok

  let x = 0x[0102ff00]
  run bitop and x a b; int 4
  run get x; bin $x

  let x = 0x[0102ffff]
  run bitop or x a b; int 4
  run get x; bin $x

  let x = 0x[000000ff]
  run bitop xor x a b; int 4
  run get x; bin $x
}

test "bitop: missing key is considered a stream of zeros" {
  let a = 0x[0102ff]
  run set a $a; ok

  let x = 0x[000000]
  run bitop and x a b; int 3
  run get x; bin $x

  let x = 0x[0102ff]
  run bitop or x a b; int 3
  run get x; bin $x

  let x = 0x[0102ff]
  run bitop xor x a b; int 3
  run get x; bin $x
}

test "bitop: and" {
  run mset key1 foobar key2 abcdef; ok
  run bitop and dest key1 key2; int 6
  run get dest; str "`bc`ab"
}

test "bitop: invalid" {
  run bitop invalid dest a b; err "ERR syntax error"
  run bitop and dest; err "ERR wrong number of arguments for 'bitop' command"
  run bitop or dest; err "ERR wrong number of arguments for 'bitop' command"
  run bitop xor dest; err "ERR wrong number of arguments for 'bitop' command"
  run bitop not dest; err "ERR wrong number of arguments for 'bitop' command"
  run bitop not dest a b; err "ERR BITOP NOT must be called with a single source key."
}

test "bitop: and - touch watched keys" {
  run mset a 1 b 2 c 3; ok
  touch x { run bitop and x a b c; int 1 }
}

test "bitop: and - no touch when empty" {
  notouch x { run bitop and x a b c; int 0 }
}

test "bitop: and - dirty" {
  dirty 0 { run bitop and x a b c; int 0 }
  dirty 1 { run set a 1; ok }
  dirty 1 { run bitop and x a b c; int 1 }
}

test "bitop: or" {
  let a = 0x[0102]
  let b = 0x[0204]
  let x = 0x[0306]
  run mset a $a b $b; ok
  run bitop or x a b; int 2
  run get x; bin $x
}

test "bitop: or - touch watched keys" {
  run set a 1; ok
  touch x { run bitop or x a b c; int 1 }
}

test "bitop: or - no touch when empty" {
  notouch x { run bitop or x a b c; int 0 }
}

test "bitop: or - dirty" {
  dirty 0 { run bitop or x a b c; int 0 }
  dirty 1 { run set a 1; ok }
  dirty 1 { run bitop or x a b c; int 1 }
}

test "bitop: xor" {
  let a = 0x[0102]
  let b = 0x[0203]
  let x = 0x[0301]
  run mset a $a b $b; ok
  run bitop xor x a b; int 2
  run get x; bin $x
}

test "bitop: xor - touch watched keys" {
  run set a 1; ok
  touch x { run bitop xor x a b c; int 1 }
}

test "bitop: xor - no touch when empty" {
  notouch x { run bitop xor x a b c; int 0 }
}

test "bitop: xor - dirty" {
  dirty 0 { run bitop xor x a b c; int 0 }
  dirty 1 { run set a 1; ok }
  dirty 1 { run bitop xor x a b c; int 1 }
}

test "bitop: not" {
  let a = 0x[ff00f0]
  let x = 0x[00ff0f]
  run set a $a; ok
  run bitop not x a; int 3
  run get x; bin $x
}

test "bitop: not - large value" {
  let a = 0..<100 | reduce -f 0x[] {|_, b| bytes build $b 0x[00] }
  let x = 0..<100 | reduce -f 0x[] {|_, b| bytes build $b 0x[ff] }
  run set a $a; ok
  run bitop not x a; int 100
  run get x; bin $x
}

test "bitop: not - touch watched keys" {
  run set y 1; ok
  touch x { run bitop not x y; int 1 }
}

test "bitop: not - no touch when empty" {
  notouch x { run bitop not x y; int 0 }
}

test "bitop: not - nil" {
  dirty 1 { run set x 1; ok }
  dirty 1 { run bitop not x y; int 0 }
  run get x; nil
}

test "bitop: not - dirty" {
  dirty 0 { run bitop not x y; int 0 }
  dirty 1 { run set y 2; ok }
  dirty 1 { run bitop not x y; int 1 }
}

test "bitop: not - empty string" {
  run set y ""; ok
  dirty 0 { run bitop not x y; int 0 }
}

test "bitop: and - nil" {
  dirty 1 { run set x 1; ok }
  dirty 1 { run bitop and x y z; int 0 }
  run get x; nil
  dirty 0 { run bitop and x y z; int 0 }
  run get x; nil
}

test "bitfield: wrong arguments" {
  run bitfield; err "ERR wrong number of arguments for 'bitfield' command"
}

test "bitfield: get and overflow" {
  run bitfield x overflow wrap get i64 "#0"; array [0]
}

test "bitfield: ro" {
  run bitfield_ro x set i64 "#0" 1; err "ERR BITFIELD_RO only supports the GET subcommand"
  run bitfield_ro x incrby i64 "#0" 1; err "ERR BITFIELD_RO only supports the GET subcommand"
  run bitfield_ro x overflow wrap; err "ERR BITFIELD_RO only supports the GET subcommand"
  run bitfield_ro x get i64 "#0"; array [0]
  run bitfield x set i64 "#0" 1; array [0]
  run bitfield_ro x get i64 "#0"; array [1]
}

test "bitfield: 64 bit wrap" {
  run bitfield x set i64 0 $'($I64MIN)'; array [0]
  run bitfield x overflow wrap incrby i64 0 $'($I64MIN)'; array [0]
}

test "bitfield: dirty" {
  dirty 0 { run bitfield x get i64 "#1"; array [0] }
  dirty 1 { run bitfield x set i64 "#1" 1; array [0] }
  dirty 1 { run bitfield x set i64 "#1" 1 set i64 "#2" 2; array [1 0 ] }
  dirty 2 { run bitfield x set i64 "#1" 2 incrby i64 "#2" 2; array [1 4] }
}

test "bitfield: no ops" {
  run bitfield x; array []
}

test "bitfield: dirty created" {
  dirty 0 { run bitfield x get i64 "#1"; array [0] }
  run get x; nil
  dirty 2 { run bitfield x set i64 "#1" 0 incrby i64 "#2" 0; array [0 0] }
}

test "bitfield: get" {
  let a = 0x[2181]
  run set a $a; ok
  run bitfield a get u5 5; array [6]

  let a = 0x[002181]
  run set a $a; ok
  run bitfield a get u5 13; array [6]
  run bitfield a get u4 4; array [0]
  run bitfield a get u4 8; array [2]

  let a = 0x[032181]
  run set a $a; ok
  run bitfield a get u12 6; array [3206]
  run bitfield a get i12 6; array [-890]
}

test "bitfield: get i64" {
  let a = 0x[0000000000000001]
  run set a $a; ok
  run bitfield a get i64 0; array [1]

  let a = 0x[000000000000000080]
  run set a $a; ok
  run bitfield a get i64 1; array [1]
}

test "bitfield: get empty" {
  run bitfield a get u32 0; array [0]
}

test "bitfield: set" {
  let a = 0x[0440]
  run bitfield a set u5 5 17; array [0]
  run get a; bin $a
}

test "bitfield: set - touch watched keys" {
  touch x { run bitfield x set u5 5 17; array [0] }
}

test "bitfield: set aligned" {
  let a = 0x[0011]
  run bitfield a set u8 8 17; array [0]
  run get a; bin $a
}

test "bitfield: invalid offset" {
  run bitfield a get u8 "#x"; err "ERR bit offset is not an integer or out of range"
}

test "bitfield: set signed" {
  let a = 0x[3bc0]
  run bitfield a set i8 2 "-17"; array [0]
  run get a; bin $a
}

test "bitfield: set overflow" {
  let a = 0x[00]
  run bitfield a overflow fail set u3 2 256; array [null]
  run get a; bin $a
}

test "bitfield: incrby" {
  run bitfield a incrby i8 2 "-17"; array [-17]
  run bitfield a get i8 2; array [-17]
}

test "bitfield: incrby - touch watched keys" {
  touch x { run bitfield x incrby i8 2 "-17"; array [-17] }
}

test "bitfield: overflow invalid" {
  run bitfield a overflow foo incrby i3 0 1; err "ERR Invalid OVERFLOW type specified"
}

test "bitfield: overflow sat" {
  run bitfield a overflow sat set u3 0 9 get u3 0; array [0 7]
  run bitfield b overflow sat set u3 0 "-9" get u3 0; array [0 0]
  run bitfield c overflow sat set i3 0 9 get i3 0; array [0 3]
  run bitfield d overflow sat set i3 0 "-9" get i3 0; array [0 -4]
}

test "bitfield: overflow unsigned" {
  run bitfield a overflow fail set u3 0 7 incrby u3 0 1 overflow wrap incrby u3 0 1 get u3 0
  array [0 null 0 0]
  run bitfield a set u3 0 0 overflow fail incrby u3 0 "-1"
  array [0 null]
}

test "bitfield: overflow signed" {
  run bitfield a set i3 0 3; array [0]
  run bitfield a overflow fail incrby i3 0 1; array [null]
  run bitfield a set i3 0 "-4"; array [3]
  run bitfield a overflow fail incrby i3 0 "-1"; array [null]
}

test "bitcount: wrongtype" {
  run hset a b c; int 1
  run bitcount a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "bitpos: wrongtype" {
  run hset a b c; int 1
  run bitpos a 0; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "bitop: wrongtype" {
  run hset a b c; int 1
  run bitop and c a b; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run bitop or c a b; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run bitop xor c a b; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run bitop not c a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "getbit: wrongtype" {
  run hset a b c; int 1
  run getbit a 7; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "setbit: wrongtype" {
  run hset a b c; int 1
  run setbit a 6 1; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "setbit: dirty" {
  dirty 1 { run setbit x 15 1; int 0 }
}

test "setbit: dirty created" {
  dirty 1 { run setbit x 15 0; int 0 }
  dirty 0 { run setbit x 12 0; int 0 }
}
