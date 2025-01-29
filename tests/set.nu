use bradis *
use std/assert

test "convert after set-max-*-entries" {
  run config set set-max-intset-entries 2; ok
  run config set set-max-listpack-entries 3; ok
  run sadd s 1 2; int 2
  run object encoding s; str intset
  run sadd s 3; int 1
  run object encoding s; str listpack
  run sadd s 4; int 1
  run object encoding s; str hashtable
}

test "convert after set-max-listpack-value" {
  run config set set-max-listpack-value 5; ok
  run sadd s xxxxx; int 1
  run object encoding s; str listpack
  run sadd s xxxxxx; int 1
  run object encoding s; str hashtable
}

test "convert to listpack for strings" {
  run sadd s 1 2; int 2
  run object encoding s; str intset
  run sadd s x; int 1
  run object encoding s; str listpack
}

test "sadd: wrong arguments" {
  run sadd 2; err "ERR wrong number of arguments for 'sadd' command"
}

test "sadd: intset" {
  discard hello 3
  run sadd s 1 2; int 2
  run sadd s 1 2 3; int 1
  run object encoding s; str intset
  run smembers s; set ["1" "2" "3"]
}

test "sadd: listpack" {
  discard hello 3
  run config set set-max-intset-entries 0; ok
  run sadd s 1 2; int 2
  run sadd s 1 2 3; int 1
  run object encoding s; str listpack
  run smembers s; set ["1" "2" "3"]
}

test "sadd: hashtable" {
  discard hello 3
  run config set set-max-intset-entries 0; ok
  run config set set-max-listpack-entries 0; ok
  run sadd s 1 2; int 2
  run sadd s 1 2 3; int 1
  run object encoding s; str hashtable
  run smembers s; set ["1" "2" "3"]
}

test "sadd: convert to intset" {
  discard hello 3
  run sadd x 4; int 1

  run sadd x $"($I8MAX + 1)"; int 1
  run object encoding x; str intset

  run sadd x $"($I16MAX + 1)"; int 1
  run object encoding x; str intset

  run sadd x $"($I32MAX + 1)"; int 1
  run object encoding x; str intset

  run smembers x; set ["4" $"($I8MAX + 1)" $"($I16MAX + 1)" $"($I32MAX + 1)"]
}

test "sadd: dirty" {
  dirty 2 { run sadd s 1 2; int 2 }
  dirty 0 { run sadd s 1 2; int 0 }
}

test "sadd: touch watched keys" {
  touch s { run sadd s 1; int 1 }
}

test "sadd: do not touch when not added" {
  run sadd s 1 2; int 2
  notouch s { run sadd s 1 2; int 0 }
}

test "scard: wrong arguments" {
  run scard; err "ERR wrong number of arguments for 'scard' command"
}

test "scard: intset" {
  run sadd s 1 2 3; int 3
  run object encoding s; str intset
  run scard s; int 3
}

test "scard: listpack" {
  run sadd s a b c; int 3
  run object encoding s; str listpack
  run scard s; int 3
}

test "scard: hashtable" {
  run config set set-max-listpack-entries 0; ok
  run sadd s a b c; int 3
  run object encoding s; str hashtable
  run scard s; int 3
}

test "sismember: wrong arguments" {
  run sismember 2; err "ERR wrong number of arguments for 'sismember' command"
  run sismember 2 3 4; err "ERR wrong number of arguments for 'sismember' command"
}

test "sismember: intset" {
  run sadd s 1 2 3; int 3
  run object encoding s; str intset
  run scard s; int 3
  run sismember s 1; int 1
  run sismember s 4; int 0
}

test "sismember: listpack" {
  run sadd s a b c; int 3
  run object encoding s; str listpack
  run scard s; int 3
  run sismember s a; int 1
  run sismember s d; int 0
}

test "sismember: hashtable" {
  run config set set-max-listpack-entries 0; ok
  run sadd s a b c; int 3
  run object encoding s; str hashtable
  run scard s; int 3
  run sismember s a; int 1
  run sismember s d; int 0
}

test "smembers: wrong arguments" {
  run smembers; err "ERR wrong number of arguments for 'smembers' command"
  run smembers 2 3; err "ERR wrong number of arguments for 'smembers' command"
}

test "smembers: intset" {
  discard hello 3
  run smembers s; set []
  run sadd s 1 2 3; int 3
  run object encoding s; str intset
  run smembers s; set ["1" "2" "3"]
}

test "smembers: listpack" {
  discard hello 3
  run smembers s; set []
  run sadd s a b c; int 3
  run object encoding s; str listpack
  run smembers s; set ["a" "b" "c"]
}

test "smembers: hashtable" {
  discard hello 3
  run config set set-max-listpack-entries 0; ok
  run smembers s; set []
  run sadd s a b c; int 3
  run object encoding s; str hashtable
  run smembers s; set ["a" "b" "c"]
}

test "smismember: wrong arguments" {
  run smismember; err "ERR wrong number of arguments for 'smismember' command"
  run smismember 2; err "ERR wrong number of arguments for 'smismember' command"
}

test "smismember: intset" {
  discard hello 3
  run smismember s 1 2 3; array [0 0 0]
  run sadd s 1 2 3; int 3
  run object encoding s; str intset
  run smismember s 3 4 5 6 7; array [1 0 0 0 0]
  run smismember s 5 4 3 2 1; array [0 0 1 1 1]
}

test "smismember: listpack" {
  discard hello 3
  run smismember s a b c; array [0 0 0]
  run sadd s a b c; int 3
  run object encoding s; str listpack
  run smismember s c d e f g; array [1 0 0 0 0]
  run smismember s e d c b a; array [0 0 1 1 1]
}

test "smismember: listpack" {
  discard hello 3
  run config set set-max-listpack-entries 0; ok
  run smismember s a b c; array [0 0 0]
  run sadd s a b c; int 3
  run object encoding s; str hashtable
  run smismember s c d e f g; array [1 0 0 0 0]
  run smismember s e d c b a; array [0 0 1 1 1]
}

test "srem: wrong arguments" {
  run srem; err "ERR wrong number of arguments for 'srem' command"
  run srem 2; err "ERR wrong number of arguments for 'srem' command"
}

test "srem: intset" {
  discard hello 3
  run sadd s 1 2; int 2
  run object encoding s; str intset
  run srem s 2 3; int 1
  run scard s; int 1
  run smembers s; set ["1"]
}

test "srem: listpack" {
  discard hello 3
  run sadd s a b; int 2
  run object encoding s; str listpack
  run srem s b c; int 1
  run scard s; int 1
  run smembers s; set ["a"]
}

test "srem: hashtable" {
  discard hello 3
  run config set set-max-listpack-entries 0; ok
  run sadd s a b; int 2
  run object encoding s; str hashtable
  run srem s b c; int 1
  run scard s; int 1
  run smembers s; set ["a"]
}

test "srem: remove the set" {
  run sadd s 1 2; int 2
  run srem s 1 2; int 2
  run scard s; int 0
  run type s; str none
}

test "srem: touch watched keys" {
  run sadd x 1 2 3; int 3
  touch x { run srem x 1 3; int 2 }
}

test "srem: do not touch if not removed" {
  run sadd x 1 2 3; int 3
  notouch x { run srem x 4 5 6; int 0 }
}

test "srem: dirty" {
  run sadd s 1 2 3; int 3
  dirty 0 { run srem s 4 5 6; int 0 }
  dirty 1 { run srem s 1; int 1 }
  dirty 2 { run srem s 2 3; int 2 }
}

test "spop: zero" {
  run sadd s 1 2; int 2
  run spop s 0; array []
  run scard s; int 2
}

test "spop: wrong arguments" {
  run spop; err "ERR wrong number of arguments for 'spop' command"
  run spop 2 3 4; err "ERR syntax error"
}

test "spop: invalid count" {
  run sadd s 1 2 3; int 3
  run spop s invalid; err "ERR offset is out of range"
}

test "spop: touch watched keys" {
  run sadd s 1; int 1
  touch s { run spop s 1; array ["1"] }
}

test "spop: do not touch if not changed" {
  run sadd s 1 2 3; int 3
  notouch s { run spop s 0; array [] }
}

test "spop: count" {
  discard hello 3
  run sadd s 1 2; int 2
  run spop s 3
  let value = read-value
  assert ("1" in $value)
  assert ("2" in $value)
  run scard s; int 0
}

test "spop: no count" {
  run sadd s 1; int 1
  run spop s; str 1
  run scard s; int 0
}

test "spop: remove the set without count" {
  run sadd s 1; int 1
  run spop s; str 1
  run scard s; int 0
  run type s; str none
}

test "spop: remove the set with count" {
  run sadd s 1; int 1
  run spop s 1; array ["1"]
  run scard s; int 0
  run type s; str none
}

test "spop: dirty" {
  run sadd s 1; int 1
  dirty 1 { run spop s; str 1 }
}

test "spop: dirty with count" {
  run sadd s 1 2; int 2
  dirty 2 { discard spop s 3 }
}

test "spop: more" {
  discard hello 3
  run sadd s 1 2 3; int 3
  run spop s 5
  let value = read-value
  assert ("1" in $value)
  assert ("2" in $value)
  assert ("3" in $value)
}
