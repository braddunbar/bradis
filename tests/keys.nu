use bradis *
use std/assert

test "keys: wrong arguments" {
  run del; err "ERR wrong number of arguments for 'del' command"
  run unlink; err "ERR wrong number of arguments for 'unlink' command"
  run exists; err "ERR wrong number of arguments for 'exists' command"
  run keys; err "ERR wrong number of arguments for 'keys' command"
  run keys 2 3; err "ERR wrong number of arguments for 'keys' command"
  run type; err "ERR wrong number of arguments for 'type' command"
  run type 2 3; err "ERR wrong number of arguments for 'type' command"
  run object; err "ERR wrong number of arguments for 'object' command"
  run object help invalid; err "ERR Unknown subcommand or wrong number of arguments for 'help'. Try OBJECT HELP."
}

test "del" {
  run set a b; ok
  run del a; int 1
  run del a; int 0
  run get a; nil
}

test "exists" {
  run set a 1; ok
  run set b 2; ok
  run exists a; int 1
  run exists a b; int 2
  run exists a b b; int 3
  run exists a b b c; int 3
}

test "unlink" {
  run set a b; ok
  run unlink a; int 1
  run unlink a; int 0
  run get a; nil
}

test "del: touch watched keys" {
  run set x 1; ok
  touch x { run del x; int 1 }
}

test "del: multiple" {
  run set a 1; ok
  run set b 2; ok
  run del a b c; int 2
  run get a; nil
  run get b; nil
}

test "del: dirty" {
  dirty 2 {
    run set a 1; ok
    run set b 2; ok
  }
  dirty 2 {
    run del a b c; int 2
  }
}

test "keys" {
  run mset a 1 abc 2; ok
  run expire a 0; int 1
  run keys *; array ["abc"]
  run keys abc; array ["abc"]
  run keys a*c; array ["abc"]
  run keys a*; array ["abc"]
  run keys "a[bc][^d]"; array ["abc"]
}

test "type" {
  run set a x; ok
  run type a; str string
  run hset b x 1; int 1
  run type b; str hash
  run type x; str none
  run lpush l x; int 1
  run type l; str list
  run sadd s 1; int 1
  run type s; str set
}

test "unwatch" {
  run watch x; ok
  run set x 1; ok
  run unwatch; ok
  run multi; ok
  run get x; str QUEUED
  run exec; array ["1"]
}

test "unwatch after exec" {
  run watch x; ok
  run append x 1; int 1
  run multi; ok
  run get x; str QUEUED
  run exec; nil
  run append x 1; int 2
  run multi; ok
  run get x; str QUEUED
  run exec; array ["11"]
}

test "unwatch after discard" {
  run watch x; ok
  run append x 1; int 1
  run multi; ok
  run discard; ok
  run append x 1; int 2
  run multi; ok
  run get x; str QUEUED
  run exec; array ["11"]
}

test "unwatch after successful exec" {
  run watch x; ok
  run multi; ok
  run get x; str QUEUED
  run exec; array [null]
  run set x 1; ok
  run multi; ok
  run get x; str QUEUED
  run exec; array ["1"]
}

test "watch in multi" {
  run multi; ok
  run watch x; err "ERR WATCH inside MULTI is not allowed"
  run set x y; str QUEUED
  run get x; str QUEUED
  run exec; array ["OK", "y"]
}

test "object help" {
  run object help
  assert str contains (read-string) OBJECT
}

test "object encoding" {
  run set s xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx; ok
  run object encoding s; str raw

  run set s xxxxxxxxxx; ok
  run object encoding s; str embstr

  run incr i; int 1
  run object encoding i; str int

  run incrbyfloat f 1.5; str 1.5
  run object encoding f; str float

  run rpush l x; int 1
  run object encoding l; str listpack

  run config set list-max-listpack-size 2; ok
  run rpush l a b c d; int 5
  run object encoding l; str quicklist

  run hset h x 1; int 1
  run object encoding h; str listpack

  run config set hash-max-listpack-entries 2; ok
  run hset h y 2 z 3; int 2
  run object encoding h; str hashtable

  run sadd set 1; int 1
  run object encoding set; str intset

  run sadd set x; int 1
  run object encoding set; str listpack

  run config set set-max-listpack-entries 2; ok
  run sadd set y; int 1
  run object encoding set; str hashtable

  run zadd z 1 a; int 1
  run object encoding z; str listpack

  run config set zset-max-listpack-entries 2; ok
  run zadd z 2 b 3 c; int 2
  run object encoding z; str skiplist

  discard hello 3
  run incrbyfloat g 1.5; float 1.5
  run object encoding f; str float
}
