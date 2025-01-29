use bradis *

const types = [[name value]; [hashtable "0"] [listpack "512"]]

def hashtable-and-listpack [name: string body: closure] {
  for t in $types {
    test $"($name) ($t.name)" {
      run config set hash-max-listpack-entries $t.value; ok
      do $body $t
    }
  }
}

test "wrong arguments" {
  run hdel x; err "ERR wrong number of arguments for 'hdel' command"
  run hexists x; err "ERR wrong number of arguments for 'hexists' command"
  run hget x; err "ERR wrong number of arguments for 'hget' command"
  run hgetall; err "ERR wrong number of arguments for 'hgetall' command"
  run hincrby a x; err "ERR wrong number of arguments for 'hincrby' command"
  run hincrbyfloat a x; err "ERR wrong number of arguments for 'hincrbyfloat' command"
  run hkeys; err "ERR wrong number of arguments for 'hkeys' command"
  run hlen; err "ERR wrong number of arguments for 'hlen' command"
  run hmget x; err "ERR wrong number of arguments for 'hmget' command"
  run hset x; err "ERR wrong number of arguments for 'hset' command"
  run hsetnx x; err "ERR wrong number of arguments for 'hsetnx' command"
  run hmset x; err "ERR wrong number of arguments for 'hmset' command"
  run hstrlen x y z; err "ERR wrong number of arguments for 'hstrlen' command"
  run hvals x y; err "ERR wrong number of arguments for 'hvals' command"
}

hashtable-and-listpack "hget/hset" {|t|
  run hset a b c d e; int 2
  run object encoding a; str $t.name
  run hget a b; str c
}

hashtable-and-listpack "hgetall" {|t|
  discard hello 3
  run hset a x 1; int 1
  run hset a y 2; int 1
  run object encoding a; str $t.name
  run hgetall a; map {x: "1" y: "2"}
}

hashtable-and-listpack "hset: multiple fields" {|t|
  run hset a x 1 y 2; int 2
  run hset a x 1 y 2 z 3; int 1
  run hset a x 2 y 3 z 4; int 0
  run object encoding a; str $t.name
  run hget a x; str 2
  run hget a y; str 3
  run hget a z; str 4
}

test "hset: convert after max len" {
  run config set hash-max-listpack-entries 1; ok
  run hset a x 1; int 1
  run object encoding a; str listpack
  run hset a y 2; int 1
  run object encoding a str hashtable
}

test "hincrby: convert after max len" {
  run config set hash-max-listpack-entries 1; ok
  run hincrby a x 1; int 1
  run object encoding a; str listpack
  run hincrby a y 2; int 2
  run object encoding a; str hashtable
}

test "hincrbyfloat: convert after max len" {
  discard hello 3
  run config set hash-max-listpack-entries 1; ok
  run hincrbyfloat a x 1.5; float 1.5
  run object encoding a; str listpack
  run hincrbyfloat a y 2.5; float 2.5
  run object encoding a str hashtable
}

test "hset: convert afer max value" {
  run config set hash-max-listpack-value 10; ok
  run hset a x 1; int 1
  run object encoding a; str listpack
  run hset a xxxxxxxxxxxxxxx 2; int 1
  run object encoding a; str hashtable
}

test "hincrby: convert afer max value" {
  run config set hash-max-listpack-value 10; ok
  run hincrby a x 1; int 1
  run object encoding a; str listpack
  run hincrby a xxxxxxxxxxxxxxx 2; int 2
  run object encoding a; str hashtable
}

test "hincrbyfloat: convert afer max value" {
  discard hello 3
  run config set hash-max-listpack-value 10; ok
  run hincrbyfloat a x 1.5; float 1.5
  run object encoding a; str listpack
  run hincrbyfloat a xxxxxxxxxxxxxxxxxx 2.5; float 2.5
  run object encoding a; str hashtable
}

test "hset: odd arguments" {|t|
  run hset h x 1 y; err "ERR wrong number of arguments for 'hset' command"
  run type h; str none
}

test "hset: touch watched keys" {
  touch x { run hset x a 1; int 1 }
}

test "hset: do not touch if not modified" {
  run hset h x 1 y 2; int 2
  notouch h { run hset h x 1; int 0 }
}

test "hset: dirty" {
  dirty 3 { run hset h x 1 y 2 z 3; int 3 }
  dirty 0 { run hset h x 1; int 0 }
  dirty 1 { run hset h a 1; int 1 }
}

hashtable-and-listpack "hsetnx" {|t|
  run hsetnx a x 1; int 1
  run hget a x; str 1
  run hsetnx a x 2; int 0
  run hget a x; str 1
  run object encoding a; str $t.name
}

test "hsetnx: touch watched keys" {
  touch x { run hsetnx x a 1; int 1 }
}

test "hsetnx: dirty" {
  dirty 1 { run hsetnx h x 1; int 1 }
  dirty 1 { run hsetnx h y 2; int 1 }
}

hashtable-and-listpack "hstrlen" {|t|
  run hsetnx a x 12345; int 1
  run object encoding a; str $t.name
  run hstrlen a x; int 5
  run hstrlen a y; int 0
}

hashtable-and-listpack "hmset" {|t|
  run hmset a x 1 y 2; ok
  run hmset a x 1 y 2 z 3; ok
  run hmset a x 2 y 3 z 4; ok
  run hget a x; str 2
  run hget a y; str 3
  run hget a z; str 4
  run object encoding a; str $t.name
}

hashtable-and-listpack "hdel" {|t|
  run hset a x 1 y 2 z 3; int 3
  run object encoding a; str $t.name
  run hdel a x; int 1
  run hget a x; nil
  run hget a y; str 2
  run hget a z; str 3
  run hdel a x y z; int 2
  run hget a x; nil
  run hget a y; nil
  run hget a z; nil
}

test "hdel: touch watched keys" {
  run hset x a 1; int 1
  touch x { run hdel x a; int 1 }
}

hashtable-and-listpack "hexists" {|t|
  run hset a x 1; int 1
  run object encoding a; str $t.name
  run hexists a x; int 1
  run hexists a y; int 0
}

test "hdel: remove empty" {
  run hset a x 1; int 1
  run hdel a x; int 1
  run get a; nil
}

hashtable-and-listpack "hdel: dirty" {|t|
  dirty 3 { run hset h x 1 y 2 z 3; int 3 }
  run object encoding h; str $t.name
  dirty 0 { run hdel h a; int 0 }
  dirty 1 { run hdel h x; int 1 }
  dirty 2 { run hdel h y z; int 2 }
}

hashtable-and-listpack "hdel: no touch when not removed" {|t|
  run hset h x 1; int 1
  notouch h { run hdel h y; int 0 }
}

hashtable-and-listpack "hkeys" {|t|
  run hset h x 1; int 1
  run hkeys h; array [x]
  run object encoding h; str $t.name
}

hashtable-and-listpack "hincrby" {|t|
  run hincrby a x asdf; err "ERR value is not an integer or out of range"
  run hincrby a x 3; int 3
  run hget a x; str 3
  run hincrby a x 7; int 10
  run hget a x; str 10
  run hincrby a x "-72"; int (-62)
  run hget a x; str "-62"
}

hashtable-and-listpack "hincrby: convert values" {|t|
  discard hello 3

  run hset h x 123; int 1
  run hincrby h x 2; int 125

  run hset h x abc; int 0
  run hincrby h x 2; err "ERR value is not an integer or out of range"

  run hset h x 0; int 0
  run hincrbyfloat h x 5; float 5.0
  run hincrby h x 2; int 7
}

hashtable-and-listpack "hincrbyfloat: convert values" {|t|
  discard hello 3

  run hset h x 123.3; int 1
  run hincrbyfloat h x 2.3; float 125.6

  run hset h x abc; int 0
  run hincrbyfloat h x 2; err "ERR value is not a valid float"

  run hset h x 0; int 0
  run hincrby h x 1; int 1
  run hincrbyfloat h x 5.5; float 6.5
}

hashtable-and-listpack "hincrby: touch watched keys" {|t|
  run hset x a 1; int 1
  touch x { run hincrby x a 1; int 2 }
}

hashtable-and-listpack "hincrby: overflow" {|t|
  run hset a x $"($I64MAX - 1)"; int 1
  run hincrby a x 1; int $I64MAX
  run hincrby a x 1; err "ERR increment or decrement would overflow"
}

hashtable-and-listpack "hincrby: dirty" {|t|
  dirty 1 { run hincrby h x 1; int 1 }
  dirty 1 { run hincrby h x 3; int 4 }
}

hashtable-and-listpack "hincrbyfloat" {|t|
  discard hello 3
  run hincrbyfloat a x asdf; err "ERR value is not a valid float"
  run hincrbyfloat a x 3; float 3.0
  run hget a x; str 3
  run hincrbyfloat a x 3; float 6.0
  run hget a x; str 6
  run hincrbyfloat a x "-7"; float (-1.0)
  run hget a x; str "-1"
  run hincrbyfloat a x "inf"; err "ERR increment would produce NaN or Infinity"
  run hincrbyfloat a x "-inf"; err "ERR increment would produce NaN or Infinity"
}

hashtable-and-listpack "hincrbyfloat: touch watched keys" {|t|
  discard hello 3
  run hincrbyfloat x a 1.5; float 1.5
  touch x { run hincrbyfloat x a 1.25; float 2.75 }
}

hashtable-and-listpack "hincrbyfloat: dirty" {|t|
  discard hello 3
  dirty 1 { run hincrbyfloat h x 1.1; float 1.1 }
  dirty 1 { run hincrbyfloat h x 3.2; float 4.3 }
}

hashtable-and-listpack "hvals" {|t|
  run hset a x 1; int 1
  run hvals a; array ["1"]
}

hashtable-and-listpack "hlen" {|t|
  run hset a x 1 y 2; int 2
  run hlen a; int 2
}

hashtable-and-listpack "hmget" {|t|
  run hset a x 1 y 2; int 2
  run hmget a x y z; array ["1" "2" null]
}

test "hget/hset: wrongtype" {
  run set a x; ok
  run hget a x; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run hset a x 1 y 2; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}
