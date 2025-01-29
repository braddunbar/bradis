use bradis *

test "store: wrong arguments" {
  run swapdb 2; err "ERR wrong number of arguments for 'swapdb' command"
  run swapdb 2 3 4; err "ERR wrong number of arguments for 'swapdb' command"
}

test "flushall" {
  run mset a 1 b 2 c 3; ok
  run expire a 10; int 1
  ttl a 10

  run select 1; ok
  run mset a 1 b 2 c 3; ok
  run expire a 10; int 1
  ttl a 10

  run flushall invalid; err "ERR syntax error"
  run flushall sync; ok
  run flushall async; ok
  run flushall; ok

  run select 0; ok
  run keys *; array []
  run pttl a; int -2

  run select 1; ok
  run keys *; array []
  run pttl a; int -2
}

test "flushdb" {
  run mset a 1 b 2 c 3; ok
  run expire a 10; int 1
  ttl a 10
  run flushdb invalid; err "ERR syntax error"
  run flushdb sync; ok
  run flushdb async; ok
  run flushdb; ok
  run keys *; array []
  run pttl a; int -2
}

test "select" {
  run set x 0; ok
  run select 1; ok
  run set x 1; ok
  run get x; str 1
  run select 0; ok
  run get x; str 0
}

test "ping pong" {
  run ping; str PONG
}

test "ping: wrong arguments" {
  run ping x x; err "ERR wrong number of arguments for 'ping' command"
}

test "ping args" {
  run ping x; str x
}

test "select invalid db" {
  run select 123456; err "ERR DB index is out of range"
}

test "swapdb" {
  run select 0; ok
  run set x 0; ok
  run select 1; ok
  run set x 1; ok

  run swapdb 0 200; err "ERR DB index is out of range"
  run swapdb 200 0; err "ERR DB index is out of range"
  run swapdb 0 1; ok

  run select 0; ok
  run get x; str 1
  run select 1; ok
  run get x; str 0
}
