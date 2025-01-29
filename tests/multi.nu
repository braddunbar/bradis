use bradis *

test "discard: wrong arguments" {
  run discard x; err "ERR wrong number of arguments for 'discard' command"
}

test "discard: with multi" {
  run multi; ok
  run discard; ok
  run exec; err "ERR EXEC without MULTI"
}

test "discard: without multi" {
  run discard; ok
  run exec; err "ERR EXEC without MULTI"
}

test "exec: abort" {
  run multi; ok
  run get x y; err "ERR wrong number of arguments for 'get' command"
  run exec; err "EXECABORT Transaction discarded because of previous errors."
}

test "exec: wrong arguments" {
  run exec xx; err "ERR wrong number of arguments for 'exec' command"
}

test "multi: wrong arguments" {
  run multi xx; err "ERR wrong number of arguments for 'multi' command"
}
