use bradis *
use std/assert

test "run command after invalid argument" {
  run-inline "get 'x'y"; err "ERR Invalid argument(s)"
  run-inline "get 'x'y"; err "ERR Invalid argument(s)"
  run set x foo; ok
  run get x; str foo
}

test "invalid single quoted argument" {
  run-inline "get 'x'y"; err "ERR Invalid argument(s)"
  # Should not exit
  run-inline "get 'x'y"; err "ERR Invalid argument(s)"
}

test "missing arguments" {
  run get; err "ERR wrong number of arguments for 'get' command"
}

test "too many arguments" {
  run get abc 123; err "ERR wrong number of arguments for 'get' command"
}

test "unknown op" {
  run unknown abc 123; err "ERR unknown command"
}

test "hello" {
  run hello 3
  let value = read-value
  assert equal $value.type "map"
  assert equal $value.value.proto "3"
  assert equal $value.value.server bradis
}

test "hello: invalid protocol" {
  run hello invalid; err "NOPROTO unsupported protocol version"
}

test "hello: setname" {
  run hello 3 setname foo
  let value = read-value
  assert equal $value.type "map"
  assert equal $value.value.proto "3"
  assert equal (client info 1 name) "foo"

  run hello 3 setname bar
  let value = read-value
  assert equal $value.type "map"
  assert equal $value.value.proto "3"
  assert equal (client info 1 name) "bar"
}
