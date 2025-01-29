use bradis *

test "echo: wrong arguments" {
  run echo; err "ERR wrong number of arguments for 'echo' command"
  run echo 2 3; err "ERR wrong number of arguments for 'echo' command"
}

test "echo" {
  run echo abc; str abc
}

test "dbsize" {
  run dbsize a; err "ERR wrong number of arguments for 'dbsize' command"
  run dbsize; int 0
  run set a 1; ok
  run dbsize; int 1
  run set b 2; ok
  run dbsize; int 2
}
