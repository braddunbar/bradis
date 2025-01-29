use bradis *

test "move: wrong arguments" {
  run move 2; err "ERR wrong number of arguments for 'move' command"
  run move 2 3 4; err "ERR wrong number of arguments for 'move' command"
}

test "copy: wrong arguments" {
  run copy; err "ERR wrong number of arguments for 'copy' command"
  run copy 2; err "ERR wrong number of arguments for 'copy' command"
}

test "move: invalid db index" {
  run set x 1; ok
  run move x 21532; err "ERR DB index is out of range"
}

test "copy: invalid db index" {
  run set x 1; ok
  run copy x y db 21532; err "ERR DB index is out of range"
}

test "copy: db not an integer" {
  run set x 1; ok
  run copy x y db notanumber; err "ERR value is not an integer or out of range"
}

test "move: same db index" {
  run set x 1; ok
  run move x 0; err "ERR source and destination objects are the same"
}

test "copy: same db index" {
  run set x 1; ok
  run copy x x; err "ERR source and destination objects are the same"
  run copy x x db 0; err "ERR source and destination objects are the same"
}

test "move: existing key" {
  run set x 0; ok
  run select 1; ok
  run set x 1; ok
  run select 0; ok
  run move x 1; int 0
  run get x; str 0
  run select 1; ok
  run get x; str 1
}

test "copy: existing key" {
  run set x 0; ok
  run set y 1; ok

  # Already exists
  run copy x y; int 0
  run get x; str 0
  run get y; str 1

  # Use replace
  run copy x y replace; int 1
  run get x; str 0
  run get y; str 0

  # Different db
  run select 1; ok
  run set y 1; ok
  run select 0; ok

  # Already exists
  run copy x y db 1; int 0
  run select 1; ok
  run get y; str 1
  run select 0; ok

  # Use replace
  run copy x y db 1 replace; int 1
  run select 1; ok
  run get y; str 0
}

test "move: missing key" {
  run move x 1; int 0
  run select 1; ok
  run get x; nil
}

test "copy: missing key" {
  run copy x y db 1; int 0
  run select 1; ok
  run get y; nil
}

test "move" {
  run set x 1; ok
  run move x 2; int 1
  run get x; nil
  run select 2; ok
  run get x; str 1
}

test "copy" {
  run set x 1; ok
  run copy x y; int 1
  run get x; str 1
  run get y; str 1
}

test "move: with expire" {
  run set x 1 ex 200; ok
  run get x; str 1
  run move x 2; int 1
  run get x; nil
  run select 2; ok
  run get x; str 1
  ttl x 200
}

test "copy: with expire" {
  run set x 1 ex 200; ok
  run get x; str 1
  run copy x y; int 1
  run get x; str 1
  run get y; str 1
  ttl y 200
}

test "move: touch watched keys" {
  run select 1; ok
  run set x 1; ok

  client 2 {
    run select 1; ok
    run watch x; ok
  }

  client 3 {
    run select 2; ok
    run watch x; ok
  }

  run select 1; ok
  run move x 2; int 1

  client 2 {
    run multi; ok
    run get x; str QUEUED
    run exec; nil
  }

  client 3 {
    run multi; ok
    run get x; str QUEUED
    run exec; nil
  }
}

test "copy: touch watched keys" {
  run select 1; ok
  run set x 1; ok

  client 2 {
    run select 1; ok
    run watch y; ok
  }

  client 3 {
    run select 2; ok
    run watch y; ok
  }

  run select 1; ok
  run copy x y; int 1
  run copy x y db 2; int 1

  client 2 {
    run multi; ok
    run get y; str QUEUED
    run exec; nil
  }

  client 3 {
    run multi; ok
    run get y; str QUEUED
    run exec; nil
  }
}

test "rename" {
  run set x 1; ok
  run rename x y; ok
  run get x; nil
  run get y; str 1
}

test "rename: existing" {
  run set x 1; ok
  run set y 2; ok
  run rename x y; ok
  run get x; nil
  run get y; str 1
}

test "rename: expire" {
  run set x 1 px 10000; ok
  run rename x y; ok
  run get x; nil
  run get y; str 1
  ttl y 10
}

test "rename: missing" {
  run rename x y; err "ERR no such key"
}

test "rename: samekey missing" {
  run rename x x; err "ERR no such key"
}

test "rename: samekey" {
  run set x 1; ok
  run rename x x; ok
  run get x; str 1
}

test "rename: touch from key" {
  run set x 1; ok
  touch x { run rename x y; ok }
}

test "rename: touch to key" {
  run set x 1; ok
  touch y { run rename x y; ok }
}

test "renamenx" {
  run set x 1; ok
  run renamenx x y; int 1
  run get x; nil
  run get y; str 1
}

test "renamenx: expire" {
  run set x 1 px 10000; ok
  run renamenx x y; int 1
  run get x; nil
  run get y; str 1
  ttl y 10
}

test "renamenx: missing" {
  run renamenx x y; err "ERR no such key"
}

test "renamenx: samekey missing" {
  run renamenx x x; err "ERR no such key"
}

test "renamenx: samekey" {
  run set x 1; ok
  run renamenx x x; int 0
  run get x; str 1
}

test "renamenx: existing" {
  run set x 1; ok
  run set y 2; ok
  run renamenx x y; int 0
}

test "renamenx: touch from key" {
  run set x 1; ok
  touch x { run renamenx x y; int 1 }
}

test "renamenx: touch to key" {
  run set x 1; ok
  touch y { run renamenx x y; int 1 }
}
