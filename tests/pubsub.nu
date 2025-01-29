use bradis *
use std/assert

test "pubsub: help" {
  discard hello 3
  run pubsub help
  assert str contains (read-string) PUBSUB
}

test "pubsub: flag" {
  client 2 { noflag 1 P }

  # subscribe starts pubsub mode
  client 1 { run subscribe x; array [subscribe x 1] }
  client 2 { flag 1 P }

  # unsubscribe ends pubsub mode
  client 1 { run unsubscribe x; array [unsubscribe x 0] }
  client 2 { noflag 1 P }

  # psubscribe starts pubsub mode
  client 1 { run psubscribe x; array [psubscribe x 1] }
  client 2 { flag 1 P }

  # punsubscribe ends pubsub mode
  client 1 { run punsubscribe x; array [punsubscribe x 0] }
  client 2 { noflag 1 P }

  # subscribe starts pubsub mode
  client 1 { run subscribe x; array [subscribe x 1] }
  client 2 { flag 1 P }

  # unsubscribe all ends pubsub mode
  client 1 { run unsubscribe; array [unsubscribe x 0] }
  client 2 { noflag 1 P }

  # psubscribe starts pubsub mode
  client 1 { run psubscribe x; array [psubscribe x 1] }
  client 2 { flag 1 P }

  # punsubscribe all ends pubsub mode
  client 1 { run punsubscribe; array [punsubscribe x 0] }
  client 2 { noflag 1 P }
}

test "subscribe" {
  discard hello 3
  run subscribe x
  push [subscribe x 1]
  run subscribe y z
  push [subscribe y 2]
  push [subscribe z 3]
  client 2 { run publish x hi; int 1 }
  push [message x hi]
}

test "unsubscribe" {
  discard hello 3
  run subscribe x y z
  push [subscribe x 1]
  push [subscribe y 2]
  push [subscribe z 3]

  client 2 { run publish x hi; int 1 }

  push [message x hi]
  run unsubscribe x
  push [unsubscribe x 2]

  client 2 {
    run publish x hi; int 0
    run publish y hi; int 1
  }

  push [message y hi]
}

test "psubscribe" {
  discard hello 3
  run subscribe x
  push [subscribe x 1]

  run psubscribe h?llo
  push [psubscribe h?llo 2]

  client 2 {
    run publish hello hi; int 1
    run publish heeeeello hi; int 0
    run publish x hi; int 1
  }

  push [pmessage h?llo hello hi]
  push [message x hi]
}

test "punsubscribe" {
  discard hello 3
  run subscribe x
  push [subscribe x 1]

  run psubscribe h?llo
  push [psubscribe h?llo 2]

  client 2 { run publish hello hi; int 1 }

  push [pmessage h?llo hello hi]
  run punsubscribe h?llo
  push [punsubscribe h?llo 1]

  client 2 {
    run publish hello hi; int 0
    run publish x hi; int 1
  }

  push [message x hi]
}

test "pubsub: numsub" {
  discard hello 3
  run subscribe x
  push [subscribe x 1]

  client 2 {
    discard hello 3
    run subscribe a x
    push [subscribe a 1]
    push [subscribe x 2]
    run pubsub numsub a x; array [a 1 x 2]
  }
}

test "pubsub: numpat" {
  discard hello 3
  run psubscribe h?llo
  push [psubscribe h?llo 1]

  client 2 {
    discard hello 3
    run psubscribe h?llo b??m
    push [psubscribe h?llo 1]
    push [psubscribe b??m 2]

    run pubsub numpat; int 2
  }
}

test "pubsub: channels" {
  discard hello 3
  run subscribe hello world
  push [subscribe hello 1]
  push [subscribe world 2]
  run psubscribe h?llo
  push [psubscribe h?llo 3]
  run pubsub channels
  let value = read-value
  assert ("hello" in $value)
  assert ("world" in $value)
  run pubsub channels h?llo; array [hello]
}

test "pubsub: resp2 ping" {
  # normal ping
  run ping; str PONG
  run ping foo; str foo

  # subscribe starts pubsub mode
  run subscribe x; array [subscribe x 1]

  # pubsub ping
  run ping; array [pong ""]
  run ping foo; array [pong foo]

  # unsubscribe ends pubsub mode
  run unsubscribe x; array [unsubscribe x 0]

  # normal ping
  run ping; str PONG
  run ping foo; str foo

  # psubscribe starts pubsub mode
  run psubscribe x; array [psubscribe x 1]

  # pubsub ping
  run ping; array [pong ""]
  run ping foo; array [pong foo]

  # punsubscribe ends pubsub mode
  run punsubscribe x; array [punsubscribe x 0]

  # normal ping
  run ping; str PONG
  run ping foo; str foo

  # subscribe starts pubsub mode
  run subscribe x; array [subscribe x 1]

  # unsubscribe all ends pubsub mode
  run unsubscribe; array [unsubscribe x 0]

  # normal ping
  run ping; str PONG
  run ping foo; str foo

  # psubscribe starts pubsub mode
  run psubscribe x; array [psubscribe x 1]

  # pubsub ping
  run ping; array [pong ""]
  run ping foo; array [pong foo]

  # punsubscribe all ends pubsub mode
  run punsubscribe; array [punsubscribe x 0]

  # normal ping
  run ping; str PONG
  run ping foo; str foo
}

test "pubsub: resp2 disallowed" {
  run subscribe x; array [subscribe x 1]

  run get x; err "ERR Can't execute 'get': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
  run set x 1; err "ERR Can't execute 'set': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
  run rpush l 1; err "ERR Can't execute 'rpush': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"

  run unsubscribe; array [unsubscribe x 0]

  run get x; nil
  run set x 1; ok
  run rpush l 1; int 1
}

test "pubsub: resp2 quit" {
  run subscribe x; array [subscribe x 1]

  run quit; ok
  assert (client closed 1)
}

test "pubsub: disconnect" {
  discard hello 3
  run subscribe x; push [subscribe x 1]
  run psubscribe h?llo; push [psubscribe h?llo 2]
  run pubsub channels x; array [x]
  run pubsub numsub x; array [x 1]
  run pubsub numpat; int 1
  run quit; ok
  client 2 {
    run pubsub channels x; array []
    run pubsub numsub x; array [x 0]
    run pubsub numpat; int 0
  }
}

test "unsubscribe: always reply" {
  run unsubscribe; array [unsubscribe null 0]
  run psubscribe x*; array [psubscribe x* 1]
  run unsubscribe; array [unsubscribe null 1]
}

test "punsubscribe: always reply" {
  run punsubscribe; array [punsubscribe null 0]
  run subscribe x; array [subscribe x 1]
  run punsubscribe; array [punsubscribe null 1]
}

test "subscribe: wrong arguments" {
  run subscribe; err "ERR wrong number of arguments for 'subscribe' command"
}

test "psubscribe: wrong arguments" {
  run psubscribe; err "ERR wrong number of arguments for 'psubscribe' command"
}

test "publish: wrong arguments" {
  run publish 2; err "ERR wrong number of arguments for 'publish' command"
  run publish 2 3 4; err "ERR wrong number of arguments for 'publish' command"
}

test "pubsub: wrong arguments" {
  run pubsub; err "ERR wrong number of arguments for 'pubsub' command"
  run pubsub help invalid; err "ERR Unknown subcommand or wrong number of arguments for 'help'. Try PUBSUB HELP."
  run pubsub channels pattern invalid; err "ERR Unknown subcommand or wrong number of arguments for 'channels'. Try PUBSUB HELP."
  run pubsub numpat invalid; err "ERR Unknown subcommand or wrong number of arguments for 'numpat'. Try PUBSUB HELP."
}
