export const I8MAX = 127
export const I16MAX = 32_767
export const I32MAX = 2_147_483_647
export const I64MAX = 9_223_372_036_854_775_807
export const I64MIN = -9_223_372_036_854_775_808
export const U128MAX = 340_282_366_920_938_463_463_374_607_431_768_211_455
const TIMEOUT = 500

export def now [] {
  (date now | into int) // 10 ** 6
}

export def "now s" [] {
  (date now | into int) // 10 ** 9
}

def unexpected [expected actual meta] {
  error make {
    msg: $"expected ($expected) but got ($actual)",
    label: {
      text: "here",
      span: $meta.span,
    },
  }
}

def await [timeout: int meta f: closure] {
  let start = now

  while (now) - $start <= $timeout {
    if (do $f) { return }
  }

  error make {
    msg: "timeout",
    label: {
      text: "here",
      span: $meta.span,
    },
  }
}

export def info [key: string] {
  run info
  read-string |
  lines |
  where { str starts-with $"($key):" } |
  split row -n 2 ":" |
  get 1
}

export def read-string [x?]: nothing -> string {
  let value = read-value
  match ($value | describe) {
    string => $value
    _ => (
      match $value.type? {
        verbatim => $value.value
        _ => (unexpected "string/verbatim" $value (metadata $x))
      }
    )
  }
}

export def "client info" [index: int key: string] {
  let id = client $index { client-id }
  run client list id $id
  read-string |
  split row " " |
  where { str starts-with $"($key)=" } |
  split row -n 2 "=" |
  get 1
}

export def "client info await" [index: int key: string expected: string] {
  await $TIMEOUT (metadata $index) {
    (client info $index $key) == $expected
  }
}

export def await-flag [index: int flag: string] {
  await $TIMEOUT (metadata $index) {
    client info $index flags | str contains $flag
  }
}

export def flag [index: int expected: string] {
  let flags = client info $index flags
  if not ($flags | str contains $expected) {
    error make {
      msg: $"expected ($flags) to contain ($expected)",
      label: {
        text: "here",
        span: (metadata $expected).span,
      },
    }
  }
}

export def noflag [index: int expected: string] {
  let flags = client info $index flags
  if ($flags | str contains $expected) {
    error make {
      msg: $"expected ($flags) not to contain ($expected)",
      label: {
        text: "here",
        span: (metadata $expected).span,
      },
    }
  }
}

export def discard [...args: string] {
  run ...$args
  read-value
}

export def dirty [expected: int body: closure] {
  let before = info rdb_changes_since_last_save | into int
  do $body
  let after = info rdb_changes_since_last_save | into int
  let actual = $after - $before
  if $actual != $expected {
    unexpected $expected $actual (metadata $expected)
  }
}

export def float [expected: float] {
  let value = read-value
  if $value != $expected {
    unexpected $expected $value (metadata $expected)
  }
  $value
}

export def int [expected: int] {
  let value = read-value
  if $value != $expected {
    unexpected $expected $value (metadata $expected)
  }
  $value
}

export def nil [x?] {
  let value = read-value
  if $value != null {
    unexpected "nil" $value (metadata $x)
  }
}

export def ok [x?] {
  let value = read-value
  if $value != "OK" {
    unexpected "OK" $value (metadata $x)
  }
}

export def bin [expected: binary] {
  let value = read-value

  if (
    not (($value | describe) in [string binary]) or
    ($value | into binary) != $expected
  ) {
    unexpected $expected $value (metadata $expected)
  }
}

export def str [expected: string] {
  let value = read-value
  if $value != $expected {
    unexpected $expected $value (metadata $expected)
  }
  $value
}

export def push [expected: list] {
  let value = read-value
  if $value.type? != push or $value.value != $expected {
    unexpected $expected $value (metadata $expected)
  }
}

export def map [expected: record] {
  let value = read-value
  if $value.type? != map or $value.value != $expected {
    unexpected $expected $value (metadata $expected)
  }
}

export def set [expected: list] {
  let value = read-value
  if $value.type? != set or ($value.value | sort) != ($expected | sort) {
    unexpected $expected $value (metadata $expected)
  }
}

export def array [expected: list] {
  let value = read-value
  if $value != $expected {
    unexpected $expected $value (metadata $expected)
  }
}

export def err [expected: string] {
  let value = read-value
  if $value.type? != "error" or $value.value != $expected {
    unexpected $expected $value (metadata $expected)
  }
  $value.value
}

export def touch [key: string body: closure] {
  run watch $key; ok
  do $body
  run multi; ok
  run ping; str QUEUED
  run exec;
  try {
    nil
  } catch {
    error make {
      msg: $"expected to touch ($key)",
      label: {
        text: "here",
        span: (metadata $body).span,
      },
    }
  }
}

export def notouch [key: string body: closure] {
  run watch $key; ok
  do $body
  run multi; ok
  run ping; str QUEUED
  run exec;
  try {
    array [PONG]
  } catch {
    error make {
      msg: $"expected not to touch ($key)",
      label: {
        text: "here",
        span: (metadata $body).span,
      },
    }
  }
}

export def ttl [key: string ttl: int] {
  run pttl $key
  let value = read-value
  let pttl = $ttl * 1000
  let delta = 50
  let expected = ($pttl - 50)..($pttl + 50)
  if not ($value in $expected) {
    unexpected $expected $value (metadata $key)
  }
}
