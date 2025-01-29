use bradis *
use std/assert

test "config: wrong arguments" {
  run config; err "ERR wrong number of arguments for 'config' command"
  run config help invalid; err "ERR Unknown subcommand or wrong number of arguments for 'help'. Try CONFIG HELP."
  run config resetstat invalid; err "ERR Unknown subcommand or wrong number of arguments for 'resetstat'. Try CONFIG HELP."
  run config get; err "ERR Unknown subcommand or wrong number of arguments for 'get'. Try CONFIG HELP."
  run config get key invalid; err "ERR Unknown subcommand or wrong number of arguments for 'get'. Try CONFIG HELP."
  run config set key; err "ERR Unknown subcommand or wrong number of arguments for 'set'. Try CONFIG HELP."
  run config set key value invalid; err "ERR Unknown subcommand or wrong number of arguments for 'set'. Try CONFIG HELP."
}

test "stat: total_connections_received" {
  client 1 { run get x; nil }
  client 2 { run get x; nil }
  client 3 { run get x; nil }
  assert equal "3" (info total_connections_received)
  run config resetstat; ok
  assert equal "0" (info total_connections_received)
}

test "stat: total_commands_processed" {
  run set x 1; ok
  run get x; str 1
  run get x; str 1
  run get x; str 1
  run get x; str 1
  assert equal "6" (info total_commands_processed)
  run config resetstat; ok
  assert equal "1" (info total_commands_processed)
}

test "config: unsupported parameter" {
  run config set unsupported 1; err "ERR Unknown option or number of arguments for CONFIG SET - 'unsupported'"
}

test "config: ignore case" {
  discard hello 3
  run config get Proto-Max-Bulk-Len
  map { proto-max-bulk-len: "536870912" }
}

test "config: zset-max-listpack-value" {
  let keys = [zset-max-listpack-value zset-max-ziplist-value]

  discard hello 3

  ["64" "128" "256"] | enumerate | each {|v|
    for $k in $keys {
      # Default
      if $v.index > 0 { run config set $k $v.item; ok }
      for $k in $keys {
        run config get $k; map {$k: $v.item}
      }
    }
  }
}

test "config: zset-max-listpack-entries" {
  let keys = [zset-max-listpack-entries zset-max-ziplist-entries]

  discard hello 3

  ["128" "1024" "2048"] | enumerate | each {|v|
    for $k in $keys {
      # Default
      if $v.index > 0 { run config set $k $v.item; ok }
      for $k in $keys {
        run config get $k; map {$k: $v.item}
      }
    }
  }
}

test "config: hash-max-listpack-value" {
  let keys = [hash-max-listpack-value hash-max-ziplist-value]

  discard hello 3

  ["64" "128" "256"] | enumerate | each {|v|
    for $k in $keys {
      # Default
      if $v.index > 0 { run config set $k $v.item; ok }
      for $k in $keys {
        run config get $k; map {$k: $v.item}
      }
    }
  }
}

test "config: hash-max-listpack-entries" {
  let keys = [hash-max-listpack-entries hash-max-ziplist-entries]

  discard hello 3

  ["512" "1024" "2048"] | enumerate | each {|v|
    for $k in $keys {
      # Default
      if $v.index > 0 { run config set $k $v.item; ok }
      for $k in $keys {
        run config get $k; map {$k: $v.item}
      }
    }
  }
}

test "config: set-max-intset-entries" {
  let k = "set-max-intset-entries"
  discard hello 3
  run config get $k; map {$k: "512"}
  run config set $k 128; ok
  run config get $k; map {$k: "128"}
}

test "config: yes/no" {
  let keys = [
    lazyfree-lazy-user-flush
    lazyfree-lazy-expire
    lazyfree-lazy-user-del
  ]
  discard hello 3
  $keys | each {|k|
    run config set $k invalid; err $"ERR Invalid argument 'invalid' for CONFIG SET '($k)' - argument must be 'yes' or 'no'"
    run config get $k; map {$k: no}
    run config set $k yes; ok
    run config get $k; map {$k: yes}
    run config set $k no; ok
    run config get $k; map {$k: no}
    run config set $k YES; ok
    run config get $k; map {$k: yes}
    run config set $k NO; ok
    run config get $k; map {$k: no}
    run config set $k Yes; ok
    run config get $k; map {$k: yes}
    run config set $k No; ok
    run config get $k; map {$k: no}
  }
}

test "proto-max-bulk-len" {
  let k = "proto-max-bulk-len"
  discard hello 3
  let values = {
    "500000": 500_000
    "5k": 5_000
    "5K": 5_000
    "10kb": 10_240
    "10Kb": 10_240
    "10KB": 10_240
    "10kB": 10_240
    "5m": 5_000_000
    "5M": 5_000_000
    "5mb": 5_242_880
    "5MB": 5_242_880
    "5Mb": 5_242_880
    "5mB": 5_242_880
    "5g": 5_000_000_000
    "5G": 5_000_000_000
    "5gb": 5_368_709_120
    "5GB": 5_368_709_120
    "5Gb": 5_368_709_120
    "5gB": 5_368_709_120
  }

  run config get $k; map {$k: "536870912"}
  run config set $k invalid; err $"ERR Invalid argument 'invalid' for CONFIG SET '($k)' - argument must be a memory value"
  $values | transpose in out | each {|v|
    run config set $k $v.in; ok
    run config get $k; map {$k: $'($v.out)'}
  }
}

test "config help" {
  run config help
  assert str contains (read-string) CONFIG
}

test "list-max-listpack-size" {
  let keys = [list-max-listpack-size list-max-ziplist-size]
  discard hello 3

  ["-2" "10" "-1"] | enumerate | each {|v|
    for $k in $keys {
      # Default
      if $v.index > 0 { run config set $k $v.item; ok }
      for $k in $keys {
        run config get $k; map {$k: $v.item}
      }
    }
  }
}
