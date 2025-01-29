use bradis *

test "expire: wrong arguments" {
  run expire; err "ERR wrong number of arguments for 'expire' command"
  run expire x; err "ERR wrong number of arguments for 'expire' command"
  run expire x 10 nx x; err "ERR wrong number of arguments for 'expire' command"
}

test "expireat: wrong arguments" {
  run expireat; err "ERR wrong number of arguments for 'expireat' command"
  run expireat x; err "ERR wrong number of arguments for 'expireat' command"
  run expireat x 10 nx x; err "ERR wrong number of arguments for 'expireat' command"
}

test "persist: wrong arguments" {
  run persist; err "ERR wrong number of arguments for 'persist' command"
  run persist 2 3; err "ERR wrong number of arguments for 'persist' command"
}

test "pexpire: wrong arguments" {
  run pexpire 2; err "ERR wrong number of arguments for 'pexpire' command"
  run pexpire x 10 nx x; err "ERR wrong number of arguments for 'pexpire' command"
}

test "pexpireat: wrong arguments" {
  run pexpireat 2; err "ERR wrong number of arguments for 'pexpireat' command"
  run pexpireat x 10 nx x; err "ERR wrong number of arguments for 'pexpireat' command"
}

test "pttl: wrong arguments" {
  run pttl; err "ERR wrong number of arguments for 'pttl' command"
  run pttl 2 3; err "ERR wrong number of arguments for 'pttl' command"
}

test "ttl: wrong arguments" {
  run ttl; err "ERR wrong number of arguments for 'ttl' command"
  run ttl 2 3; err "ERR wrong number of arguments for 'ttl' command"
}

test "expire" {
  run set a x; ok
  run expire a 10; int 1
  ttl a 10
}

test "expire: delete" {
  run set a x; ok
  run expire a "-10"; int 1
  run get a; nil
}

test "expire: overflow" {
  run set a x; ok
  run expire a $'($U128MAX)'; err "ERR invalid expire time in expire command"
  run ttl a; int -1
}

test "expire: nx" {
  run set a x; ok
  run expire a 10 nx; int 1
  ttl a 10
  run expire a 20 nx; int 0
  ttl a 10
}

test "expire: xx" {
  run set a x; ok
  run expire a 10 xx; int 0
  run pttl a; int -1
  run expire a 10; int 1
  ttl a 10
  run expire a 20 xx; int 1
  ttl a 20
}

test "expire: gt" {
  run set a x; ok
  # Non-volatile keys are treated as infinite
  run expire a 10 gt; int 0
  run pttl a; int -1
  run expire a 10; int 1
  ttl a 10
  run expire a 5 gt; int 0
  ttl a 10
  run expire a 20 gt; int 1
  ttl a 20
}

test "expire: lt" {
  run set a x; ok
  # Non-volatile keys are treated as infinite
  run expire a 10 lt; int 1
  ttl a 10
  run expire a 15 lt; int 0
  ttl a 10
  run expire a 5 lt; int 1
  ttl a 5
}

test "pexpire" {
  run set a x; ok
  run pexpire a 10000; int 1
  ttl a 10
}

test "pexpire: delete" {
  run set a x; ok
  run pexpire a "-10000"; int 1
  run get a; nil
}

test "pexpire: overflow" {
  run set a x; ok
  run pexpire a $'($U128MAX)'; err "ERR invalid expire time in pexpire command"
  run ttl a; int -1
}

test "pexpire: nx" {
  run set a x; ok
  run pexpire a 10000 nx; int 1
  ttl a 10
  run pexpire a 20000 nx; int 0
  ttl a 10
}

test "pexpire: xx" {
  run set a x; ok
  run pexpire a 10000 xx; int 0
  run pttl a; int -1
  run pexpire a 10000; int 1
  ttl a 10
  run pexpire a 20000 xx; int 1
  ttl a 20
}

test "pexpire: gt" {
  run set a x; ok
  # Non-volatile keys are treated as infinite
  run pexpire a 10000 gt; int 0
  run pttl a; int -1
  run pexpire a 10000; int 1
  ttl a 10
  run pexpire a 5000 gt; int 0
  ttl a 10
  run pexpire a 20000 gt; int 1
  ttl a 20
}

test "pexpire: lt" {
  run set a x; ok
  # Non-volatile keys are treated as infinite
  run pexpire a 10000 lt; int 1
  ttl a 10
  run pexpire a 15000 lt; int 0
  ttl a 10
  run pexpire a 5000 lt; int 1
  ttl a 5
}

test "expireat" {
  let s = (now s) + 10
  run set a x; ok
  run expireat a $'($s)'; int 1
  run expiretime a; int $s
}

test "expireat: delete" {
  let s = (now s) - 10
  run set a x; ok
  run expireat a $'($s)'; int 1
  run get a; nil
}

test "expireat: overflow" {
  run set a x; ok
  run expireat a $'($U128MAX)'; err "ERR invalid expire time in expireat command"
  run ttl a; int -1
}

test "expireat: nx" {
  let s1 = (now s) + 10
  let s2 = (now s) + 20
  run set a x; ok
  run expireat a $'($s1)' nx; int 1
  run expiretime a; int $s1
  run expireat a $'($s2)' nx; int 0
  run expiretime a; int $s1
}

test "expireat: xx" {
  let s1 = (now s) + 10
  let s2 = (now s) + 20
  run set a x; ok
  run expireat a $'($s1)' xx; int 0
  run expiretime a; int -1
  run expireat a $'($s1)'; int 1
  run expiretime a; int $s1
  run expireat a $'($s2)' xx; int 1
  run expiretime a; int $s2
}

test "expireat: gt" {
  let s5 = (now s) + 5
  let s10 = (now s) + 10
  let s15 = (now s) + 15
  run set a x; ok
  # Non-volatile keys are treated as infinite
  run expireat a $'($s10)' gt; int 0
  run expiretime a; int -1
  run expireat a $'($s10)'; int 1
  run expiretime a; int $s10
  run expireat a $'($s5)' gt; int 0
  run expiretime a; int $s10
  run expireat a $'($s15)' gt; int 1
  run expiretime a; int $s15
}

test "expireat: lt" {
  let s5 = (now s) + 5
  let s10 = (now s) + 10
  let s15 = (now s) + 15
  run set a x; ok
  # Non-volatile keys are treated as infinite
  run expireat a $'($s10)' lt; int 1
  run expiretime a; int $s10
  run expireat a $'($s15)' lt; int 0
  run expiretime a; int $s10
  run expireat a $'($s5)' lt; int 1
  run expiretime a; int $s5
}

test "pexpireat" {
  let ms = (now) + 10_000
  run set a x; ok
  run pexpireat a $'($ms)'; int 1
  run pexpiretime a; int $ms
}

test "pexpireat: delete" {
  let ms = (now) - 10_000
  run set a x; ok
  run pexpireat a $'($ms)'; int 1
  run get a; nil
}

test "pexpireat: nx" {
  let ms10 = (now) + 10_000
  let ms20 = (now) + 20_000
  run set a x; ok
  run pexpireat a $'($ms10)' nx; int 1
  run pexpiretime a; int $ms10
  run pexpireat a $'($ms20)' nx; int 0
  run pexpiretime a; int $ms10
}

test "pexpireat: xx" {
  let ms1 = (now) + 10_000
  let ms2 = (now) + 20_000
  run set a x; ok
  run pexpireat a $'($ms1)' xx; int 0
  run pexpiretime a; int -1
  run pexpireat a $'($ms1)'; int 1
  run pexpiretime a; int $ms1
  run pexpireat a $'($ms2)' xx; int 1
  run pexpiretime a; int $ms2
}

test "pexpireat: gt" {
  let ms5 = (now) + 5_000
  let ms10 = (now) + 10_000
  let ms15 = (now) + 15_000
  run set a x; ok
  # Non-volatile keys are treated as infinite
  run pexpireat a $'($ms10)' gt; int 0
  run pexpiretime a; int -1
  run pexpireat a $'($ms10)'; int 1
  run pexpiretime a; int $ms10
  run pexpireat a $'($ms5)' gt; int 0
  run pexpiretime a; int $ms10
  run pexpireat a $'($ms15)' gt; int 1
  run pexpiretime a; int $ms15
}

test "pexpireat: lt" {
  let ms5 = (now) + 5_000
  let ms10 = (now) + 10_000
  let ms15 = (now) + 15_000
  run set a x; ok
  # Non-volatile keys are treated as infinite
  run pexpireat a $'($ms10)' lt; int 1
  run pexpiretime a; int $ms10
  run pexpireat a $'($ms15)' lt; int 0
  run pexpiretime a; int $ms10
  run pexpireat a $'($ms5)' lt; int 1
  run pexpiretime a; int $ms5
}

test "ttl: not expired" {
  run set a x; ok
  run expire a 10; int 1
  ttl a 10
}

test "ttl: expired" {
  let s = (now s) - 10
  run set a x; ok
  run expireat a $'($s)'; int 1
  run ttl a; int -2
  run pttl a; int -2
  run get x; nil
}

test "ttl: exists" {
  run set a x; ok
  run ttl a; int -1
  run pttl a; int -1
}

test "ttl: missing" {
  run ttl a; int -2
  run pttl a; int -2
}

test "expire: touch watched keys" {
  run set x 1; ok
  touch x { run expire x 10; int 1 }
}

test "pexpire: touch watched keys" {
  run set x 1; ok
  touch x { run pexpire x 10000; int 1 }
}

test "expireat: touch watched keys" {
  let s = (now s) + 10
  run set x 1; ok
  touch x { run expireat x $'($s)'; int 1 }
}

test "expire: do not touch missing keys" {
  run set a 1; ok
  notouch b { run expire b 10; int 0 }
}

test "persist" {
  run persist x; int 0
  run set x 1; ok
  run persist x; int 0
  run expire x 10; int 1
  ttl x 10
  run persist x; int 1
  run ttl x; int -1
}

test "expiretime" {
  let s = (now s) + 10
  run expiretime x; int -2
  run set x 1; ok
  run expiretime x; int -1
  run expireat x $'($s)'; int 1
  run expiretime x; int $s
}

test "pexpiretime" {
  let ms = (now) + 10_000
  run pexpiretime x; int -2
  run set x 1; ok
  run pexpiretime x; int -1
  run pexpireat x $'($ms)'; int 1
  run pexpiretime x; int $ms
}
