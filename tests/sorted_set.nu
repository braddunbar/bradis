use bradis *

def skiplist-and-listpack [name: string body: closure] {
  let types = [[name value]; [skiplist "0"] [listpack "512"]]
  for t in $types {
    test $"($name) ($t.name)" {
      run config set zset-max-listpack-entries $t.value; ok
      do $body $t
    }
  }
}

test "zadd: wrong arguments" {
  run zadd 2; err "ERR wrong number of arguments for 'zadd' command"
  run zadd 2 3; err "ERR wrong number of arguments for 'zadd' command"
}

skiplist-and-listpack $"zadd: update" {|t|
  run zadd z 1 x; int 1
  run object encoding z; str $t.name
  run zadd z 2 x 3 y 4 z; int 2
}

skiplist-and-listpack "zadd: same score" {|t|
  run zadd z 0 c 0 b 0 a; int 3
  run object encoding z; str $t.name
  run zrange z 0 "-1"; array [a b c]
}

test "zadd: dirty" {
  dirty 2 { run zadd z 1 x 1 y; int 2 }
  dirty 2 { run zadd z 1 x 2 y 3 z; int 1 }
  dirty 0 { run zadd z 1 x 2 y 3 z; int 0 }
}

skiplist-and-listpack "zadd: xx" {|t|
  run zadd z xx 2 x; int 0
  run type z; str none
  run zadd z 1 x; int 1
  run zadd z xx 2 x; int 0
  run object encoding z; str $t.name
  run zscore z x; str 2
}

skiplist-and-listpack "zadd: nx" {|t|
  run zadd z nx 1 x; int 1
  run zscore z x; str 1
  run zadd z nx 2 x; int 0
  run object encoding z; str $t.name
  run zscore z x; str 1
}

skiplist-and-listpack "zadd: gt" {|t|
  run zadd z 1 a 2 b 3 c; int 3
  run zadd z gt 0 a 3 b 3 c; int 0
  run object encoding z; str $t.name
  run zscore z a; str 1
  run zscore z b; str 3
  run zscore z c; str 3
}

skiplist-and-listpack "zadd: lt" {|t|
  run zadd z 1 a 2 b 3 c; int 3
  run zadd z lt 0 a 3 b 3 c; int 0
  run object encoding z; str $t.name
  run zscore z a; str 0
  run zscore z b; str 2
  run zscore z c; str 3
}

skiplist-and-listpack "zadd: ch" {|t|
  run zadd z 0 a 1 b 2 c 3 d; int 4
  run zadd z ch 0 a 4 b 2 c; int 1
  run object encoding z; str $t.name
  run zrange z 0 "-1"; array [a c d b]
}

test "zadd: invalid" {
  run zadd z xx nx 1 x; err "ERR XX and NX options at the same time are not compatible"
  run zadd z gt lt 1 x; err "ERR GT, LT, and/or NX options at the same time are not compatible"
  run zadd z gt nx 1 x; err "ERR GT, LT, and/or NX options at the same time are not compatible"
  run zadd z nx lt 1 x; err "ERR GT, LT, and/or NX options at the same time are not compatible"
}

test "zadd: invalid score" {
  run zadd z 0 a 1 b 2 c invalid d; err "ERR value is not a valid float"
  run zcard z; int 0
}

test "zadd: ch includes added" {
  run zadd z 0 a 1 b; int 2
  run zadd z ch 0 a 2 b 3 c; int 2
  run zrange z 0 "-1"; array [a b c]
}

test "zadd: convert after max entries" {
  run config set zset-max-listpack-entries 1; ok
  run zadd z 1 a; int 1
  run object encoding z; str listpack
  run zadd z 2 b; int 1
  run object encoding z; str skiplist
}

test "zadd: convert after max value" {
  run config set zset-max-listpack-value 10; ok
  run zadd z 1 a; int 1
  run object encoding z; str listpack
  run zadd z 2 bbbbbbbbbbbb; int 1
  run object encoding z; str skiplist
}

test "zcard" {
  run zcard z; int 0
  run zadd z 1 x; int 1
  run zcard z; int 1
  run zadd z 2 y 3 z; int 2
  run zcard z; int 3
  run zrem z x; int 1
  run zcard z; int 2
}

test "zcount" {
  run zcount z 0 15; int 0
  run zadd z 1 a 2 b 3 c 4 d 5 e 6 f; int 6
  run zcount z 0 15; int 6
  run zcount z 1 5; int 5
  run zcount z 2 5; int 4
  run zcount z 4 4; int 1
}

test "zcount: bounds" {
  run zadd z 1 x 2 y 3 z; int 3
  run zcount z "-inf" +inf; int 3
  run zcount z +inf "-inf"; int 0
  run zcount z 1 3; int 3
  run zcount z "(1" 3; int 2
  run zcount z 1 "(3"; int 2
  run zcount z "(1" "(3"; int 1
}

test "zpopmin: wrong arguments" {
  run zpopmin; err "ERR wrong number of arguments for 'zpopmin' command"
}

test "zpopmin" {
  run zpopmin z; array []
  run zpopmin z 3; array []
  run zpopmin z "-3"; array []
  run zpopmin z invalid; err "ERR value is not an integer or out of range"
  run zpopmin z 3 5; err "ERR syntax error"
  run zadd z 0 a 1 b 2 c 3 d 4 e; int 5
  run zpopmin z 0; array []
  run zpopmin z "-3"; array []
  run zpopmin z; array [a "0"]
  run zrange z 0 "-1"; array [b c d e]
  run zpopmin z 1; array [b "1"]
  run zrange z 0 "-1"; array [c d e]
  run zpopmin z 2; array [c "2" d "3"]
  run zrange z 0 "-1"; array [e]
  run zpopmin z; array [e "4"]
  run type z; str none
}

test "zpopmin: resp3" {
  discard hello 3
  run zadd z 0 a 1 b 2 c 3 d 4 e; int 5
  run zpopmin z 0; array []
  run zpopmin z "-3"; array []
  run zpopmin z; array [a 0.0]
  run zpopmin z 1; array [[b 1.0]]
  run zpopmin z 2; array [[c 2.0] [d 3.0]]
}

test "zpopmax: wrong arguments" {
  run zpopmax; err "ERR wrong number of arguments for 'zpopmax' command"
}

test "zpopmax" {
  run zpopmax z; array []
  run zpopmax z 3; array []
  run zpopmax z "-3"; array []
  run zpopmax z invalid; err "ERR value is not an integer or out of range"
  run zpopmax z 3 5; err "ERR syntax error"
  run zadd z 0 a 1 b 2 c 3 d 4 e; int 5
  run zpopmax z 0; array []
  run zpopmax z "-3"; array []
  run zpopmax z; array [e "4"]
  run zrange z 0 "-1"; array [a b c d]
  run zpopmax z 1; array [d "3"]
  run zrange z 0 "-1"; array [a b c]
  run zpopmax z 2; array [c "2" b "1"]
  run zrange z 0 "-1"; array [a]
  run zpopmax z; array [a "0"]
  run type z; str none
}

test "zpopmax: resp3" {
  discard hello 3
  run zadd z 0 a 1 b 2 c 3 d 4 e; int 5
  run zpopmax z 0; array []
  run zpopmax z "-3"; array []
  run zpopmax z; array [e 4.0]
  run zpopmax z 1; array [[d 3.0]]
  run zpopmax z 2; array [[c 2.0] [b 1.0]]
}

test "bzpopmin: wrong arguments" {
  run bzpopmin; err "ERR wrong number of arguments for 'bzpopmin' command"
  run bzpopmin 2; err "ERR wrong number of arguments for 'bzpopmin' command"
}

test "bzpopmax: wrong arguments" {
  run bzpopmax; err "ERR wrong number of arguments for 'bzpopmax' command"
  run bzpopmax 2; err "ERR wrong number of arguments for 'bzpopmax' command"
}

test "bzpopmax: wrongtype" {
  client 1 {
    run bzpopmax wrong_type a 0
  }

  client 2 {
    await-flag 1 b
    run bzpopmax wrong_type a 0
  }

  client 3 {
    await-flag 2 b
    run set wrong_type foo; ok
    run zadd a 1 x 2 y 3 z; int 3
  }

  client 1 {
    err "WRONGTYPE Operation against a key holding the wrong kind of value"
    run ping; str PONG
  }

  client 2 {
    err "WRONGTYPE Operation against a key holding the wrong kind of value"
    run ping; str PONG
  }

  client 3 {
    run zrange a "-inf" +inf byscore withscores; array [x "1" y "2" z "3"]
  }
}

test "bzpopmin: wrongtype" {
  client 1 {
    run bzpopmin wrong_type a 0
  }

  client 2 {
    await-flag 1 b
    run bzpopmin wrong_type a 0
  }

  client 3 {
    await-flag 2 b
    run set wrong_type foo; ok
    run zadd a 1 x 2 y 3 z; int 3
  }

  client 1 {
    err "WRONGTYPE Operation against a key holding the wrong kind of value"
    run ping; str PONG
  }

  client 2 {
    err "WRONGTYPE Operation against a key holding the wrong kind of value"
    run ping; str PONG
  }

  client 3 {
    run zrange a "-inf" +inf byscore withscores; array [x "1" y "2" z "3"]
  }
}

skiplist-and-listpack "bzmpop: multiple" {|t|
  run zadd key 1 a 2 b 3 c 4 d; int 4
  run object encoding key; str $t.name
  run bzmpop 0 2 missing key min count 2; array [key [[a "1"] [b "2"]]]
  run zrange key "-inf" +inf byscore withscores; array [c "3" d "4"]
}

test "bzmpop: touch watched keys" {
  run zadd key 1 a 2 b 3 c; int 3
  touch key {
    run bzmpop 0 1 key min; array [key [[a "1"]]]
  }
}

test "bzmpop: blocking" {
  client 1 {
    run bzmpop 0 1 key min count 2
  }

  client 2 {
    await-flag 1 b
    run bzmpop 0 1 key min count 2
  }

  client 3 {
    await-flag 2 b
    run zadd key 1 x 2 y 3 z; int 3
    run zcard key; int 0
  }

  client 1 {
    array [key [[x "1"] [y "2"]]]
    run ping; str PONG
  }

  client 2 {
    array [key [[z "3"]]]
    run ping; str PONG
  }
}

test "bzmpop: exec multiple" {
  run zadd key 1 a 2 b 3 c 4 d; int 4
  run multi; ok
  run bzmpop 0 2 missing key min count 2; str QUEUED
  run exec; array [[key [[a "1"] [b "2"]]]]
  run zrange key "-inf" +inf byscore withscores; array [c "3" d "4"]
}

test "bzmpop: invalid numkeys" {
  run bzmpop 0 0 key min; err "ERR numkeys should be greater than 0"
  run bzmpop 0 invalid key min; err "ERR numkeys should be greater than 0"
  run bzmpop 0 100 key min; err "ERR syntax error"
}

test "bzmpop: invalid timeout" {
  run bzmpop invalid 1 key min; err "ERR timeout is not a float or out of range"
}

test "bzmpop: invalid count" {
  run bzmpop 0 1 key min count 0; err "ERR count should be greater than 0"
  run bzmpop 0 1 key min count invalid; err "ERR count should be greater than 0"
}

test "bzmpop: invalid option" {
  run bzmpop 0 1 key min invalid 3; err "ERR syntax error"
}

test "bzmpop: wrongtype" {
  run set key 1; ok
  run bzmpop 0 1 key min; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "bzmpop: exec missing" {
  run multi; ok
  run bzmpop 0 1 key min; str QUEUED
  run exec; array [null];
  run zadd b 1 x 2 y; int 2
  run bzmpop 0 2 a b min; array [b [[x "1"]]]
}

skiplist-and-listpack "bzmpop: less than count" {|t|
  run zadd a 1 x 2 y; int 2
  run object encoding a; str $t.name
  run bzmpop 0 1 a min count 100; array [a [[x "1"] [y "2"]]]
}

skiplist-and-listpack "bzmpop: more than count" {|t|
  run zadd a 1 x 2 y 3 z; int 3
  run object encoding a; str $t.name
  run bzmpop 0 1 a min count 2; array [a [[x "1"] [y "2"]]]
  run zrange a "-inf" +inf byscore withscores; array [z "3"]
}

skiplist-and-listpack "zrange: byscore" {|t|
  run zadd z 1 x 2 y 3 z; int 3
  run object encoding z; str $t.name

  run zrangebyscore z 0 5; array [x y z]
  run zrange z 0 5 byscore; array [x y z]
  run zrange z 0 5 byscore rev; array [z y x]
  run zrevrangebyscore z 0 5; array [z y x]

  run zadd z 4 x; int 0
  run zrangebyscore z 0 5; array [y z x]
  run zrange z 0 5 byscore; array [y z x]
}

skiplist-and-listpack "zrange: byscore bounds" {|t|
  run zadd z 1 x 2 y 3 z; int 3
  run object encoding z; str $t.name

  run zrangebyscore z "-inf" +inf; array [x y z]
  run zrange z "-inf" +inf byscore; array [x y z]

  run zrangebyscore z +inf "-inf"; array []
  run zrange z +inf "-inf" byscore; array []

  run zrangebyscore z 1 3; array [x y z]
  run zrange z 1 3 byscore; array [x y z]

  run zrangebyscore z "(1" 3; array [y z]
  run zrange z "(1" 3 byscore; array [y z]

  run zrangebyscore z 1 "(3"; array [x y]
  run zrange z 1 "(3" byscore; array [x y]

  run zrangebyscore z "(1" "(3"; array [y]
  run zrange z "(1" "(3" byscore; array [y]
}

skiplist-and-listpack "zrange: byscore nan" {|t|
  run zadd z 1 x 2 y 3 z; int 3
  run object encoding z; str $t.name
  run zrangebyscore z nan 5; err "ERR value is not a valid float"
  run zrange z 5 nan byscore; err "ERR value is not a valid float"
}

test "zrangebyscore: disallow by" {|t|
  run zrangebyscore z 0 5 bylex; err "ERR syntax error"
  run zrangebyscore z 0 5 byscore; err "ERR syntax error"
}

test "zrangebyscore: disallow rev" {|t|
  run zrangebyscore z 0 5 rev; err "ERR syntax error"
}

test "zrevrange: disallow limit" {|t|
  run zrevrange z 0 5 limit 0 2; err "ERR syntax error"
}

test "zrevrange: disallow by" {|t|
  run zrevrange z 0 5 bylex; err "ERR syntax error"
  run zrevrange z 0 5 byscore; err "ERR syntax error"
}

test "zrevrangebyscore: disallow by" {|t|
  run zrevrangebyscore z 0 5 bylex; err "ERR syntax error"
  run zrevrangebyscore z 0 5 byscore; err "ERR syntax error"
}

skiplist-and-listpack "zrangebyscore: withscores" {|t|
  run zadd z 1 x 2 y 3 z; int 3
  run object encoding z; str $t.name
  run zrangebyscore z 0 5 withscores; array [x "1" y "2" z "3"]
  run zrange z 0 5 byscore withscores; array [x "1" y "2" z "3"]
  discard hello 3
  run zrangebyscore z 0 5 withscores; array [x 1.0 y 2.0 z 3.0]
  run zrange z 0 5 byscore withscores; array [x 1.0 y 2.0 z 3.0]
}

skiplist-and-listpack "zrange: rev" {|t|
  run zadd x 1 a 2 b 3 c 4 d; int 4
  run object encoding x; str $t.name
  run zrange x 0 2 rev; array [c b a]
}

skiplist-and-listpack "zrange" {|t|
  run zrange x 0 1; array []
  run zadd x 1 a 2 b 3 c 4 d; int 4
  run object encoding x; str $t.name

  run zrange x 0 1; array [a b]
  run zrange x 0 1 rev; array [b a]
  run zrevrange x 0 1; array [b a]

  run zrange x 0 2; array [a b c]
  run zrange x 0 2 rev; array [c b a]
  run zrevrange x 0 2; array [c b a]

  run zrange x "-2" "-1"; array [c d]
  run zrange x "-2" "-1" rev; array [d c]
  run zrevrange x "-2" "-1"; array [d c]

  run zrange x 1 0; array []
  run zrange x 1 0 rev; array []
  run zrevrange x 1 0; array []

  run zrange x 0 invalid; err "ERR value is not an integer or out of range"
  run zrange x invalid 1; err "ERR value is not an integer or out of range"
}

test "zrange: by" {
  run zrange x 0 "-1" byscore bylex; err "ERR syntax error"
  run zrange x 0 "-1" bylex byscore; err "ERR syntax error"
}

skiplist-and-listpack "zrange: withscores" {|t|
  run zadd x 1 a 2 b 3 c 4 d; int 4
  run object encoding x; str $t.name
  run zrange x 0 1 withscores; array [a "1" b "2"]
  discard hello 3
  run zrange x 0 1 withscores; array [a 1.0 b 2.0]
}

test "zrange: limit without byscore/bylex" {
  run zrange x 0 "-1" limit 0 2; err "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
}

skiplist-and-listpack "zrangebyscore: limit" {|t|
  run zadd x 1 a 2 b 3 c 4 d; int 4
  run object encoding x; str $t.name
  run zrangebyscore x 1 5 limit 0 2; array [a b]
  run zrange x 1 5 byscore limit 0 2; array [a b]
  run zrangebyscore x 1 5 limit 0 2 withscores; array [a "1" b "2"]
  run zrange x 1 5 byscore limit 0 2 withscores; array [a "1" b "2"]
  run zrangebyscore x 1 5 limit 1 2; array [b c]
  run zrange x 1 5 byscore limit 1 2; array [b c]
  run zrangebyscore x 1 5 limit 1 5; [b c d]
  run zrange x 1 5 byscore limit 1 5; array [b c d]
}

skiplist-and-listpack "zrem" {|t|
  run zadd z 1 x 2 y 3 z; int 3
  run object encoding z; str $t.name
  run zrange z 0 "-1"; array [x y z]
  run zrem z x y; int 2
  run zrange z 0 "-1"; array [z]
  run zrem z x y z; int 1
  run zrange z 0 "-1"; array []
  run type z; str none
}

skiplist-and-listpack "zremrangebyscore" {|t|
  run zadd z 0 a 1 b 2 c 3 d 4 e 5 f 6 g; int 7
  run object encoding z; str $t.name
  run zremrangebyscore z 2 5; int 4
  run zrange z 0 "-1"; array [a b g]
  run zremrangebyscore z "-inf" +inf; int 3
  run type z; str none
}

skiplist-and-listpack "zscore" {|t|
  run zscore x a; nil
  run zadd x 1 a; int 1
  run object encoding x; str $t.name
  run zscore x b; nil
  run zscore x a; str 1
}

skiplist-and-listpack "zrank" {|t|
  run set x 1; ok
  run zrank x a; err "WRONGTYPE Operation against a key holding the wrong kind of value"
  run zrank z a; nil
  run zadd z 0 a 1 b 2 c 3 d 4 e 5 f; int 6
  run object encoding z; str $t.name
  run zrank z a; int 0
  run zrank z c; int 2
  run zrank z f; int 5
}

skiplist-and-listpack "bzpopmax: exec" {|t|
  run multi; ok
  run bzpopmax a 0; str QUEUED
  run exec; array [null]
}

skiplist-and-listpack "bzpopmin: exec" {|t|
  run multi; ok
  run bzpopmin a 0; str QUEUED
  run exec; array [null]
}

skiplist-and-listpack "bzpopmax: remove empty" {|t|
  run zadd key 1 a 2 b 3 c; int 3
  run object encoding key; str $t.name
  run bzpopmax key 0; array [key c "3"]
  run type key; str zset
  run bzpopmax key 0; array [key b "2"]
  run type key; str zset
  run bzpopmax key 0; array [key a "1"]
  run type key; str none
}

skiplist-and-listpack "bzpopmin: remove empty" {|t|
  run zadd key 1 a 2 b 3 c; int 3
  run object encoding key; str $t.name
  run bzpopmin key 0; array [key a "1"]
  run type key; str zset
  run bzpopmin key 0; array [key b "2"]
  run type key; str zset
  run bzpopmin key 0; array [key c "3"]
  run type key; str none
}

skiplist-and-listpack "bzpopmax: multiple keys" {|t|
  run zadd key 1 a 2 b 3 c; int 3
  run object encoding key; str $t.name
  run bzpopmax empty key 0; array [key c "3"]
}

skiplist-and-listpack "bzpopmin: multiple keys" {|t|
  run zadd key 1 a 2 b 3 c; int 3
  run object encoding key; str $t.name
  run bzpopmin empty key 0; array [key a "1"]
}

test "bzpopmax: wrong type" {
  run set wrong asdf; ok
  run bzpopmax wrong key 0; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "bzpopmin: wrong type" {
  run set wrong asdf; ok
  run bzpopmin wrong key 0; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

test "bzpopmax: touch" {
  run zadd key 1 a 2 b 3 c; int 3
  touch key { run bzpopmax key 0; array [key c "3"] }
}

test "bzpopmin: touch" {
  run zadd key 1 a 2 b 3 c; int 3
  touch key { run bzpopmin key 0; array [key a "1"] }
}

skiplist-and-listpack "zmpop: multiple" {|t|
  run zadd key 1 a 2 b 3 c 4 d; int 4
  run object encoding key; str $t.name
  run zmpop 2 missing key min count 2; array [key [[a "1"] [b "2"]]]
  run zrange key "-inf" +inf byscore withscores; array [c "3" d "4"]
}

test "zmpop: invalid numkeys" {
  run zmpop 0 key min; err "ERR numkeys should be greater than 0"
  run zmpop invalid key min; err "ERR numkeys should be greater than 0"
  run zmpop 100 key min; err "ERR syntax error"
}

test "zmpop: invalid count" {
  run zmpop 1 key min count 0; err "ERR count should be greater than 0"
  run zmpop 1 key min count invalid; err "ERR count should be greater than 0"
}

test "zmpop: invalid option" {
  run zmpop 1 key min invalid 3; err "ERR syntax error"
}

test "zmpop: wrong type" {
  run set key 1; ok
  run zmpop 1 key min; err "WRONGTYPE Operation against a key holding the wrong kind of value"
}

skiplist-and-listpack "zmpop: missing" {|t|
  run zmpop 1 a min; nil
  run zadd b 1 x 2 y; int 2
  run object encoding b; str $t.name
  run zmpop 2 a b min; array [b [[x "1"]]]
}

skiplist-and-listpack "zmpop: less than count" {|t|
  run zadd a 1 x 2 y; int 2
  run object encoding a; str $t.name
  run zmpop 1 a min count 100; array [a [[x "1"] [y "2"]]]
}

skiplist-and-listpack "zmpop: more than count" {|t|
  run zadd a 1 x 2 y 3 z; int 3
  run zmpop 1 a min count 2; array [a [[x "1"] [y "2"]]]
  run zrange a "-inf" +inf byscore withscores; array [z "3"]
}

test "zmpop: touch" {
  run zadd key 1 a 2 b 3 c; int 3
  touch key { run zmpop 1 key min; array [key [[a "1"]]] }
}
