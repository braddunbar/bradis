use bradis *

test "eval" {
  run eval "return 1 + 2" 0; int 3
}

test "eval: nil" {
  run eval "return nil" 0; nil
}

test "eval: getkeys" {
  run command getkeys eval "return 1" invalid a b c d; err "ERR Invalid arguments specified for command"
  run command getkeys eval "return 1" 2 a; err "ERR Invalid arguments specified for command"
  run command getkeys eval "return 1" 2 a b c d; array [a b]
  run command getkeys eval "return 1" 2 a b; array [a b]
  run command getkeys eval "return 1" 0; array []
}
