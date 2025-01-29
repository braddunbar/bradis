## Bradis

Bradis is a partial implementation of Redis in Rust as a learning exercise.
It's a serious attempt at recreating Redis functionality, but not intended
for serious use.

## Goals

### Idiomatic Rust

The number one goal of this project is to learn how to implement Redis
with Rust. Writing readable, idiomatic, documented Rust comes before all
other goals.

### Redis data structures

The second goal is to learn about Redis data structures and how they work
together to create a solution to real problems. Some of the things I've
implemented might be available as off the shelf crates, but writing them
myself is the whole point of this exercise, so I'm doing that!

### Idiomatic async Rust

Reading requests and writing responses is a great opportunity for using
the asynchronous features of Rust. Learning to implement parallel processing
of requests and responses is difficult and information about strategies for
doing so are few and far between. I'd like to come up with my own ideas about
it!

### Match the latest Redis release

My target for functionality is generally the latest Redis release. This
project will always lag behind, but that's what I'm shooting for.

### Iteratively improve performance

Measuring, profiling, and improving the performance of a code path is
difficult and requires many tools. Making that loop tighter is an explicit
goal of mine here.

## Non-Goals

### Implement all of Redis

Some parts of Redis are more interesting to me than others. I'll be doing
all the parts that I'm interested in and not the others.

### Contribute to C Redis

I have no desire to replace or contribute to C Redis. This is about writing
Rust, not C.

### Performance at the cost of understanding

I'd like to achieve performance comparable to C Redis, but only if it's easily
understandable and idiomatic. I don't care about squeezing out every tiny bit
of performance.

## Architecture

Redis is often described as being "single threaded", referring to the way
commands are processed sequentially. However, there is a great deal of work
that can be done in parallel. Currently, I've split that work roughly into
the following tasks.

### Waiting for connections

The `Server` waits for connections and spawns new clients.

### Reading commands

Each `Client` spawns a task for reading commands. It waits for bytes to arrive,
parses them into arguments, and sends them through a channel along with
separators to denote command boundaries.

### Writing responses

Each `Client` also spawns a `Replier` to accept replies through a channel
and write them to a socket one at a time.

### Waiting for commands

The `Client` needs to wait for a full command to arrive before sending a
ready message to the store. Also, if the client is killed it should stop
waiting, send a disconnect message to the server, stop reading commands,
and drop itself. The replier will be dropped as soon as all replies have
been sent.

### Freeing memory

Redis can free memory asynchronously (e.g. `UNLINK`) and we need a task
to accept values and free them on a separate thread.

### Procesing commands

Commands are processed one at a time by sending clients that are ready to
a channel that owns the store and running them one at a time.
