# Covenant[^1], an embedded bytes-to-bytes key-value store

[^1]: While the actual key-value store here is split into the two components `phobos` and `seqstore`, the main demo is
called `covenant`. So, technically, the _key-value_ store itself is not called "covenant", but it has a nicer name
than "phobos + seqstore", so that's what the repo's called.

This is an experimental embedded key-value store written in Rust (_experimental_ meaning that you probably shouldn't
be using this to store mission-critical data).

The idea is to use [FST](https://crates.io/crates/fst)s[^2] as a bytes-to-integer map, where the integer values
correspond (roughly) to file offsets in a memory-mapped sequential store. Throw in automatic file handling and FST
merging, additional checks to ensure invalid/outdated internal IDs do not cause issues, careful ordering of operations
to ensure durability etc., and then you have a damn speedy storage.

[^2]: If you haven't read the `fst` crate's accompanying [blog post](https://blog.burntsushi.net/transducers/),
I _highly_ recommend giving it a go. It not only walks you through the underlying
logic in an understandable way, but also gives a rough performance report on
indexing more than a billion keys - spoiler, it's _fast_.

The goal of this KV store is to create a file indexer (and **possibly** some form of FUSE filesystem) that allows for
efficient lookups of files by their hashes with any number of hash algorithms. Notably, as there are no restrictions on
keys, using a perceptual hash would be possible for, say, detecting probable image duplicates.

## Repo overview

- `phobos/`
    - Handles insertion, querying, updating and (eventually) deleting key->internal ID
      associations
    - Manages FSTs on disk and auto-merges them when
      necessary
- `seqstore/`
    - Handles the internal ID->value associations
    - On inserts, attempts to use previously-freed space to minimise unneeded growth
- `int-multistore/`
    - A wrapper around `seqstore` that stores (potentially) multiple integers as values
      in a storage-efficient manner
- `covenant/`
    - The main file-hashing store (so called because it helps to maintain an *Ark*ive...)

## Comparison with sqlite

* sqlite supports full relational DB access - tables, joins, complex query expressions - while covenant is only[^3] a
  key-value store.
* Based on the benchmarks in [the `bench/` dir](./bench/), covenant seems ot be significantly faster than sqlite, at
  least where keys are variable-length random byte sequences and values are sets of integers (i.e. hash-to-ID lookup).
* sqlite uses two files (main DB and WAL/recovery DB), while covenant uses three + $log_K(keys)$ files.[^4]
* Both sqlite and covenant are fully embedded and thus do not require a separate process or network connections.
* sqlite is written in C, covenant is written in Rust.
    * Given how common C ABI bindings are, sqlite can be used from many programming languages.
    * C header files could be generated for covenant, but are omitted for now.
* sqlite is very much battle-tested and proven to be reliable, covenant is entirely experimental.

[^3]: The fst crate supports [arbitrary automata](https://docs.rs/fst/latest/fst/automaton/trait.Automaton.html), so
more complex key lookups (by Levenshtein distance or range queries, for example) are theoretically possible. This,
however, does still not make covenant a relational DB.

[^4]: `phobos` rolls up multiple FSTs into a larger one as they gain more keys in a logarithmic fashion - here, $K$ is
some arbitrary large integer. Additionally, there is a compaction action that merges all FSTs into a single one; thus,
maintenance can lower covenant to using only four files.
