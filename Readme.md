# lodex

`lodex` is probably the simplest key-value datastore. Key-value pairs are
appended to the database file along with the blocks of the trie-based index that
keep track of where everything is. Only the first couple of bytes of the file are
mutable, and this is where we keep the checkpoints. `lodex` keeps two
checkpoints, which might seem meaningless at the moment, but might make more
sense in the future.

## Getting started

Install `lodex` by adding it to your path. [python3](https://www.python.org) is a prerequisite.

```bash
❯ cp lodex ~/bin
```

## Running the tests

Call `test_lodex.sh` to run the tests.

```bash
❯ sh test_lodex.sh
Testing lodex database CLI
ok: "testvalue == testvalue"
ok: "0 == 0"
ok: "127 == 127"
ok: "1 == 1"
ok: "testkey:testvalue, == testkey:testvalue,"
ok: "0 == 0"
ok: "2 == 2"
ok: "another_testkey:testvalue,testkey:testvalue, == another_testkey:testvalue,testkey:testvalue,"
ok: "0 == 0"
ok: "1 == 1"
ok: "testkey:testvalue, == testkey:testvalue,"
ok: "127 == 127"
ok: "127 == 127"
ok: "testkey:testvalue, == testkey:testvalue,"

```

## Usage

Like most mammals, `lodex` comes with six basic operations, `put`, `get`,
`delete`, `stats`, `dump`, and `load`. If no database path is specified,
`lodex` will just use `./database.ldx`.

```bash
❯ lodex put mykey myvalue
❯ lodex get mykey
myvalue

❯ lodex stats
db:	database.ldx
items:	1
size:	201

❯ lodex dump | lodex load --db database2.ldx
❯ lodex delete mykey
❯ lodex stats       
db:	database.ldx
items:	0
size:	312

❯ lodex stats --db database2.ldx 
db:	database2.ldx
items:	1
size:	201

```

## Todos

I'm going to clean up the internal API so that you can use `lodex` in your
python programs as well. So far, I've just worked on the command line
interface.

I'm also going to add some error checking to be able to truncate the database
in case of aborted transactions. These days the database will just continue
appending on top existing-but-unavailable key-value pairs if a transaction is
aborted. Which is fine, but not optimal.

Also, write access needs to be locked down. There are several ways of doing this,
but I need to figure out which way is simplest. `lodex` is all about being
simplest.

## Contributing

Contributions would be sweet, although not necessary, since `lodex` is perfect.

No, just kidding.

All contributions are welcome, especially the ones that reduce LOC without
sacrificing features or readability. `lodex` has no ambition of obsoleting any
real, grown-up datastores. No, on the contrary, `lodex` is perfectly happy just
being an educational tool.

## Authors

Stian Lode [github](https://github.com/slode)

# License

`lodex` is licensed under the MIT License - see the [LICENSE.md](LICENSE.md)
file for details
