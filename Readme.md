# lodex

`lodex` is probably the simplest key-value datastore. Both the key-value pairs
and the blocks of the trie based index are appended to the database file.

## Getting started

Install `lodex` by adding it to your path. [python3](https://www.python.org) is a prerequisite.

```
cp lodex ~/bin
```

## Running the tests

Call test_lodex.sh to run the tests.

```
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

`lodex` comes with six basic operations, `put`, `get`, `delete`, `stats`, `dump`,
and `load`. If no database path is specified, `lodex` will use `./database.ldx`.

```
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

❯ ./lodex stats --db database2.ldx 
db:	database2.ldx
items:	1
size:	201

```

## Todos

I'm going to clean up the internal API so that you can use `lodex` in your
python programs as well. At the moment, I've just worked on the CLI.

## Contributing

Contributions would be sweet, although not necessary, since `lodex` is perfect.

No, just kidding.

All contributions are welcome, especially the ones that
reduce LOC without sacrificing feature or readability. Ideally, `lodex` will be
used for educational purposes.

## Authors

Stian Lode [github](https://github.com/slode)

# License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
