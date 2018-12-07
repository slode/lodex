#!/usr/bin/env python3

import argparse
import pickle
import sys

from enum import Enum
Type = Enum("Type", "LEAF NODE MEM_NODE")

class DirtyBlocks:
    def __init__(self):
        self.dirty_blocks = []

    def put(self, value):
        self.dirty_blocks.append(value)
        return len(self.dirty_blocks) - 1

    def get(self, offset):
        return self.dirty_blocks[offset]

    def __len__(self):
        return len(self.dirty_blocks)

class IndexBlock:
    def __init__(self):
        self.index_block = {}

    def has(self, key_frag):
        return key_frag in self.index_block

    def put(self, key_frag, key, value, typ):
        self.index_block[key_frag] = (key, value, typ)

    def get(self, key_frag):
        return self.index_block[key_frag]

    def keys(self):
        return sorted(self.index_block.keys())

class FileLog:
    def __init__(self, filename):
        self.filename = filename
        try:
            self.file = open(filename, "rb+")
        except IOError:
            self.file = open(filename, "wb+")

        if len(self) == 0:
            self.write_checkpoint(0)
            self.write_checkpoint(self.put(IndexBlock()))

    def write_checkpoint(self, offset):
        self.file.seek(0, 0)
        self.file.write(offset.to_bytes(4, byteorder="big"))
        self.file.write(offset.to_bytes(4, byteorder="big"))
        self.file.flush()

    def read_checkpoint(self):
        self.file.seek(0, 0)
        offset = int.from_bytes(self.file.read(4), byteorder="big")
        offset2 = int.from_bytes(self.file.read(4), byteorder="big")
        assert(offset == offset2)
        return offset

    def put(self, value):
        offset = len(self)
        value_bytes = pickle.dumps(value)
        length = len(value_bytes)
        self.file.write(length.to_bytes(4, byteorder="big"))
        self.file.write(value_bytes)
        return offset
    
    def get(self, offset):
        self.file.seek(offset, 0)
        length = int.from_bytes(self.file.read(4), byteorder="big")
        value_bytes = self.file.read(length)
        return pickle.loads(value_bytes)

    def __len__(self):
        self.file.seek(0, 2)
        return self.file.tell()
    
    def close(self):
        self.file.close()
        self.file = None

def split_by_n(seq, n):
    while seq:
        yield seq[:n]
        seq = seq[n:]

class LogIndex:
    def __init__(self, log):
        self.log = log
        self.reset()

    def reset(self):
        self.root = self.log.get(self.log.read_checkpoint())
        self.in_memory_blocks = DirtyBlocks()

    def walk(self, callback):
        def rec_do(block, depth):
            for subkey in block.keys():
                entry = block.get(subkey)
                if entry[2] == Type.NODE:
                    rec_do(self.log.get(entry[1]), depth + 1)
                elif entry[2] == Type.MEM_NODE:
                    rec_do(self.in_memory_blocks.get(entry[1]), depth + 1)
                elif entry[2] == Type.LEAF:
                    if entry[1] is not None:
                        callback(entry[0], entry[1])
                else:
                    raise ValueError("Illegal element type.")
        return rec_do(self.root, 0)

    def put(self, key, value):
        block = self.root
        self.in_memory_blocks.put(block)
        for subkey in split_by_n(key, 2):
            if block.has(subkey) == False:
                block.put(subkey, key, value, Type.LEAF)
                return
            elif block.get(subkey)[2] == Type.NODE:
                child_block = self.log.get(block.get(subkey)[1])
                child_block_index = self.in_memory_blocks.put(child_block)
                block.put(subkey, None, child_block_index, Type.MEM_NODE)
                block = child_block
                continue
            elif block.get(subkey)[2] == Type.MEM_NODE:
                child_block = self.in_memory_blocks.get(block.get(subkey)[1])
                block = child_block
                continue
            elif block.get(subkey)[2] == Type.LEAF:
                old_entry = block.get(subkey)
                if old_entry[0] == key:
                    block.put(subkey, key, value, Type.LEAF)
                    return
                else:
                    new_block = IndexBlock()
                    index = self.in_memory_blocks.put(new_block)
                    block.put(subkey, None, index, Type.MEM_NODE)
                    self.put(old_entry[0], old_entry[1])
                    self.put(key, value)
                    return
            else:
                raise ValueError("Illegal element type.")

        # NODE is also LEAF
        block.put("", key, value, Type.LEAF)

    def get(self, key):
        block = self.root
        for subkey in split_by_n(key, 2):
            if block.has(subkey):
                entry = block.get(subkey)
                entry_type = entry[2]
                if entry_type == Type.LEAF:
                    if entry[0] == key and entry[1] is not None:
                        return entry[1]
                    raise KeyError("Key '{}' not found".format(key))
                elif entry_type == Type.NODE:
                    block_offset = block.get(subkey)
                    block = self.log.get(block_offset[1])
                elif entry_type == Type.MEM_NODE:
                    block_offset = block.get(subkey)
                    block = self.in_memory_blocks.get(block_offset[1])
                else:
                    raise ValueError("Illegal element type.")
            else:
                raise KeyError("Key '{}' not found".format(key))

        # If NODE is also LEAF
        if block.has(""):
            entry = block.get("")
            if entry[0] == key and entry[1] is not None:
                return entry[1]

        raise KeyError("Key '{}' not found".format(key))

    def commit(self):
        if not len(self.in_memory_blocks):
            return None

        def commit_rec(block):
            for subkey in block.keys():
                entry = block.get(subkey)
                if entry[2] == Type.MEM_NODE:
                    block_id = commit_rec(self.in_memory_blocks.get(entry[1]))
                    block.put(subkey, entry[0], block_id, Type.NODE)
            return self.log.put(block)

        root_offset = commit_rec(self.root)
        self.log.write_checkpoint(root_offset)
        self.reset()


db_parser = argparse.ArgumentParser(add_help=False)
db_parser.add_argument(
        "--db", type=str, default="database.ldx", metavar="path",
        help="the database path (default 'database.ldx')")
key_parser = argparse.ArgumentParser(add_help=False)
key_parser.add_argument("key")
value_parser = argparse.ArgumentParser(add_help=False)
value_parser.add_argument("value")
sep_parser = argparse.ArgumentParser(add_help=False)
sep_parser.add_argument(
        "--sep", type=str, default="\t",
        help="key-value separator (default: '\\t')")

parser = argparse.ArgumentParser(
        description='An api interfacing a ldx database.')
sub_parsers = parser.add_subparsers(dest="operation")
_ = sub_parsers.add_parser(
        "put", help="add key-value pair",
        parents=[db_parser, key_parser, value_parser])
_ = sub_parsers.add_parser(
        "get", help="retrieve a value",
        parents=[db_parser, key_parser])
_ = sub_parsers.add_parser(
        "delete", help="delete a value",
        parents=[db_parser, key_parser])
_ = sub_parsers.add_parser(
        "stats", help="print database metrics",
        parents=[db_parser])
_ = sub_parsers.add_parser(
        "dump", help="print key-values to stdout",
        parents=[db_parser, sep_parser])
_ = sub_parsers.add_parser(
        "load", help="add key-value pairs from stdout",
        parents=[db_parser, sep_parser])

args = parser.parse_args()

if args.operation:
    log = FileLog(args.db)
    index = LogIndex(log)
    try:
        if args.operation == "put":
            index.put(args.key, log.put(args.value))
            index.commit()
        elif args.operation == "get":
            print(log.get(index.get(args.key)))
        elif args.operation == "delete":
            index.put(args.key, None)
            index.commit()
        elif args.operation == "stats":
            counter = [0]
            def item_counter(_1, _2):
                counter[0] += 1
            index.walk(item_counter)
            print("db:\t{}\nitems:\t{}\nsize:\t{}".format(
                log.filename, counter[0], len(log)))
        elif args.operation == "dump":
            sep = args.sep.encode("utf-8").decode("unicode_escape")
            def item_printer(key, value):
                print("{}{}{}".format(key, sep, log.get(value)))
            index.walk(item_printer)
        elif args.operation == "load":
            sep = args.sep.encode("utf-8").decode("unicode_escape")
            for line in sys.stdin:
                key, value = line.split(sep)
                index.put(key, log.put(value.rstrip()))
            index.commit()
    except BaseException as e:
        sys.stderr.write(repr(e))
        exit(-1)
    finally:
        log.close()
else:
    parser.print_help()
