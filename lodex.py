#!/usr/bin/env python3

import argparse
import pickle
import sys

from enum import Enum
Type = Enum("Type", "LEAF NODE MEM_NODE ROOT_NODE")

class DirtyBlocks:
    def __init__(self):
        self.log = []

    def put(self, value):
        self.log.append(value)
        return len(self.log) - 1

    def get(self, offset):
        return self.log[offset]

    def get_last(self):
        return self.log[-1]

    def __len__(self):
        return len(self.log)

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

def split_by_n(seq, n):
    while seq:
        yield seq[:n]
        seq = seq[n:]

class LogIndex:
    def __init__(self, log, root_offset):
        self.log = log
        self.root = log.get(root_offset)
        self.in_memory_blocks = DirtyBlocks()

    def walk(self, callback, internal_nodes=False):
        def rec_do(block, depth):
            for subkey in block.keys():
                entry = block.get(subkey)
                #print("    " * depth + subkey + ": " + str(entry))
                if entry[2] == Type.NODE:
                    rec_do(self.log.get(entry[1]), depth + 1)
                    if internal_nodes:
                        callback(entry[0], entry[1])
                elif entry[2] == Type.MEM_NODE:
                    rec_do(self.in_memory_blocks.get(entry[1]), depth + 1)
                    if internal_nodes:
                        callback(entry[0], entry[1])
                elif entry[2] == Type.LEAF:
                    callback(entry[0], entry[1])
                else:
                    raise ValueError("Failure to walk structure")
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
                raise ValueError("Should not happen. Illegal element type.")

        # NODE is also LEAF
        block.put("", key, value, Type.LEAF)

    def get(self, key):
        block = self.root
        for subkey in split_by_n(key, 2):
            if block.has(subkey):
                entry = block.get(subkey)
                entry_type = entry[2]
                if entry_type == Type.LEAF: # LEAF
                    if entry[0] == key:
                        return entry[1]
                    raise KeyError(key + " not found")
                elif entry_type == Type.NODE: # directory
                    block_offset = block.get(subkey)
                    block = self.log.get(block_offset[1])
                elif entry_type == Type.MEM_NODE:
                    block_offset = block.get(subkey)
                    block = self.in_memory_blocks.get(block_offset[1])
                else:
                    assert False
            else:
                raise KeyError(key + " not found")

        # If NODE is also LEAF
        if block.has(""):
            entry = block.get("")
            if entry[0] == key:
                return entry[1]
        raise KeyError(key + " not found")

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
        self.in_memory_blocks = DirtyBlocks()
        assert self.log.read_checkpoint() == root_offset
        return root_offset

def main():
    main_parser = argparse.ArgumentParser(description='Lodex database API.')
    main_parser.add_argument(
            "--db",
            type=str,
            default="database.ldx",
            help="Database filename.")
    sub_parsers = main_parser.add_subparsers(dest='operation')
    put_parser = sub_parsers.add_parser("put")
    put_parser.add_argument("key")
    put_parser.add_argument("value")
    get_parser = sub_parsers.add_parser("get")
    get_parser.add_argument("key")
    del_parser = sub_parsers.add_parser("delete")
    del_parser.add_argument("key")
    status_parser = sub_parsers.add_parser("status")
    dump_parser = sub_parsers.add_parser("dump")
    dump_parser.add_argument(
            "--sep",
            type=str,
            default="\t",
            help="key-value separator.")
    load_parser = sub_parsers.add_parser("load")
    load_parser.add_argument(
            "--sep",
            type=str,
            default="\t",
            help="key-value separator.")

    args = main_parser.parse_args()

    if args.db and args.operation:
        log = FileLog(args.db)
        index = LogIndex(log, log.read_checkpoint())

        if args.operation == "put":
            log_offset = log.put(args.value)
            print(log_offset)
            index.put(args.key, log.put(args.value))
            index.commit()
        elif args.operation == "get":
            try:
                print(log.get(index.get(args.key)))
            except KeyError:
                sys.stderr.write("Unable to find item '{}'".format(args.key))
                exit(-1)
        elif args.operation == "delete":
            index.put(args.key, None)
            index.commit()
        elif args.operation == "status":
            counter = 0
            def item_counter(_1, _2):
                counter += 1
            index.walk(item_counter)
            print("Stats for {}".format(log.filename))
            print(" items: {}".format(counter))
            print(" size: {}".format(len(log)))
        elif args.operation == "dump":
            def item_printer(key, value):
                print("{}{}{}".format(key, args.sep, value))
            index.walk(item_printer)
        elif args.operation == "load":
            try:
                for line in sys.stdin.readline():
                    key, value = line.split(args.sep)
                    index.put(key, log.put(value))
                index.commit()
            except:
                sys.stderr.write("Failed to load values.")
                exit(-1)
        log.close()

if __name__ == "__main__":
    main()

