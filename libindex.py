#!/usr/bin/env python3

import cbor
import sys
import os

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
    def __init__(self, index_block=None):
        self.index_block = index_block if index_block is not None else {}

    def has(self, key_frag):
        return key_frag in self.index_block

    def put(self, key_frag, value, typ):
        self.index_block[key_frag] = (value, typ)

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
            self.write_checkpoint(self.put(IndexBlock().index_block))

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
        value_bytes = cbor.dumps(value)
        length = len(value_bytes)
        self.file.write(length.to_bytes(2, byteorder="big"))
        self.file.write(value_bytes)
        return offset

    def get(self, offset):
        self.file.seek(offset, 0)
        length = int.from_bytes(self.file.read(2), byteorder="big")
        value_bytes = self.file.read(length)
        return cbor.loads(value_bytes) if value_bytes else None

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


class TestIndex:
    def __init__(self, filename, value_log, key="_id", multi=False):
        self.log = FileLog(filename+"."+key)
        self.index_key = key
        self.root = IndexBlock(index_block=IndexBlock())
        self.in_memory_blocks = DirtyBlocks()

    def put(self, key, value):
        block = self.root
        
        
        for subkey in split_by_n(key, 2):
            if block.has(subkey) is False:
                block.put(subkey, value, 0)
                return
            
            entry = block.get(subkey)
            if entry[1] == 0:
                if entry[0] is None:
                    block.put(subkey, value, 0)
                    return
                old_record = self.value_log.get(entry[0])
                if old_record[self.index_key] == key:
                    block.put(subkey, value, 0)
                    return
                else:
                    index = self.in_memory_blocks.put(IndexBlock())
                    block.put(subkey, index, 2)
                    self.put(old_record[self.index_key], entry[0])
                    self.put(key, value)
                    return
            elif entry[1] == 1:
                child_block = self.in_memory_blocks.get(entry[0])
                block = child_block
                continue

        # NODE is also LEAF
        block.put("", value, 0)

