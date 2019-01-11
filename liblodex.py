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


class LogIndex:
    def __init__(self, filename, value_log, key="_id"):
        self.log = FileLog(filename+"."+key)
        self.value_log = value_log
        self.index_key = key
        self.reset()

    def reset(self):
        self.root = IndexBlock(index_block=self.log.get(self.log.read_checkpoint()))
        self.in_memory_blocks = DirtyBlocks()

    def walk(self, callback):
        def rec_do(block, depth):
            for subkey in block.keys():
                entry = block.get(subkey)
                if entry[1] == 1:
                    rec_do(IndexBlock(index_block=self.log.get(entry[0])), depth + 1)
                elif entry[1] == 2:
                    rec_do(self.in_memory_blocks.get(entry[0]), depth + 1)
                elif entry[1] == 0 and entry[0] is not None:
                    record = self.value_log.get(entry[0])
                    callback(record)
        return rec_do(self.root, 0)

    def put(self, key, value):
        block = self.root
        self.in_memory_blocks.put(block)
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
            # Just traversal
            elif entry[1] == 1:
                child_block = IndexBlock(index_block=self.log.get(entry[0]))
                child_block_index = self.in_memory_blocks.put(child_block)
                block.put(subkey, child_block_index, 2)
                block = child_block
                continue
            elif entry[1] == 2:
                child_block = self.in_memory_blocks.get(entry[0])
                block = child_block
                continue

        # NODE is also LEAF
        block.put("", value, 0)

    def get(self, key):
        block = self.root
        for subkey in split_by_n(key, 2):
            if block.has(subkey):
                entry = block.get(subkey)
                entry_type = entry[1]
                if entry_type == 0:
                    if entry[0] is not None:
                        record = self.value_log.get(entry[0])
                        if record[self.index_key] == key:
                            return entry[0]
                    raise KeyError("Key '{}' not found".format(key))
                elif entry_type == 1:
                    block = IndexBlock(index_block=self.log.get(entry[0]))
                elif entry_type == 2:
                    block = self.in_memory_blocks.get(entry[0])
            else:
                raise KeyError("Key '{}' not found".format(key))

        # If NODE is also LEAF
        if block.has(""):
            entry = block.get("")
            record = self.value_log.get(entry[0])
            if record[self.index_key] == key and entry[0] is not None:
                return entry[0]

        raise KeyError("Key '{}' not found".format(key))

    def commit(self):
        if not len(self.in_memory_blocks):
            return None

        def commit_rec(block):
            for subkey in block.keys():
                entry = block.get(subkey)
                if entry[1] == 2:
                    block_id = commit_rec(self.in_memory_blocks.get(entry[0]))
                    block.put(subkey, block_id, 1)
            return self.log.put(block.index_block)

        root_offset = commit_rec(self.root)
        self.log.write_checkpoint(root_offset)
        self.reset()

class Lodex:
    def __init__(self, filename="database.ldx"):
        self.filename = os.path.realpath(filename)
        self.log = FileLog(self.filename)
        self.indices = {}
        self.add_index("_id")
        for p in os.listdir(os.path.dirname(self.filename)):
            if p.startswith(filename+"."):
                self.add_index(os.path.splitext(p)[1][1:])

    def add_index(self, key):
        self.indices[key]= LogIndex(self.filename, self.log, key=key)

    def put(self, doc):
        if "_id" not in doc:
            import uuid
            doc["_id"] = uuid.uuid4().hex
        offset = self.log.put(doc)
        for index_key in self.indices:
            if index_key in doc:
                print(index_key, doc)
                self.indices[index_key].put(doc[index_key], offset)
        return doc["_id"]

    def get(self, doc):
        for index_key in self.indices:
            if index_key in doc:
                offset = self.indices[index_key].get(doc[index_key])
        return self.log.get(offset)

    def delete(self, doc):
        for index_key in self.indices:
            if index_key in doc:
                offset = self.indices[index_key].put(doc[index_key], None)

    def walk(self, cb):
        self.indices["_id"].walk(cb)

    def commit(self):
        for index_key in self.indices:
            self.indices[index_key].commit()

    def __len__(self):
        return len(self.log)

    def close(self):
        self.log.close()

