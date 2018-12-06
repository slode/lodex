import copy

class MemLog:
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

class Log:
    def __init__(self):
        self.log = []

    def put(self, value):
        self.log.append(value)
        return len(self.log) - 1

    def get(self, offset):
        return copy.deepcopy(self.log[offset])

    def get_last(self):
        return copy.deepcopy(self.log[-1])

    def __len__(self):
        return len(self.log)

from enum import Enum
Type = Enum("Type", "LEAF NODE MEM_NODE ROOT_NODE")

class CheckPointBlock:
    def __init__(self):
        self.root_offset = 0

class IndexBlock:
    def __init__(self):
        self.index_block = {}

    def has(self, key_frag):
        return key_frag in self.index_block

    # type NODE, LEAF, cache
    # Use sorted list for c impl
    def put(self, key_frag, key, value, typ):
        self.index_block[key_frag] = (key, value, typ)

    def get(self, key_frag):
        return self.index_block[key_frag]

    def keys(self):
        return sorted(self.index_block.keys())

def split_by_n( seq, n ):
    while seq:
        yield seq[:n]
        seq = seq[n:]

class TransactionalIndex:
    def __init__(self, log):
        self.log = log
        self.root = log.get_last()
        self.in_memory_blocks = MemLog()

    def walk(self, callback, internal_nodes=False):
        def rec_do(block, depth):
            for subkey in block.keys():
                entry = block.get(subkey)
                print("    " * depth + subkey + ": " + str(entry))
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
            elif block.get(subkey)[2] == Type.NODE: # is NODE. bring into CACHE
                child_block = self.log.get(block.get(subkey)[1])
                child_block_index = self.in_memory_blocks.put(child_block)
                block.put(subkey, None, child_block_index, Type.MEM_NODE)
                block = child_block
                continue
            elif block.get(subkey)[2] == Type.MEM_NODE: # is cached NODE. 
                child_block = self.in_memory_blocks.get(block.get(subkey)[1])
                block = child_block
                continue
            elif block.get(subkey)[2] == Type.LEAF:
                old_entry = block.get(subkey) # key-value
                if old_entry[0] == key:
                    block.put(subkey, key, value, Type.LEAF)
                    return
                else:
                    new_block = IndexBlock() # block
                    index = self.in_memory_blocks.put(new_block)
                    block.put(subkey, None, index, Type.MEM_NODE)
                    self.put(old_entry[0], old_entry[1])
                    self.put(key, value)
                    return
            else:
                raise ValueError("Illegal element type.")

        # We don't have any subkeys left, hence, the block
        # we're looking at is also a leaf. We add the empty value
        block.put("", key, value, Type.LEAF)

    def get(self, key):
        block = self.root
        entry = None
        for subkey in split_by_n(key, 2):
            if block.has(subkey):
                entry = block.get(subkey)
                entry_type = entry[2]
                if entry_type == Type.LEAF: # LEAF
                    if entry[0] == key:
                        return entry[1]
                    else:
                        raise KeyError("Could not find key {} in block {}".format(key, entry))
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

        if block.has(""):
            entry = block.get("")
            if entry[0] == key:
                return entry[1]

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

        root_index = commit_rec(self.root)
        self.root = log.get(root_index)
        self.in_memory_blocks = MemLog()
        return root_index

if __name__ == "__main__":
    log = Log()
    log.put(CheckPointBlock())
    log.put(CheckPointBlock())
    log.put(IndexBlock())
    index = TransactionalIndex(log)

    import uuid
    for i in range(1000):
        key = uuid.uuid4().hex[:20]
        idx = log.put(key)
        index.put(key, idx)
        assert(index.get(key) == idx)

    for key in ["test", "test", "testa", "testb", "testab", "testab", "testac",
                "test", "test", "testa", "testb", "testab", "testab", "testac"]:
        idx = log.put(key)
        index.put(key, idx)
        assert(index.get(key) == idx)


    counter = 0
    import sys
    def tmp(key, value):
        global counter
        counter += 1

    index.walk(tmp, internal_nodes=True)
    index.commit()
    print("COMMIT")
    index.walk(tmp, internal_nodes=True)
    index.put("testa", log.put("testa"))
    print("ADDED test:test")
    index.walk(tmp, internal_nodes=True)
    index.commit()
    print("COMMIT")
    index.walk(tmp, internal_nodes=True)
