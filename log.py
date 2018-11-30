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

Type = Enum("Type", "LEAF NODE MEM_NODE")
class IndexBlock:
    def __init__(self):
        self.index_block = {}

    def has(self, key_frag):
        return key_frag in self.index_block

    # type NODE, LEAF, cache
    def put(self, key_frag, key, value, typ):
        self.index_block[key_frag] = (key, value, typ)

    def get(self, key_frag):
        return self.index_block[key_frag]

def split_by_n( seq, n ):
    offset = 0
    while seq:
        yield (offset, seq[:n])
        offset += n
        seq = seq[n:]

class TransactionalIndex:
    def __init__(self, log):
        self.log = log
        self.root = log.get_last()
        self.in_memory_blocks = MemLog()

    def walk(self, callback, all_nodes=False):
        def rec_do(block, depth):
            #print("--" * depth + str(block.index_block))
            for subkey in block.index_block:
                entry = block.get(subkey)
                if entry[2] == Type.NODE:
                    rec_do(self.log.get(entry[1]), depth + 1)
                    if all_nodes:
                        callback(entry[0], entry[1])
                elif entry[2] == Type.MEM_NODE:
                    rec_do(self.in_memory_blocks.get(entry[1]), depth + 1)
                    if all_nodes:
                        callback(entry[0], entry[1])
                elif entry[2] == Type.LEAF:
                    callback(entry[0], entry[1])
                else:
                    raise ValueError("Failure to walk structure")
        return rec_do(self.root, 0)


    def put(self, key, value):
        block = self.root
        self.in_memory_blocks.put(block)

        for (offset, subkey) in split_by_n(key, 2):
            if block.has(subkey) == False:
                block.put(subkey, key, value, Type.LEAF)
                return

            if block.get(subkey)[2] == Type.NODE: # is not LEAF
                child_block = self.log.get(block.get(subkey)[1])
                child_block_index = self.in_memory_blocks.put(block)
                block.put(subkey, None, child_block_index, Type.MEM_NODE)
                block = child_block
                continue

            elif block.get(subkey)[2] == Type.MEM_NODE: # is not LEAF
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
        for (offset, subkey) in split_by_n(key, 2):
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
            for subkey in block.index_block:
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
    log   = Log()
    log.put(IndexBlock())
    index = TransactionalIndex(log)

    import uuid
    for i in range(100000):
        uid = uuid.uuid4().hex[:]
        key = uid + "_key"
        value = uid + "_value"
        value_offset = log.put(value)
        index.put(key, value_offset)
        assert(index.get(key) == value_offset)
        assert(log.get(index.get(key)) == value)


    for key in ["test", "test", "testa", "testb", "testab", "testab", "testac"
                "test", "test", "testa", "testb", "testab", "testab", "testac"]:
        index.put(key, log.put(key))

    counter = 0
    def tmp(key, value):
        global counter
        counter += 1
    index.walk(tmp, True)
    print("values", counter)
    print("commit offset ", index.commit())

    counter = 0
    index.walk(tmp)
    print("values", counter)
