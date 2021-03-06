import rpyc
import sys
import copy


DEBUG = False
class BlockStore(rpyc.Service):
    """
	Initialize any datastructures you may need.
	"""
    def __init__(self):
        super(BlockStore, self).__init__()
        self.block_map = {}
        """
        store_block(h, b) : Stores block b in the key-value store, indexed by
        hash value h

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	"""
    def exposed_getHashNum(self):
        return len(self.block_map)
    def exposed_store_block(self, h, block):
        if DEBUG:
            print("trying to store", h, block)
        self.block_map[copy.deepcopy(h)] = copy.deepcopy(block)

    """
	b = get_block(h) : Retrieves a block indexed by hash value h

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	"""
    def exposed_get_block(self, h):
        if DEBUG:
            print("trying to get", h, self.block_map[h])
        return self.block_map[h]

    """
        rue/False = has_block(h) : Signals whether block indexed by h exists
        in the BlockStore service

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	"""
    def exposed_has_block(self, h):
        if DEBUG:
            print("trying to serach" ,h, self.block_map.get(h, "can'find"),"\n",(h in self.block_map))
        return h in self.block_map

    def exposed_ping(self):
        return True


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    port = int(sys.argv[1])
    server = ThreadPoolServer(BlockStore(), port=port)
    #print("start blockstore in port : %d" %(port, ))
    server.start()
