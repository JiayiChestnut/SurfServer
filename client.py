import rpyc
import hashlib
import os
import sys
#import timeit
import time
DEBUG = False
"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""
class ErrorResponse(Exception):
	def __init__(self, message):
		super(ErrorResponse, self).__init__(message)
		self.error = message

	def missing_blocks(self, hashlist):
		self.error_type = 1
		self.missing_blocks = hashlist

	def wrong_version_error(self, version):
		self.error_type = 2
		self.current_version = version

	def file_not_found(self):
		self.error_type = 3

def sha256(byteStr):
    # m = hashlib.sha256(str.encode(s))
	res = hashlib.sha256(byteStr).hexdigest()
	return res

def create_blocklist(filename):
    try:
        file = open(filename, 'rb')
        hash_block_map = {}
        while True:
            block = file.read(4096)
            read_amt = len(block)

            if read_amt != 0:
                _hash = sha256(block)
                hash_block_map[_hash] = block
            else:
                break

        return hash_block_map

    except IOError: return None


def findServerHash(h, numBlockStores):
	return int(h,16) % numBlockStores



class SurfStoreClient():
    """
    Initialize the client and set up connections to the block stores and
    metadata store using the config file
    """
    def __init__(self, config):
        with open(config, "r") as f:
            lines = f.read().split("\n")
        self.block_num = int(lines[0].split(":")[-1])




        #set up conn with blocks
        self.block_list = []
        for i in range(2, 2 + self.block_num):
            infos = lines[i].split(":")
            self.block_list.append(rpyc.connect(infos[1].strip(), int(infos[2])).root)

        #set up conn with meta store
        infos = lines[1].split(":")
#        print(infos)
        self.metadataStore = rpyc.connect(infos[1].strip(), int(infos[2])).root

        #choose the server finding
        self.findServerOption = int(lines[2 + self.block_num])
        """
        	upload(filepath) : Reads the local file, creates a set of


        	hashed blocks and uploads them onto the MetadataStore
        	(and potentially the BlockStore if they were not already present there).
        	"""
    def upload(self, filepath):
        #create block list
        hash_block_map = create_blocklist(filepath)
        if (hash_block_map == None):
            raise IOError("missing file : %s !" % (filepath,))
        filename = os.path.basename(filepath)
#        filename = filepath


        #create new blocks if not exist
        #always use blockStore 0 to store things
        if (self.findServerOption == 1):
            for h in hash_block_map.keys():
                exist = False
                for b in self.block_list:
                    #check blocks exists, make sure the min_Block to insert
                    if b.has_block(h):
                        exist = True
                        break
                if not exist:
                    #need to insert to block
                    block2insert = self.block_list[findServerHash(h, self.block_num)]
                    block2insert.store_block(h, hash_block_map[h])#hash, correponding block content  ## use tuple

        if (self.findServerOption == 2):
            #perform pings, store all blocks to
            minTime = 999999
            block2insertIdx = 0
            for i in range(self.block_num):
#                pingTime = timeit.timeit('self.block_list[i].ping()', number = 3)
                pingTime = 0
                for test in range(3):
                    startT = time.time()
                    self.block_list[i].ping()
                    endT = time.time()
                    pingTime += endT-startT
                pingTime /= 3


                if (pingTime < minTime):
                    block2insertIdx = i
                    minTime = pingTime
            block2insert = self.block_list[block2insertIdx]
            print "save the files to block %d th" % block2insertIdx
            for h in hash_block_map.keys():
                block2insert.store_block(h, hash_block_map[h])








#        print("uploading %s "% filename)
#        print(hash_block_map)
#

        #create new file
        file_version, file_hashlist = self.metadataStore.read_file(filename)#create a new empty one

        #add hashlist for it
        self.metadataStore.modify_file(filename, file_version + 1, list(hash_block_map.keys()))#use tuple
#        print("OK")
        return

    """
	delete(filename) : Signals the MetadataStore to delete a file.
	"""
    def delete(self, filename):
        #check the existence of file
#        try:
#            self.metadataStore.delete_file(filename, 0)
#        except ErrorResponse as e:
#            if (e.error_type == 2):
#                self.metadataStore.delete_file(filename, e.current_version + 1)
#            else:
#                raise e
#
        version, hashlist = self.metadataStore.read_file(filename)
        if (version == 0 and len(hashlist) == 0):
            #no exist this filename
#            print("Not Found")
            pass
        else:
            self.metadataStore.delete_file(filename, version + 1)
#            print("OK")

        return
        """
        download(filename, dst) : Downloads a file (f) from SurfStore and saves
        it to (dst) folder. Ensures not to download unnecessary blocks.
	"""
    def download(self, filename, location):
        global DEBUG
        #get blocks
        if DEBUG:
            print("try to download:", filename)
        curr_version, curr_hashlist = self.metadataStore.read_file(filename)

        if (curr_version == 0 and len(curr_hashlist) == 0):
            print("Not Found")
            return

        if DEBUG:
            print(curr_version)
        curr_blocks = []

#        print("currversion" + str(curr_version) + "curr_hashlist" + str(curr_hashlist))
        for h in curr_hashlist:

            for b in self.block_list:
                if b.has_block(h):
                    curr_blocks.append(b.get_block(h))
                    break


        #what if missing block?
        if (len(curr_blocks) == 0):
            e = ErrorResponse("missing blocks of " + filename)
            e.missing_blocks([])
            raise e


        #merge blocks,write file
        byteContent = b"".join(curr_blocks)
        with open(os.path.join(location, filename), "wb") as f:
            f.write(byteContent)
#        print("OK")
        print "OK"
        return
    """
	 Use eprint to print debug messages to stderr
	 E.g -
	 self.eprint("This is a debug message")
	"""
    def eprint(*args, **kwargs):
#        print(*args, file=sys.stderr, **kwargs)
#        print *args, file=sys.stderr, **kwargs
        return
    def test1(self):
        self.metadataStore.read_file("fil1.txt")

if __name__ == '__main__':
    client = SurfStoreClient(sys.argv[1])
    # print("OK")#only success should we print ok. or we should print out NO found

    operation = sys.argv[2]
    if operation == 'upload':
        client.upload(sys.argv[3])
    elif operation == 'download':
        client.download(sys.argv[3], sys.argv[4])
    elif operation == 'delete':
        client.delete(sys.argv[3])
    else:
        print("Invalid operation")
