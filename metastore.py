import rpyc
import sys
import copy
import os
'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues -
1. The file being modified has misspping blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''

DEBUG = False
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

#def findServer(h, numBlockStores):
#	return int(h,16) % numBlockStores

def findServer(h, block_list):
    for i in range(len(block_list)):
        b = block_list[i]
        if (b.has_block(h)):
            return i

'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''
class MetadataStore(rpyc.Service):
    class StoreFile():
        def __init__(self, filename, hashlist):
            self.filename = filename
            self.version = 0
            self.hashlist = hashlist
    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
	"""
    def __init__(self, config):
        super(MetadataStore, self).__init__()
        self.readConfig(config)
        self.file_list = {}#filename: storeFileClass
    def readConfig(self, config):
        with open(config, "r") as f:
            lines = f.read().split("\n")
        self.block_num = int(lines[0].split(":")[-1])
        self.port = int(lines[1].split(":")[-1])
        self.block_list = []
        for i in range(2, 2 + self.block_num):
            infos = lines[i].split(":")
            self.block_list.append(rpyc.connect(infos[1].strip(), int(infos[2])).root)

    '''
        ModifyFile(f,v,hl): Modifies file f so that it now contains the
        contents refered to by the hashlist hl.  The version provided, v, must
        be exactly one larger than the current version that the MetadataStore
        maintains.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
    def exposed_modify_file(self, filename, version, hashlist):
        filename = copy.deepcopy(os.path.basename(filename))
        version = copy.deepcopy(version)
        #deep copy hash list
        new_hashlist = [""]*len(hashlist)
        for i in range(len(hashlist)):
            new_hashlist[i] = copy.deepcopy(hashlist[i])
        global DEBUG
        if (DEBUG):
            print("modify", filename,version)
        #check files
#        currFile = self.file_list.get(filename, None)
#        if (currFile == None):
#            e = ErrorResponse("file not found : " + filename)
#            e.file_not_found()
#            raise e
        currFile = self.file_list.get(filename, None)
        if (currFile == None):
            currFile = self.StoreFile(filename, [])

        #check version
        currVersion = currFile.version
        if (currVersion != version - 1):
            e = ErrorResponse("wrong version number of %s, current version is %d, but given %d" % (filename, currVersion, version))
            e.wrong_version_error(0)#set as 0
            raise e
        #update version
        currFile.version = version

        #check blocks
        for h in new_hashlist:
#            idxBlockStore = findServer(h, self.block_num)#we can't use this. we need to heavliy search the hash code in each block
            idxBlockStore = findServer(h, self.block_list)
            if not self.block_list[idxBlockStore].has_block(h):
                e = ErrorResponse("missing blocks of " + filename)
                e.missing_blocks(hashlist)
                raise e



        #modify file
        if DEBUG:
            print("successfully modify", version, hashlist )


        currFile.hashlist = new_hashlist
        self.file_list[filename] = currFile



        '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
	'''
    def exposed_delete_file(self, filename, version):
        global DEBUG
        if DEBUG:
            print("delete", filename, version)
        filename = copy.deepcopy(os.path.basename(filename))
        version = copy.deepcopy(version)
        #check files
        currFile = self.file_list.get(filename, None)
        if (currFile == None):#if deleted
            e = ErrorResponse("file not found" + filename)
            e.file_not_found()
            raise e

        if (len(currFile.hashlist) == 0):
            return

        #check version

        if (currFile.version != version - 1):

            e = ErrorResponse("wrong version number of %s, current version is %d, but given %d" % (filename, currFile.version, version))
            e.wrong_version_error(currFile.version)
            raise e

        #update version
        currFile.version = version
        #delete file:clean hashlist
        currFile.hashlist= []
        return
    '''
        (v,hl) = ReadFile(f): Reads the file with filename f, returning the
        most up-to-date version number v, and the corresponding hashlist hl. If
        the file does not exist, v will be 0.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_read_file(self, filename):
        global DEBUG
        if (DEBUG):
            print("read", filename)
        filename = copy.deepcopy(os.path.basename(filename))

        currFile = self.file_list.get(filename, None)
        if (currFile == None):

#            currFile = self.StoreFile(filename, [])
#            self.file_list[filename] = currFile
            if DEBUG:
                print("try to read :", filename, "but not exist")
            return 0,[]
        else:
            if DEBUG:
                print("try to read :", filename, currFile.version, currFile.hashlist)

            # don't know weather I should do it
        return currFile.version, currFile.hashlist


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    port = 6000
    server = ThreadPoolServer(MetadataStore(sys.argv[1]), port = port)
#    print("OK")
#    print("start metadatastore in port: " + str(port))
    server.start()
