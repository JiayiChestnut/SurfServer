import rpyc
import sys
import copy
'''
A RAFT RPC server class.

Implementation of Leader Election of RAFT consensus protocal using RPYC package.
'''
import time, random, threading, os
DEBUG = False
configFile = ""
serverID = 0
serverPort = 5000

class RaftNode(rpyc.Service):


    #self.state: 0 follower, 1 candidate, 2 leader
	#self.term: increase monotonically, each time become candidate, term will plus 1self.

    def __init__(self):
        global configFile, serverID
        super(RaftNode, self).__init__()
        self.ID = serverID

        self.peerList = []#(id, object)

        #set up connection
        self.getPeerConnect(configFile, serverID)

        self.term = 0
        self.voteFor = -1
        self.ElectionTimeOutUpperBound = 300
        self.ElectionTimeOutLowerBound = 150
        self.heartBeatTimeOut = 50 / 100.0
        self.stateMap = {0:"follower", 1:"candidate", 2:"leader"}

        #try to build a log files, persist change of term and state every time
        if not os.path.exists("./tmp"):
            os.mkdir("./tmp")
        filename = "./tmp/log4Raft" + str(self.ID)
        self.fhandle = open(filename, "w")

        self.state = 0
        threading.Thread(target = self.startIteration).start()

    def startIteration(self):
		#Once the service is on, this function will not stop until you
		#kill it using system pid kill
        if DEBUG:
            print("start server")
        while(True):
            if (self.state == 0):
                self.startFollower()
            elif(self.state == 1):
                self.startElection()
            else:
                self.startLeader()
        return

    def refreshElectionTimeOut(self):
        global DEBUG
        self.ElectionTimeOut = random.randint(self.ElectionTimeOutLowerBound, self.ElectionTimeOutUpperBound) / 100.0
        if DEBUG:
            print("new election timeout: ", self.ElectionTimeOut)
        self.currElectionTimeOut = time.time() + self.ElectionTimeOut

    def startElection(self):

        #initialize time out
        self.voteFor = self.ID
        self.setTerm(self.term + 1)
        self.refreshElectionTimeOut()
        self.voteRatio = 0



        voteRatio = self.requestVote()
        #send RequestVote, calculate result, transform into next state
        while (time.time() < self.currElectionTimeOut):
            if self.state == 1:
                if (voteRatio > 0.5):
#                if res.ready and res.value > 0.5:
                    self.setState(2)
                    return

            else:
                #if other state, we should break the startElectionState
                return

        #if no wins,startElection again
        self.startElection()

    def exposed_appendEntry(self, leaderTerm, leaderID):
		#if a leader receive appendEntry with higher term, it will update term and become follower
        global DEBUG
        if DEBUG:
            print("leaderID : %d\tleaderTerm : %d\tselfTerm : %d" % (leaderID, leaderTerm, self.term))
        self.refreshElectionTimeOut()
        if (leaderTerm < self.term):
            return self.term, False
        elif (leaderTerm >= self.term):

            self.setTerm(leaderTerm)

            if (self.state == 1):
                self.setState(0)
                return self.term, True





    def setTerm(self, newTerm):

        global DEBUG
        if DEBUG:
            print("old term : %d\tnew term : %d" %(self.term, newTerm))
        #add persistent
        self.fhandle.write("term" + str(newTerm) + "\n")
        self.fhandle.flush()
        self.term = newTerm


    def getAuthority(self, peerIp, peerPort, resList, i):
        try:
			#one way is to use timed function for asynchronized programming.But Here
			#we prefer not to use timed function since we already use threading to
			#call this getAuthority function
#            peer = rpyc.connect(peerIp, peerPort).root
#            resList[i] = rpyc.timed(peer.appendEntry(self.term, self.ID), self.heartBeatTimeOut)##!!! should we allow small time to proceed

            resList[i] = rpyc.connect(peerIp, peerPort).root.appendEntry(self.term, self.ID)
        except:
            resList[i] = None

    def startLeader(self):
        self.voteFor = self.ID
        self.setTerm(self.term + 1)
        while(self.state == 2):
            timeOut = time.time() + self.heartBeatTimeOut
            resList = [[]]*len(self.peerList)
            threadList = [None ]* len(self.peerList)
            for i in range(len(self.peerList)):
                paras = self.peerList[i]
                threadList[i] = threading.Thread(target = self.getAuthority, args = (paras[0], paras[1], resList, i))
                threadList[i].start()

            #we goto next heartbeat if heartBeat Inteval is finished.
            while(time.time() < timeOut):
                for i in range(len(resList)):
                    threadList[i].join()
                    if resList[i] != None:
                        peerTerm = copy.deepcopy(resList[i][0])
                        if self.term < peerTerm:
                            self.setState(0)
                            self.setTerm(peerTerm)
                            return




    def startFollower(self):
        #become follower is no need to upgrade term, we can wait for the
		#appendEntry RPC and update term at that time. Otherwise, every timed
		#we become follower we just update term, it will result in racing and
		#leader can't be elected out but the terms will increase rapidly.
        self.refreshElectionTimeOut()
        while (time.time() < self.currElectionTimeOut):
            if self.state == 1 or self.state == 2:
                return
        #if timeout, become candidate, return back to looping
        self.setState(1)
        return



    def setState(self, newState):

        global DEBUG
        if DEBUG:
            print("old state : %s\tnew state : %s" % (self.stateMap.get(self.state), self.stateMap.get(newState)))
        self.fhandle.write("newstate" + str(newState) + "\n")
        self.fhandle.flush()
        self.state = newState

    def setVoteFor(self, newVote):
        global DEBUG
        if DEBUG:
            print("old vote : %d\tnew vote : %d" % (self.voteFor, newVote))
        self.voteFor = newVote

    def exposed_ansVoteRequest(self, peerTerm, peerID):
        if DEBUG:
            print("receive vote request from ID %d in of term %d, my vote now : %d" % (peerID, peerTerm, self.voteFor))
        #no matter what state self is, respond to it
        #in same term, if already voteFor someone, it can't vote the later ansVoteRequest
        #if term is larger than current term, update term and vote for some one.
        if (self.term > peerTerm):
            return self.term, False

        else:
            self.setVoteFor(peerID)
            self.setState(0)
            return self.term, True


    #if peer server is alive, request vote RPC will get ans quickly
    def requestVote(self):
        aliveNum = 0
        voteNum = 0
        resList = [None]* len(self.peerList)
        threadList = [None ]* len(self.peerList)
        #here we should asynchronizely send request!!!
        def getVote(peerIp, peerPort, resL, idx):

            try:
                resL[i] = rpyc.connect(peerIp, peerPort).root.ansVoteRequest(self.term, self.ID)#should we allow less time ????
            except:
                resL[i] = None

        for i in range(len(self.peerList)):
            paras = self.peerList[i]
            threadList[i] = threading.Thread(target = getVote, args = (paras[0], paras[1], resList, i))
            threadList[i].start()
        for i in range(len(self.peerList)):

            #wait for ready
            threadList[i].join()

            res = resList[i]
            if(res == None):
                continue
            else:

                aliveNum += 1
                if res[1] == True:
                    voteNum += 1
                if DEBUG:
                    print(str(self.peerList[i][1]) + " is alive\tget vote : " + str(res))
#        self.voteRatio = voteNum / float(aliveNum)
        if DEBUG:
            print("vote for me : %d\tonline number : %d" % (voteNum, aliveNum))
        if(aliveNum > 1):#if there are not more than 2(including self) machines alive, we can't elect a leader
            if (self.voteFor == self.ID):
                return (voteNum + 1) / (1 + float(aliveNum))
            else:
                return (voteNum)/(1 + float(aliveNum))
        else:
            return 0
#        return voteNum / float(aliveNum)



    def getPeerConnect(self, configFile, sID):
        #connect others except yourself into self.peerList, (id, rpyc object)
        with open(configFile, "r") as f:
            lines = f.read().split("\n")
        line0 = lines[0].split(":")
        numPeer = int(line0[1].strip())
        for i in range(numPeer):
            infos = lines[i + 1].split(":")
            if DEBUG:
                print(infos)
            if (sID is not i):
                self.peerList.append((infos[1].strip(), int(infos[2])))


    def exposed_is_leader(self):
        if (self.state == 2):
            return True
        else:
            return False


if __name__ == '__main__':
    #command line:
    #python raftnode.py config.txt 1 5002
    #python raftnode.py config.txt 2 5003
    #python raftnode.py config.txt 3 5004
    #kill:
    #pkill -f raftnode
    import rpyc
    from rpyc.utils.server import ThreadPoolServer
#    from rpyc.utils.helpers import classpartial
    configFile = sys.argv[1]
    serverID = int(sys.argv[2])
    serverPort = int(sys.argv[3])
	server = ThreadPoolServer(RaftNode(), port = serverPort)
#    server = ThreadPoolServer(RaftNode(configFile, serverID), port = serverPort)
#    print(server.port, server.host)
#    myservice = classpartial(RaftNode, configFile, serverID)

#    server = rpyc.utils.server.ThreadedServer(RaftNode(), port = serverPort)

    server.start()

#    myRPYC = rpyc.connect("0.0.0.0", port = 5001 + serverID).root
#    print("connected")
#    myRPYC.startIteration()
