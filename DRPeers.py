__author__ = 'reggie'

from DRShared import *
import json

class Function(object):
    #poi = ""        #function process object identifier
    #name = ""       #name
    #input_type = "" #raw type | rdf type
    #output_type ="" # raw_type | rdf_type
    #complexity = None #function complexity
    #semantic = None   #inline semantic | url

    def __init__(self, poi, name, input_t, output_t, complexity=None, semantic=None):
        self.poi = poi
        self.name = name
        self.input_type = input_t
        self.output_type = output_t
        self.complexity = complexity
        self.semantic = semantic

    @classmethod
    def fromJSON(cls, JSON):
        parms = json.loads(JSON)
        return cls(parms["poi"], parms["name"], parms["input_type"], parms["output_type"], parms["complexity"], parms["semantic"])


    def getJSON(self):
        json_text = '{' \
                    '"poi": "'+str(self.poi)+ '",'+ \
                    '"name" : "'+str(self.name) + '",'+\
                    '"input_type" : "'+str(self.input_type) + '",'+\
                    '"output_type" : "'  +str(self.output_type) + '",'+\
                    '"complexity" : "' +str(self.complexity) + '",'+\
                    '"semantic" : "' + str(self.semantic) + '"'+\
                    '}'
        return json_text

    def getName(self):
        return self.name

class Communication(object):

    #type = ""       #TFTP | P2PTFTP | RABBITMQ
    #host = ""       #ip | host
    #port = ""       #port
    #cred = ""       #username:password | token
    #aux1 = ""       #randevouz_server:port | queue_name
    #aux2 = ""       #extra_parameters
    TFTP_TYPE = "TFTP"

    def __init__(self, type, host, port, cred=None, aux1=None, aux2=None):
        self.type = type
        self.host = host
        self.port = port
        self.cred = cred
        self.aux1 = aux1
        self.aux2 = aux2

    @classmethod
    def fromJSON(cls, JSON):
        p = json.loads(JSON)
        return cls(p["type"], p["host"], p["port"], p["cred"], p["aux1"], p["aux2"])

    def getJSON(self):
        json_text = '{' \
                    '"type" : "'+str(self.type)+'",' \
                    '"host" : "'+str(self.host)+'",' \
                    '"port" : "'+str(self.port)+'",' \
                    '"cred" : "'+str(self.cred)+'",' \
                    '"aux1" : "'+str(self.aux1)+'",' \
                    '"aux2" : "'+str(self.aux2)+'"' \
                    '}'

        return json_text

class Peer(object):
    #uid = ""        #peer uid
    #ring = ""       #ring_id
    #comms = []
    #functions = []
    #peers = []

    def __init__(self, uid, ring="default"):
        self.uid = uid
        self.ring = ring
        self.comms = []
        self.peers = []
        self.functions = []
        pass

    def getJSON(self, rec=True):
        json_text = '{' \
                    '"uid"  : "'+str(self.uid)+'",'\
                    '"ring" : "'+str(self.ring)+'",'\
                    '"comms" : ['+self.__getCommsJSON() +'],'\
                    '"functions" : ['+self.__getFuncsJSON()+'],'\
                    '"peers" : ['+self.__getPeerJSON(rec)+']' \
                    '}'

        return json_text


    def __getPeerJSON(self, rec=True):
        allp = ""
        if(rec == True):
            for p in self.peers:
                allp = allp + p.getJSON(False) + ','

        return str(allp[:-1])

    def __getCommsJSON(self):
        allc=""
        for c in self.comms:
            allc = allc  + c.getJSON() +','
        return str(allc[:-1])

    def __getFuncsJSON(self):
        allf=""
        for f in self.functions:
            allf = allf + f.getJSON() + ','
        return str(allf[:-1])

    def addComm(self, comm):
        if( type( comm ) == Communication):
            self.comms.append(comm)
        else:
            log.warn("Trying to add wrong object type to peer comms")

    def addFunction(self, function):
        if( type( function ) == Function):
            self.functions.append( function )
        else:
            log.warn("Trying to add wrong object type to functions")

    def addPeer(self, peer):
        if( type( peer ) == Peer):
            self.peers.append(peer)
        else:
            log.warn("Trying to add wrong object type to peers")

    def getPeerForFunc(self, function):
        for p in self.peers:
            for f in p.functions:
                if f.getName() == function:
                    return p
        return None

    def getTftpComm(self):
        for c in self.comms:
            if c.type == Communication.TFTP_TYPE:
                return c

        return None

