__author__="reggie"


import uuid
import re

class Packet(object):
    NULL_BITS               = 0b0
    TIMING_BIT              = 0b1         #1
    TRACK_BIT               = 0b10        #2
    ACK_BIT                 = 0b100       #4
    TRACER_BIT              = 0b1000      #8
    BROADCAST_BIT           = 0b10000     #16
    FORCE_BIT               = 0b100000    #32
    LOAD_BIT                = 0b1000000
    CODE_BIT                = 0b10000000
    GONZALES_BIT            = 0b100000000
    BCKPRESSURE_BIT         = 0b1000000000
    PRESSURETOGGLE_BIT      = 0b10000000000

    PKT_STATE_NEW           = "NEW"
    PKT_TAG_NONE            = "NONE:NONE"
    PKT_TTL_DISABLED        = 'D'

    @staticmethod
    def new_empty_packet(ship_id, cont_id=None, automaton=None):
        pkt = []
        header = {}
        if not automaton:
            automaton = {}

        ship_id = ship_id
        if not cont_id:
            cont_id =  str(uuid.uuid4())[:8]

        header["ship"]          = ship_id
        header["cont_id"]       = cont_id
        header["box"]           = 0
        header["fragment"]      = 0
        header["e"]             = 0
        header["state"]         = Packet.PKT_STATE_NEW
        header["aux"]           = Packet.NULL_BITS
        header["ttl"]           = Packet.PKT_TTL_DISABLED
        header["c_tag"]         = Packet.PKT_TAG_NONE
        header["t_state"]       = None
        header["t_otype"]       = None
        header["c_size"]        = 0
        header["last_host"]     = None
        header["stop_func"]     = None
        header["last_contact"]  = None
        header["last_func"]     = None
        header["last_timestamp"]= 0


        pkt.append(header)
        pkt.append(automaton)


        pkt.append( {"stag" : "RAW", "exstate" : "0001", "ep" : "local"} )

        return pkt

    @staticmethod
    def get_ip_from_ep(ep):
        parts = re.split(r'[//:\s]*',ep)
        if len(parts) > 2:
            return parts[1]

    @staticmethod
    def get_proto_from_ep(ep):
        ep_a = ep.split("://")
        return ep_a[0]

    @staticmethod
    def is_pkt_gonzales(pkt):
        header = pkt[0]
        if header["aux"] & Packet.GONZALES_BIT:
            return True
        else:
            return False