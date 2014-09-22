__author__ = 'reggie'

import threading
import socket, os
import time

import ujson as json
import hashlib
import struct
import fcntl
import zmq
import Queue
import subprocess as sp
import pika
import tftpy
import zlib
import stun
import netaddr
#import netifaces

#from Queue import *

from select import select
from socket import *
from threading import *
import PmkShared


from PmkShared import *

AMAZON_IPS = [ '72.44.32.0/19','67.202.0.0/18','75.101.128.0/17','174.129.0.0/16','204.236.192.0/18','184.73.0.0/16','184.72.128.0/17','184.72.64.0/18','50.16.0.0/15','50.19.0.0/16','107.20.0.0/14','23.20.0.0/14','54.242.0.0/15','54.234.0.0/15','54.236.0.0/15','54.224.0.0/15','54.226.0.0/15','54.208.0.0/15','54.210.0.0/15','54.221.0.0/16','54.204.0.0/15','54.196.0.0/15','54.198.0.0/16','54.80.0.0/13','54.88.0.0/14','54.92.0.0/16','54.92.128.0/17','54.160.0.0/13','54.172.0.0/15','50.112.0.0/16','54.245.0.0/16','54.244.0.0/16','54.214.0.0/16','54.212.0.0/15','54.218.0.0/16','54.200.0.0/15','54.202.0.0/15','54.184.0.0/13','54.68.0.0/14','204.236.128.0/18','184.72.0.0/18','50.18.0.0/16','184.169.128.0/17','54.241.0.0/16','54.215.0.0/16','54.219.0.0/16','54.193.0.0/16','54.176.0.0/15','54.183.0.0/16','54.67.0.0/16','79.125.0.0/17','46.51.128.0/18','46.51.192.0/20','46.137.0.0/17','46.137.128.0/18','176.34.128.0/17','176.34.64.0/18','54.247.0.0/16','54.246.0.0/16','54.228.0.0/16','54.216.0.0/15','54.229.0.0/16','54.220.0.0/16','54.194.0.0/15','54.72.0.0/14','54.76.0.0/15','54.78.0.0/16','54.74.0.0/15','185.48.120.0/22','54.170.0.0/15','175.41.128.0/18','122.248.192.0/18','46.137.192.0/18','46.51.216.0/21','54.251.0.0/16','54.254.0.0/16','54.255.0.0/16','54.179.0.0/16','54.169.0.0/16','54.252.0.0/16','54.253.0.0/16','54.206.0.0/16','54.79.0.0/16','54.66.0.0/16','175.41.192.0/18','46.51.224.0/19','176.32.64.0/19','103.4.8.0/21','176.34.0.0/18','54.248.0.0/15','54.250.0.0/16','54.238.0.0/16','54.199.0.0/16','54.178.0.0/16','54.95.0.0/16','54.92.0.0/17','54.168.0.0/16','54.64.0.0/15','177.71.128.0/17','54.232.0.0/16','54.233.0.0/18','54.207.0.0/16','54.94.0.0/16','54.223.0.0/16','96.127.0.0/18','96.127.64.0/18' ]

class cmd(Queue.Queue):
    def __init__(self):
        Queue.Queue.__init__(self)
        pass

def get_cloud_ip():
     x = sp.Popen("ip addr show | grep  172.16 | awk '{print $2}'", stdout= sp.PIPE, shell=True).stdout.read().split("/")[0]
     return x

def get_interface_ip(ifname):
    s = socket(AF_INET, SOCK_DGRAM)
    return inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s',
                                ifname[:15]))[20:24])
    pass

#def get_interface_ip6(ifname):
#    #addrs = netifaces.ifaddresses(ifname)
#    #return addrs[netifaces.AF_INET6][0]['addr']
#    pass
#
#def get_lan_ip6():
#    interfaces = [
#            "lo0"
#            "eth0",
#            "eth1",
#            "eth2",
#            "wlan0",
#            "wlan1",
#            "wifi0",
#            "ath0",
#            "ath1",
#            "ppp0",
#            ]
#    for ifname in interfaces:
#            try:
#                ip = get_interface_ip6(ifname)
#                s = json.dumps(ip)
#                print str(s)
#                break
#            except:
#                pass
#    return ip

def get_llan_ip():
    ip = get_cloud_ip()

    if not ip:
        interfaces = [
            "eth0",
            "eth1",
            "eth2",
            "wlan0",
            "wlan1",
            "wifi0",
            "ath0",
            "ath1",
            "ppp0",
            ]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
                break
            except:
                pass
    return ip

def get_lan_ip():

    #FIXME get_cloud_ip() is a hack for SC should be removed
    ip = get_cloud_ip()

    if not ip:

        pip = get_public_ip()
        if is_amazon(pip):
            #if it is an amazon Public IP return it else get interface IP
            return pip

        interfaces = [
            "eth0",
            "eth1",
            "eth2",
            "wlan0",
            "wlan1",
            "wifi0",
            "ath0",
            "ath1",
            "ppp0",
            ]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
                break
            except:
                pass
    return ip

def get_public_ip():
    nat_type, external_ip, external_port = stun.get_ip_info()
    return str(external_ip)

def is_private(ip):
    ipa = netaddr.IPAddress(ip)
    if ipa.is_private():
        return True
    return False

def is_amazon(ip):
    ipa = netaddr.IPAddress(ip)
    if ipa.is_private():
        return False

    if ipa.is_reserved():
        return False

    for anet in AMAZON_IPS:
        if ipa in netaddr.IPNetwork(anet):
            return True

    return False
def get_zmq_supernodes(node_list):
    ret = []
    for sn in node_list:
        s = "tcp://"+sn+":"+str(ZMQ_PUB_PORT)
        ret.append(s)
    return ret


class RabbitMQBroadcaster(SThread):
    def __init__(self, context, exchange='global'):
        SThread.__init__(self)
        self.context = context
        self.cmd = self.context.get_cmd_queue()

        self.exchange = exchange
        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,  credentials=credentials, virtual_host=vhost))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, type='fanout')
        #self.queue = self.channel.queue_declare(exclusive=True)

    def _connect(self):
        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials, virtual_host=vhost))
        self.channel = self.connection.channel()

    def run(self):

        #test_str = '"cmd" : {"type" : "arp", "id" : "patient_X2", "reply-to" : "amqp://'+self.context.getUuid()+'"}'
        #test_str = None
        cmd_str = None

        while True:

            cmd_str = None

            if not self.context.getProcGraph().isRegistryModified():

                data = self.context.getProcGraph().dumpExternalRegistry()

                try:
                    cmd_str = self.cmd.get(False)
                except Queue.Empty as e:
                    pass


                if cmd_str:
                    if len(data) > 5:
                        data = data[:-1]
                        data = data+","+cmd_str+"}"
                    else:
                        data = "{"+cmd_str+"}"
                else:
                    time.sleep(self.context.get_broadcast_rate())

                dataz = zlib.compress(data)

                if self.connection.is_closed:
                    self._connect()

                self.channel.basic_publish(exchange=self.exchange,routing_key='',body=dataz)


                if self.stopped():
                    logging.debug("Exiting thread: "+self.__class__.__name__)
                    break
                else:
                    continue

            if self.context.getProcGraph().isRegistryModified():
                data = self.context.getProcGraph().dumpExternalRegistry()

                # if cmd_str:
                #     if len(data) > 5:
                #         data = data[:-1]
                #         data = data+","+cmd_str+"}"
                #     else:
                #         data = "{"+cmd_str+"}"

                dataz = zlib.compress(data)
                self.context.getProcGraph().ackRegistryUpdate()

                if self.connection.is_closed:
                    self._connect()

                self.channel.basic_publish(exchange=self.exchange,routing_key='',body=dataz)

                if self.stopped():
                    logging.debug("Exiting thread: "+self.__class__.__name__)
                    break
                else:
                    continue

class RabbitMQBroadcastSubscriber(SThread):
    def __init__(self, context, exchange='global'):
        SThread.__init__(self)
        self.context = context

        self.exchange = exchange
        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,  credentials=credentials, virtual_host=vhost))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.queue = result.method.queue
        self.channel.queue_bind(exchange=self.exchange,
                   queue=self.queue)

    def run(self):

        while True:

            method, properties, dataz = self.channel.basic_get(queue=self.queue, no_ack=True)
            if method:
                if (method.NAME == 'Basic.GetEmpty'):
                    time.sleep(1)
                else:
                    data = zlib.decompress(dataz)
                    logging.debug("Incomming data from ["+self.queue+"]: "+data)
                    d = json.loads(data)
                    for k in d.keys():
                        if not (k == "cmd"):
                            self.context.getProcGraph().updateRegistry(d[k])
                        else:
                            logging.debug('Command detected: '+str(d[k]))
                            if(d[k]["type"] == "arp"):
                                pkt_id = d[k]["id"]
                                pkt = self.context.get_pkt_from_shelve(pkt_id)
                                for p in pkt:
                                    ep = d[k]["reply-to"]
                                    p[0]["state"] = "ARP_OK"
                                    exdisp = self.context.getExternalDispatch()
                                    logging.debug("Sending ARP response: "+json.dumps(p))
                                    exdisp.send_to_ep(p, ep)
            else:
                time.sleep(1)

class ZMQBroadcaster(SThread):
    def __init__(self, context, zmq_context,  sn):
        SThread.__init__(self)
        self.context = context
        self.sn = sn
        self.zmq_cntx = zmq_context
        self.cmd = self.context.get_cmd_queue()

    def run(self):
        logging.info("Starting thread: "+self.__class__.__name__)
        sock = self.zmq_cntx.socket(zmq.PUB)
        try:
            #sock.bind("tcp://*:"+str(self.port))
            sock.bind(self.sn)

        except Exception as er:
            logging.warn("ZMQ Broadcaster disabled (another is already running)")
            sock.close()
            return


        test_str = '"cmd" : {"type" : "arp", "id" : "afadfadf", "reply-to" : "127.0.0.1:7789"}'

        while True:
            cmd_str = None
            try:
                cmd_str = self.cmd.get_nowait()
            except Queue.Empty as e:
                pass

            if not self.context.getProcGraph().isRegistryModified():
                time.sleep(self.context.get_broadcast_rate())
                data = self.context.getProcGraph().dumpExternalRegistry()

                if cmd_str:
                    if len(data) > 5:
                        data = data[:-1]
                        data = data+","+cmd_str+"}"
                    else:
                        data = "{"+cmd_str+"}"
                dataz = zlib.compress(data)
                sock.send(dataz)
                if self.stopped():
                    logging.debug("Exiting thread: "+self.__class__.__name__)
                    break
                else:
                    continue

            if self.context.getProcGraph().isRegistryModified():
                data = self.context.getProcGraph().dumpExternalRegistry()
                self.context.getProcGraph().ackRegistryUpdate()

                if cmd_str:
                    if len(data) > 5:
                        data = data[:-1]
                        data = data+","+cmd_str+"}"
                    else:
                        data = "{"+cmd_str+"}"
                dataz = zlib.compress(data)
                sock.send(dataz)
                if self.stopped():
                    logging.debug("Exiting thread: "+self.__class__.__name__)
                    break
                else:
                    continue

class ZMQBroadcastSubscriber(SThread):
    def __init__(self, context, zmq_context, zmq_endpoint):
        SThread.__init__(self)
        self.context =  context
        self.zmq_endpoint = zmq_endpoint
        self.zmq_cntx = zmq_context


    def run(self):


        sock = self.zmq_cntx.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, '')
        sock.connect(self.zmq_endpoint)

        while True:

            dataz = sock.recv()

            data = zlib.decompress(dataz)


            logging.debug("Incomming data from ["+self.zmq_endpoint+"]: "+data)
            d = json.loads(data)
            for k in d.keys():
                if not (k == "cmd"):
                    self.context.getProcGraph().updateRegistry(d[k])
                else:

                    logging.debug('Command dedected: '+str(d[k]))
                    if(d[k]["type"] == "arp"):
                        pkt_id = d[k]["id"]
                        pkt = self.context.get_pkt_from_shelve(pkt_id)
                        for p in pkt:
                            ep = d[k]["reply-to"]
                            p[0]["state"] = "ARP_OK"
                            exdisp = self.context.getExternalDispatch()
                            logging.debug("Sending ARP response: "+json.dumps(p))
                            exdisp.send_to_ep(p, ep)

class BroadcastListener(Thread):

    def __init__(self, context, port, zmq_context=None):
        Thread.__init__(self)
        self.__stop = Event()
        self.__port = int(port)
        self.context = context
        self.bclist = {}
        self.zmq_context = zmq_context
        pass

    def run(self):
        logging.info("Starting broadcast listener on port "+str(self.__port))
        sok = socket(AF_INET, SOCK_DGRAM)
        try:
            sok.bind(('', self.__port))
        except Exception as er:
            logging.warn("Broadcast listener disabled (another is already running)")
            sok.close()
            return

        sok.settimeout(5)
        while 1:
            try:
                data, wherefrom = sok.recvfrom(4096, 0)

            except(timeout):
                #logging.debug("Timeout")
                if self.stopped():
                    logging.debug("Exiting thread")
                    break
                else:
                    continue
            logging.debug("Broadcast received from: "+repr(wherefrom))
            logging.debug("Broadcast data: "+data)
            #datam = hashlib.md5(data).hexdigest()
            #logging.debug("MD5 data: "+datam)
            self.handle(data,wherefrom)

            #reply = self.__context.dumpRegistry()
            #sok.sendto(reply, wherefrom)
            #if not ( wherefrom[0] in self.tested):
            #    self.handle(data,wherefrom)

    def handle(self, data, wherefrom):
        context = self.context
        zmq_context = self.zmq_context
        try:
            #pass
            #self.tested[wherefrom[0]] = True
            d = json.loads(data)

            for ep in d:
                if not ep["host"] == self.context.getUuid() and not ep["ep"] in self.context.peers.keys():
                    zmqsub = ZMQBroadcastSubscriber(context, zmq_context, ep["ep"])
                    zmqsub.start()
                    context.peers[ep["ep"]] = ep["host"]
                    context.addThread(zmqsub)

            #for k in d.keys():
            #    self.context.getProcGraph().updateRegistry(d[k])

            #uid = d["uid"]
            #if not uid in self.bclist:
            #    logging.info("Discovered new peer: "+uid)
            #    logging.debug("New peer data: "+data)

            #self.bclist[uid] = d
            #port = int(d["comms"][0]["port"])
            #client = tftpy.TftpClient(wherefrom[0], port)
            #filename = "sample_"+wherefrom[0]+".jpg"
            #client.download('sample.jpg', filename)
            #self.tested[wherefrom[0]] = True

        except:
            logging.error("Broadcast receiving JSON error.")
            logging.debug("##############")
            logging.debug(data)
            logging.debug("##############")
            pass



    def stop(self):
        self.__stop.set()

    def stopped(self):
        return self.__stop.isSet()

class Broadcaster(SThread):
    def __init__(self, context, port=UDP_BROADCAST_PORT, rate=30):
        SThread.__init__(self)
        self.__port = int(port)
        self.__rate = rate
        self.context = context
        pass

    def run(self):
        logging.debug("Shouting presence to port "+str(self.__port)+" at rate "+str(self.__rate))
        #sok = socket(AF_INET, SOCK_DGRAM)
        #sok.bind(('', 0))
        #sok.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        #data = self.__context.getUuid() + '\n'
        #data = self.__context.getPeer().getJSON()


        while 1:
            #Only announce zmq point
            #data = "tcp://"+str(self.context.get_local_ip())+":"+str(PmkShared.ZMQ_PUB_PORT)
            data = '[{"host" : "'+self.context.getUuid()+'", "ep" : "'+self.context.get_our_pub_ep("tcp")+'"}]'
            self.announce(data, self.__port)
            time.sleep(self.__rate)

            #sok.sendto(data, ('<broadcast>', UDP_BROADCAST_PORT))
            #time.sleep(self.__rate)

            #data = self.context.getProcGraph().dumpExternalRegistry()

            # if self.context.getProcGraph().isRegistryModified():
            #     self.context.getProcGraph().ackRegistryUpdate()
            #     self.announce(data)
            #     time.sleep(2)
            # else:
            #     #time.sleep(self.__rate)
            #     time.sleep(2)
            #     #self.announce(data, self.__port)
            #     connection_string = "tcp://"+str(self.context.get_local_ip())+":PORT"
            #     self.announce(connection_string, self.__port)


            if self.stopped():
                break
            else:
                continue

    def announce(self, msg, port=UDP_BROADCAST_PORT):
        sok = socket(AF_INET, SOCK_DGRAM)
        sok.bind(('', 0))
        if self.context.getAttributeValue().broadcast:
            sok.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
            sok.sendto(msg, ('<broadcast>', port))

        for sn in self.context.getSupernodeList():
            sok.sendto(msg, (sn, port) )
            time.sleep(1)
        pass

class FileServer(Thread):
    def __init__(self, context, port, root="./rx/"):
        Thread.__init__(self)
        self.__stop = Event()
        self.__port = port
        self.__root = root
        self.__context = context

        self.__ensure_dir(self.__root)
        pass

    def run(self):
        logging.info("Starting file server on port "+str(self.__port)+" at root "+str(self.__root))

        self.__server = tftpy.TftpServer(self.__root)
        self.__server.listen("0.0.0.0", TFTP_FILE_SERVER_PORT, 10)

    def __ensure_dir(self, f):
        d = os.path.dirname(f)
        if not os.path.exists(d):
            os.makedirs(d,mode=0775)
            os.chmod(d,0775)

    def stop(self):
        self.__stop.set()
        self.__server.stop()

    def stopped(self):
        return self.__stop.isSet()



#class RendezvousServer(Thread):
#
#
#    def __init__(self, context, port):
#        Thread.__init__(self)
#        self.__stop = Event()
#        self.__port = port
#        self.__context = context
#        self.poolqueue = {}
#
#    def run(self):
#        logging.info("Starting Rendezvous server on port "+str(self.__port))
#        sok = socket(AF_INET, SOCK_DGRAM)
#        sok.bind(('', self.__port))
#        sok.settimeout(5)
#        while 1:
#            try:
#                data, addr = sok.recvfrom(32)
#            except timeout:
#                logging.debug("RendezvousServer Timeout")
#                if self.stopped():
#                    logging.debug("Exiting RendezvousServer thread")
#                    break
#                else:
#                    continue
#
#            logging.info("Connection from %s:%d" % addr)
#            pool = data.strip()
#            sok.sendto( "ok "+pool, addr )
#            data, addr = sok.recvfrom(2)
#            if data != "ok":
#                continue
#            logging.info("Request received for pool: ", pool)
#            try:
#                a, b = self.poolqueue[pool], addr
#                sok.sendto( self.addr2bytes(a), b )
#                sok.sendto( self.addr2bytes(b), a )
#                logging.info("Linked", pool)
#                del self.poolqueue[pool]
#            except KeyError:
#                self.poolqueue[pool] = addr
#
#    def addr2bytes( self, addr ):
#
#        """Convert an address pair to a hash."""
#        host, port = addr
#        try:
#            host = socket.gethostbyname( host )
#        except (socket.gaierror, socket.error):
#            raise ValueError, "Invalid host"
#        try:
#            port = int(port)
#        except ValueError:
#            raise ValueError, "Invalid port"
#        bytes  = socket.inet_aton( host )
#        bytes += struct.pack( "H", port )
#        return bytes
#
#    def stop(self):
#        self.__stop.set()
#
#    def stopped(self):
#        return self.__stop.isSet()
#
#
#class HoleComm(Thread):
#
#    def __init__(self, context, server, port, link):
#        Thread.__init__(self)
#        self.__stop = Event()
#        self.__port = port
#        self.__context = context
#        self.__server = server
#        self.__link = link
#
#    def run(self):
#
#        master = (self.__server, int(self.__port))
#
#        sockfd = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
#        sockfd.sendto( self.__link, master )
#        data, addr = sockfd.recvfrom( len(self.__link)+3 )
#        if data != "ok "+self.__link:
#            print >>sys.stderr, "unable to request!"
#            sys.exit(1)
#        sockfd.sendto( "ok", master )
#        print >>sys.stderr, "request sent, waiting for parkner in pool '%s'..." % self.__link
#        data, addr = sockfd.recvfrom( 6 )
#
#        target = self.bytes2addr(data)
#        print >>sys.stderr, "connected to %s:%d" % target
#
#        while True:
#            rfds,_,_ = select( [0, sockfd], [], [] )
#            if 0 in rfds:
#                data = sys.stdin.readline()
#                if not data:
#                    break
#                sockfd.sendto( data, target )
#            elif sockfd in rfds:
#                data, addr = sockfd.recvfrom( 1024 )
#                sys.stdout.write( data )
#
#        sockfd.close()
#
#    def bytes2addr(self, bytes ):
#        """Convert a hash to an address pair."""
#        if len(bytes) != 6:
#            raise ValueError, "invalid bytes"
#        host = socket.inet_ntoa( bytes[:4] )
#        port, = struct.unpack( "H", bytes[-2:] )
#        return host, port
