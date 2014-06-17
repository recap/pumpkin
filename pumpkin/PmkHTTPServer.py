__author__ = 'reggie'


import re, json
from socket import *


import PmkSeed
import PmkContexts
import time
import datetime
import base64

from PmkShared import *


import BaseHTTPServer


CONTEXT = None

class Head(object):

    def __init__(self, header):
        self.params_hash = {}
        self.params_list = []
        self.params_string = ""
        hd = re.findall(r"(GET|POST) (?P<value>.*?)\s", header)
        try:
            if len(hd[0][1].split("?")) > 1:
                parlist = hd[0][1].split("?")[1].split("&")
                for p in parlist:
                    key = p.split("=")[0]
                    value = p.split("=")[1]
                    self.params_hash[key] = value
                    self.params_list.append(value)
                    self.params_string += str(value)+","

                self.params_string = self.params_string[:-1]

            serv = hd[0][1].split("?")[0].split("/")
            self.module = serv[len(serv)-2]
            self.method = serv[len(serv)-1]
        except:
            self.module = None
            self.method = None


class HttpServer(SThread):
     def __init__(self, context):
         SThread.__init__(self)
         self.context = context

         pass

     def run(self):
        HOST_NAME = '0.0.0.0' # !!!REMEMBER TO CHANGE THIS!!!
        PORT_NUMBER = HTTP_TCP_PORT # Maybe set this to 9000.
        server_class = BaseHTTPServer.HTTPServer
        httpd = server_class((HOST_NAME, PORT_NUMBER),  MyHandler)
        logging.info("Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER))
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        httpd.server_close()
        logging.info("Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER))

class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_HEAD(s):
        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.end_headers()
    def do_GET(s):
        """Respond to a GET request."""
        logging.debug("Request "+s.path)
        context = PmkContexts.MainContext(None)

        if s.path == "/d3":
            s.send_response(200)
            s.send_header("Content-type", "text/html")
            s.end_headers()

            context.getProcGraph().dumpGraphToFile("force.json")
            with open("force.html") as f:
                content = f.read()
            f.close()

            s.wfile.write(content)
        if s.path == "/force.json":
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()

            #context.getProcGraph().dumpGraphToFile("force.json")
            with open("force.json") as f:
                content = f.read()
            f.close()
            s.wfile.write(content)


        if s.path =="/graph.json":
            rep = context.getProcGraph().dumpGraph()
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            s.wfile.write(str(rep))

        if s.path =="/extgraph.json":
            rep = context.getProcGraph().dumpExternalRegistry()
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            s.wfile.write(str(rep))

        if s.path =="/txrx.json":

            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            rx_size = context.get_rx_size()
            tx_size = context.get_tx_size()

            rep = '{"rx_size" : "'+str(rx_size)+'", "tx_size" : "'+str(tx_size)+'"}'

            s.wfile.write(str(rep))

        if s.path =="/routing.json":
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            rep  = context.getProcGraph().dumpRoutingTable()

            s.wfile.write(str(rep))

        if s.path =="/packets.json":
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            rep = json.dumps(dict(context.getPktShelve()))

            s.wfile.write(str(rep))

        if s.path == "/counters.json":
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            rep = ""
            for x in PmkSeed.iplugins.keys():
               klass = PmkSeed.iplugins[x]
               rep = rep + "\n" + klass.get_state_counters()
            s.wfile.write(str(rep))

        if s.path == "/stats.json":
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            tm = time.time()
            ip = str(context.get_local_ip())
            if not ip:
                ip = "NONE"
            rep = "["
            rep += '{"timestamp" : '+str(tm)+',"ip" : "'+ip+'"},\n'
            rep = rep + "{"
            for x in PmkSeed.iplugins.keys():
               klass = PmkSeed.iplugins[x]
               ent = "\""+klass.get_name() +"\" : \""+ str(klass.is_enabled())+"\""
               rep = rep + ent + ","

            rep = rep[0:len(rep)-1]
            rep = rep + "},"

            total_in = 0
            total_out = 0
            for x in PmkSeed.iplugins.keys():
               klass = PmkSeed.iplugins[x]
               rep = rep + "\n" + klass.get_state_counters()
               tin, tout = klass.get_all_counters()
               total_in += tin
               total_out += tout
            rep += ',\n'
            rep = rep + '{"total_in":'+str(total_in)+',"total_out":'+str(total_out)+'}'
            rep += ","

            rx_size = context.get_rx_size()
            tx_size = context.get_tx_size()

            rep += '\n'
            rep += '{"rx_size" : "'+str(rx_size)+'", "tx_size" : "'+str(tx_size)+'"}'

            rep += "]"

            s.wfile.write(str(rep))

        if "status" in s.path:
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()

            cmd_queue = context.get_cmd_queue()

            rep = ""
            parts = s.path.split("?")
            pkt_id = parts[1]
            oep = context.get_our_endpoint("tcp://")
            if oep:
                cmd_str = '"cmd" : {"type" : "arp", "id" : "'+pkt_id+'", "reply-to" : "'+oep[0]+'"}'
                logging.debug("Queueing command: "+cmd_str)
                cmd_queue.put(cmd_str)
            #pkt = context.get_pkt_from_shelve(pkt_id)

            rep = "["
            for pkt in context.get_pkt_from_shelve(pkt_id):
                rep += json.dumps(pkt)+","
            for pkt in context.get_pkt_from_shelve2(pkt_id):
                rep += json.dumps(pkt)+","
            if len(rep) > 1:
                rep = rep[:-1]
                rep += "]"
            else:
                rep = "{unavailable_info}"

            s.wfile.write(str(rep))

        if "stop" in s.path:
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()

            parts = s.path.split("?")
            func_name = parts[1]
            rep = context.getProcGraph().stopSeed(func_name)
            if rep:
                s.wfile.write("OK")
            else:
                s.wfile.write("ERROR")

        if "start" in s.path:
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()

            parts = s.path.split("?")
            func_name = parts[1]
            rep = context.getProcGraph().startSeed(func_name)

            if rep:
                s.wfile.write("OK")
            else:
                s.wfile.write("ERROR")



        if "submit" in s.path:
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()

            parts = s.path.split("?")
            container = "None"
            #print parts[1]
            pkt_dec = base64.decodestring(parts[1])
            logging.debug("Decoded packet: "+pkt_dec)
            # pktd = parts[1]
            pktdj = json.loads(pkt_dec)
            pkt_len = len(pktdj)

            stag_p = pktdj[pkt_len - 1]["stag"].split(":")

            pkt_id = None
            pkt = pktdj
            if pkt[0]["state"] == "MERGE":
                pkt_id= pkt[0]["ship"]+":"+pkt[0]["container"]+":"+pkt[0]["box"]+":"+pkt[0]["fragment"]+":M"
            else:
                pkt_id= pkt[0]["ship"]+":"+pkt[0]["container"]+":"+pkt[0]["box"]+":"+pkt[0]["fragment"]

            if len(stag_p) < 3:
                group = "A"
                type = stag_p[0]
                tag = stag_p[1]
            else:
                group = stag_p[0]
                type = stag_p[1]
                tag = stag_p[2]

            dt = datetime.datetime.now()
            context.getTx().put((group, tag,type,pktdj))


            logging.debug("Submit packet through http: "+pkt_id)
            rep = '{"packet_ref":'+pkt_id+', "timestamp":'+str(dt)+'}'

            s.wfile.write(rep)

            # s.wfile.write("<html><head><title>Pumpkin Web</title></head>")
            # s.wfile.write("<body><p>Submitted Packet: "+pkt_id+" "+str(dt)+"</p>")
            # s.wfile.write("</body></html>")



        if s.path == "/seeds.json":
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            s.wfile.write("[")

            rep = ""
            for x in PmkSeed.iplugins.keys():
               klass = PmkSeed.iplugins[x]
               rep += "{ \"name\" : \""+klass.get_fullname()+"\"}"
               rep += ","
            if len(rep) > 0:
                rep = rep[:-1]
            s.wfile.write(rep)
            s.wfile.write("]")

        if s.path == "/":
            s.send_response(200)
            s.send_header("Content-type", "text/html")
            s.end_headers()
            s.wfile.write("<html><head><title>Pumpkin Web</title></head>")
            s.wfile.write("<body><p>Loaded Seeds</p>")
            for x in PmkSeed.iplugins.keys():
               klass = PmkSeed.iplugins[x]
               s.wfile.write("<p>"+klass.get_name()+"</p>")
            s.wfile.write("</body></html>")

        #s.wfile.write("<html><head><title>Title goes here.</title></head>")
        #s.wfile.write("<body><p>This is a test.</p>")
        # If someone went to "http://something.somewhere.net/foo/bar/",
        # then s.path equals "/foo/bar/".
        #s.wfile.write("<p>You accessed path: %s</p>" % s.path)
        #s.wfile.write("</body></html>")



class HttpServer_OLD(SThread):


    def __init__(self, context):
        SThread.__init__(self)
        self.context = context
        pass

    def getSize(self, filestr):
        fileobject = open(filestr, 'rb')
        fileobject.seek(0,2) # move the cursor to the end of the file
        size = fileobject.tell()
        fileobject.close()
        return size

    def run(self):

        s = socket(AF_INET, SOCK_STREAM)
        s.settimeout(5)
        s.bind((HTTP_TCP_IP, HTTP_TCP_PORT))
        s.listen(1)
        while 1:
            try:
                conn, addr = s.accept()
            except timeout:
                #logging.debug("Timeout")
                if self.stopped():
                    logging.debug("Exiting thread "+self.__class__.__name__)
                    break
                else:
                    #logging.debug("HTTP timeout")
                    continue
            logging.debug('HTTP Connection address:'+ str(addr))

            data = conn.recv(HTTP_BUFFER_SIZE)
            if not data: break
            #logging.debug("HTTP data: "+data)
            if "favicon" in data:
                conn.close()
                continue

            rep = ""
            #logging.debug(data)
            h = Head(data)
            if not h.module:
             #   sf = self.getSize("./pumpkin/force.html")
             #   prot = "HTTP/1.1 200 OK\n" \
             #       "Content-Type: text/html; charset=utf-8\n"\
             #       "Content-Length:"+str(sf)+"\n"
             #   data = ""
             #   with open ("./pumpkin/force.html", "r") as myfile:
             #       data = str(myfile.readlines())
             #   rep = str(prot) + str(data)
             #   logging.debug(rep)
                rep = self.context.getProcGraph().dumpGraph()
                #self.context.getProcGraph().dumpGraphToFile("state.json")
            else:

                if h.module in PmkSeed.iplugins.keys():
                    klass = PmkSeed.iplugins[h.module]
                    if not h.params_string:
                        rep = getattr(klass, h.method)()
                    else:
                        rep = getattr(klass, h.method)(h.params_string)
                    #rt = klass.run(h.params_string)
                    #print rep
            #    conn.send(str(rt))
            #else:
            #    logging.warn("Trying to invoke module with HTTP: "+h.module+" but doe not exist.")
            #self.context.getProcGraph().dumpGraphToFile("./pumpkin/miserables.json")


            #print data

            conn.send(str(rep))
            #if h.module in PmkSeed.hplugins.keys():
            #    klass = PmkSeed.hplugins[h.module](self.context)
            #    klass.on_load()
            #    if not h.params_string:
            #        rt = getattr(klass, h.method)()
            #    else:
            #        rt = getattr(klass, h.method)(h.params_string)
            #    #rt = klass.run(h.params_string)
            #    print rt
            #    conn.send(str(rt))
            #else:
            #    logging.warn("Trying to invoke module with HTTP: "+h.module+" but doe not exist.")

            #conn.close()
        pass


