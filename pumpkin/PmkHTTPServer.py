__author__ = 'reggie'


import re, json
from socket import *


import PmkSeed
import PmkContexts

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
        HOST_NAME = 'localhost' # !!!REMEMBER TO CHANGE THIS!!!
        PORT_NUMBER = HTTP_TCP_PORT # Maybe set this to 9000.
        server_class = BaseHTTPServer.HTTPServer
        httpd = server_class((HOST_NAME, PORT_NUMBER), MyHandler)
        log.info("Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER))
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        httpd.server_close()
        log.info("Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER))

class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_HEAD(s):
        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.end_headers()
    def do_GET(s):
        """Respond to a GET request."""
        log.debug("Request "+s.path)
        context = PmkContexts.MainContext(None)

        if s.path =="/graph.json":
            rep = context.getProcGraph().dumpGraph()
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            s.wfile.write(str(rep))

        if s.path =="/packets.json":
            s.send_response(200)
            s.send_header("Content-type", "application/json")
            s.end_headers()
            rep = json.dumps(dict(context.getPktShelve()))

            s.wfile.write(str(rep))

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
                #log.debug("Timeout")
                if self.stopped():
                    log.debug("Exiting thread "+self.__class__.__name__)
                    break
                else:
                    #log.debug("HTTP timeout")
                    continue
            log.debug('HTTP Connection address:'+ str(addr))

            data = conn.recv(HTTP_BUFFER_SIZE)
            if not data: break
            #log.debug("HTTP data: "+data)
            if "favicon" in data:
                conn.close()
                continue

            rep = ""
            #log.debug(data)
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
             #   log.debug(rep)
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
            #    log.warn("Trying to invoke module with HTTP: "+h.module+" but doe not exist.")
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
            #    log.warn("Trying to invoke module with HTTP: "+h.module+" but doe not exist.")

            #conn.close()
        pass


