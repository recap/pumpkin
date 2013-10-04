#! /usr/bin/python
#####################################################################
##parameters to run local
##--server localhost --queue global/config --user guest --pass ******* --moduledir ./tasks/ --testmodule TestStart
#################################

import time
import os.path
import sys
import subprocess
import socket
import uuid


###############NEEDED BY PLUGINS#############
import commands
import urllib2
import base64
import tarfile
import shutil
import random
import string
#############################################

import plugin
import os
import imp
import pika
import rdflib
import argparse
import threading
import re
import python_webdav.client as webdav_client

from lxml import etree

class Dependency:
    file        = ""
    copyto      = "./deps/"

class DataServer:
    protocol    = ""
    url         = ""
    path        = ""
    metric      = 0
    user        = ""
    password    = ""
    params      = ""
    name        = ""

class ExchangeServer:
    type        = ""
    name        = ""
    url         = ""
    params      = ""
    user        = ""
    password    = ""
    vhost       = ""
    port        = ""

class ModuleLibrary:
    protocol    = ""
    name        = ""
    url         = ""
    path        = ""
    params      = ""
    user        = ""
    password    = ""

class DepLibrary:
    protocol    = ""
    name        = ""
    url         = ""
    path        = ""
    params      = ""
    user        = ""
    password    = ""

class Module:
    name            = ""
    pluginname      = ""
    context         = ""
    tag             = ""
    instance        = 0
    type            = "PythonPlugin"
    partofworkflow  = ""
    version         = ""
    inputports      = []
    outputports     = []
    dependencies    = []
    parameters      = {}
    def __init__(self):
        self.inputports = []
        self.outputports = []
        self.dependencies = []
        self.parameters = {}

class Port:
    name            = ""
    id              = ""
    direction       = ""
    type            = ""
    partition       = "none"
    callback        = None
    exchange        = 'none'
    state           = 'new' #new; used (after first message); closed

servers     = []
exservers   = []
modulelib   = []
depmodulelib = []
moduledir = None
current_module = None
max_tries   = 30
max_loads   = 5
VERSION = "0.1.8d-nocleanup"

def search(plugindir):
    for root, dirs, files in os.walk(plugindir):
        for fname in files:
            modname = os.path.splitext(fname)[0]
            print(modname)
            print(os.path.join(root, fname))
            try:
                module=imp.load_source(modname,os.path.join(root,fname))
            except Exception: continue

def ensure_dir(f):
    d = os.path.dirname(f)
    print "Path: ",d
    if not os.path.exists(d):
        print "Does not exist"
        os.makedirs(d)

def plug_direct(modulename):
    module = Module()
    module.name             = modulename
    module.context          = "stub_ctx"
    module.instance         = "stub_ins"
    module.type             = "stub_type"
    module.partofworkflow   = "None"
    module.version          = "000"


    plugin.plugins = []
    try:
        imp.load_source(module.name,"./modules/"+module.name+".py")
    except Exception:
        print "Loading Error ", Exception

    first_plugin = plugin.plugins[0]
    if first_plugin.__class__.__name__ == "PluginType":
        current_module = first_plugin(module, exservers, servers, connection)
        #current_module.func(module)
        current_module.on_load()
        th_run = threading.Thread(target=current_module.run)
        current_module.threads.append(th_run)
        th_run.start()
        for t in current_module.threads:
            t.join()
        current_module.internal_unload()
        


def load_module(module):
    for el in module.dependencies:
        ensure_dir(el.copyto)
        print "Found Dep: "+el.file
        for dlib in depmodulelib:
            if dlib.protocol == "webdav":
                wclient = webdav_client.Client(dlib.url, webdav_path = dlib.path)
                wclient.set_connection(username=dlib.user, password=dlib.password)
                wclient.download_file(el.file,dest_path=el.copyto)
                epath = el.copyto + el.file
                os.chmod(epath, 0755)
                break
            if dlib.protocol == "local":
                lfile = dlib.path+"/"+el.file
                print "FILE: "+lfile
                if(os.path.exists(lfile)):
                    shutil.copy(lfile,el.copyto)
                    epath = el.copyto + el.file
                    os.chmod(epath, 0755)
                break

    plugin.plugins = []

    try:
        print "[DEBUG] Loading "+moduledir+"/"+module.pluginname
        imp.load_source(module.name,moduledir+"/"+module.pluginname)
    except Exception:
        print "[ERROR] Loading Error ",Exception
        sys.exit(1)
        return

    first_plugin = plugin.plugins[0]
    if first_plugin.__class__.__name__ == "PluginType":
        if(testing == True):
            current_module = first_plugin(module, exservers, servers,testmode=True)
        else:
            current_module = first_plugin(module, exservers, servers,testmode=False)
        #current_module.func(module)
        current_module.on_load()
        current_module._write_event("loaded")
        th_run = threading.Thread(target=current_module.run)
        current_module.threads.append(th_run)
        th_run.start()
        for t in current_module.threads:
            t.join()
        current_module.internal_unload()
        
        current_module._write_event("unloaded")
        


def plug_remote(module):
    ensure_dir(moduledir)
    for el in modulelib:
        if el.protocol == "webdav":
            wclient = webdav_client.Client(el.url, webdav_path = el.path)
            wclient.set_connection(username=el.user, password=el.password)
            #wclient.download_file(module.name+".py",dest_path=moduledir)
            wclient.download_file(module.pluginname,dest_path=moduledir)
            break

    load_module(module)

    pass

def plug_local(module):    
    ensure_dir(moduledir)
    #mpath = moduledir+"/"+module.name+".py"
    mpath = moduledir+"/"+module.pluginname
    if(os.path.exists(mpath)):
        load_module(module)
    else:
        print "[ERROR] module path does not exist: "+mpath+"\n"
    pass

def start_me(config, code, local=False):
    
    doc = etree.fromstring(config)
    result = doc.xpath('//Server')

    for el in result:
        s = DataServer()
        s.name = el.text
        s.metric = el.get("metric")
        #s.url = el.get("url")
        count = 0
        for par in el.get("url").split("/"):
            if count < 3:
                s.url = s.url + "/" + par
            else:
                s.path = s.path +"/"+par
            count = count + 1
        s.url = s.url[1:]
        print "DASD: ",s.url," ",s.path
        s.protocol = el.get("protocol")
        s.params = el.get("param")
        #print s.params
        for i in s.params.split(";"):
            if i.partition('=')[0] == "user":
                s.user = i.partition('=')[2]
            if i.partition('=')[0] == "pwd":
                s.password = i.partition('=')[2]
        servers.append(s)

    result = doc.xpath('//ExServer')

    for el in result:
        s = ExchangeServer()
        s.name = el.text
        s.type = el.get("type")
        s.url = el.get("url")
        s.params = el.get("param")
        s.port = el.get("port")
        #print s.params
        for i in s.params.split(";"):
            if i.partition('=')[0] == "user":
                s.user = i.partition('=')[2]
            if i.partition('=')[0] == "pwd":
                s.password = i.partition('=')[2]
            if i.partition('=')[0] == "vhost":
                s.vhost = i.partition('=')[2]
        exservers.append(s)

    result = doc.xpath('//Library')

    for el in result:
        s = ModuleLibrary()
        s.name = el.text
        s.protocol = el.get("protocol")
        
        count = 0
        for par in el.get("url").split("/"):
            if count < 3:
                s.url = s.url + "/" + par
            else:
                s.path = s.path +"/"+par
            count = count + 1
        s.url = s.url[1:]        
        s.params = el.get("param")
        #print s.params
        for i in s.params.split(";"):
            if i.partition('=')[0] == "user":
                s.user = i.partition('=')[2]
            if i.partition('=')[0] == "pwd":
                s.password = i.partition('=')[2]
            
        modulelib.append(s)

    result = doc.xpath('//DepLibrary')

    for el in result:
        s = DepLibrary()
        s.name = el.text
        s.protocol = el.get("protocol")
        order = None
        if(el.get("order")):
            order = int(el.get("order"))

        if(s.protocol == "local"):
            s.url = el.get("url")
            s.path = s.url
        else:
            count = 0
            for par in el.get("url").split("/"):
                if count < 3:
                    s.url = s.url + "/" + par
                else:
                    s.path = s.path +"/"+par
                count = count + 1
            s.url = s.url[1:]
            s.params = el.get("param")
            #print s.params
            for i in s.params.split(";"):
                if i.partition('=')[0] == "user":
                    s.user = i.partition('=')[2]
                if i.partition('=')[0] == "pwd":
                    s.password = i.partition('=')[2]

        print "ORDER: "+ s.name + " "+str(order)+"\n"
        
        if(order):
            depmodulelib.insert(order, s)
        else:
            depmodulelib.append(s)

    #context = etree.iterwalk(doc, tag="Port")
    #for action, elem in context :
    #    print action, elem.tag, elem.text

    module = Module()
    module.name             = doc.xpath('//Module/Name')[0].text
    module.pluginname       = doc.xpath('//Module/PluginName')[0].text
    module.context          = doc.xpath('//Module/Context')[0].text
    module.tag              = doc.xpath('//Module/Tag')[0].text
    module.instance         = doc.xpath('//Module/Instance')[0].text
    module.type             = doc.xpath('//Module/Type')[0].text
    module.partofworkflow   = doc.xpath('//Module/PartOfWorkflow')[0].text
    module.version          = doc.xpath('//Module/Version')[0].text
    ports                   = doc.xpath('//Module/Port')
    deps                    = doc.xpath('//Module/Dependency')
    params                  = doc.xpath('//Module/Parameter')


    pyfile =  args.moduledir+"/"+ module.pluginname
    f = open(pyfile, 'w')
    f.write(code)
    f.close()


    for el in ports:
        p = Port()
        p.name      = el.text
        p.id        = el.get('id')
        p.direction = el.get('direction')
        p.type      = el.get('type')
        p.partition = el.get('partition')
        p.exchange  = el.get('exchange')

        if p.direction == "input" :
            module.inputports.append(p)
        else:
            module.outputports.append(p)

    for pt in module.outputports:
        print "PORT:", pt.name

    for p in params:
        k = p.get('name')
        v = p.text
        print "A PARAM ",k," ",v
        module.parameters[k] = v

    if(args.pass_on != None):
        cntr = 0
        for param in args.pass_on.split(","):
            cntr += 1
            module.parameters[str(cntr)] = param


    for d in deps:
        dep = Dependency()
        dep.file = d.text
        if(d.get("copyto")):
            dep.copyto = d.get("copyto")
        if d.text:
            module.dependencies.append(dep)

    # load and start running module
    if(local == False):
        plug_remote(module)
    else:
        plug_local(module)

    
    #for sr in servers:
    #    print sr.name
    #for el in exservers:
    #    print el.name

    #end start_me

def __d_callback(ch, method, properties, body):
    pass





#def my_thread_1():
#    print "In thread 1"
#    time.sleep(10)
#    pass
#def my_thread_2():
#    print "In thread 2"
    #sleep(10)
#    pass

# start

#thread.start_new_thread(my_thread_1,())
#my_thread_2()

#sys.exit(0)

parser = argparse.ArgumentParser(description='Harness for Datafluo jobs')
parser.add_argument('--server', action='store', dest='xserver', default="localhost",
                   help='xchange server from where to download configuration')
parser.add_argument('--port', action='store', dest='xserverport', default="5572",
                   help='xchange server port')
parser.add_argument('--vhost', action='store', dest='vhost', default="datafluo",
                   help='xchange server virtual host')
parser.add_argument('--user', action='store', dest='user', default="guest",
                   help='username for xchange server')
parser.add_argument('--password', action='store', dest='secret', default="******",
                   help='xchange password')
parser.add_argument('--queue', action='store', dest='gqueue', default="global/config",
                   help='global configuration queue on xserver')
parser.add_argument('--piggyback', action='store', dest='piggyback', default=False,
                  help='embed pyhton plugin with xml description message')
parser.add_argument('--moduledir', action='store', dest='moduledir', default="/tmp/",
                   help='local module repository cache')
parser.add_argument('--testmodule', action='store', dest='testmodule', default=None,
                   help='test a module')
parser.add_argument('--max_tries', action='store', dest='max_tries', default=30,
                   help='maximum times to check the message exchange')
parser.add_argument('--max_loads', action='store', dest='max_loads', default=5,
                   help='maximum plugin loads')
parser.add_argument('--copy_from', action='store', dest='copy_from', default=None,
                   help='a source queue')
parser.add_argument('--copy_to', action='store', dest='copy_to', default=None,
                   help='a destination queue')
parser.add_argument('--pass_on', action='store', dest='pass_on', default=None,
                   help='pass parameters on to plugin. Paramters are passed on as csv. They can be accessed through the plugin using self._get_parameter("1") etc')
parser.add_argument('--bootstrap', action='store', dest='workflow', default=None,
                   help='pyharness starts workflow by queuing job first job. No plugins are loaded.')



#parser.add_argument('--version', action='version', version='%(prog)s 0.1.4c')
parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
args = parser.parse_args()
args.uid = str(socket.gethostname())+"-"+str(uuid.uuid4())[:8]

moduledir = args.moduledir
testing = False

max_tries = int(args.max_tries)
max_loads = int(args.max_loads)


def __cmd_callback(ch, method, properties, body):     
     message_props = {}
     abody = body.splitlines(True)
     for mline in abody:         
         (key, val) = mline.split("=")
         message_props[key] = val
     if(message_props["cmd"] == "stop"):
         print "[DEBUG] Exiting with command"
         args.channel.queue_delete(queue=args.uid+"-in")
         sys.exit(0)
     pass

if(args.testmodule == None):
    testing = True
    args.credentials = pika.PlainCredentials(args.user, args.secret)
    args.connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.xserver,port=int(args.xserverport),credentials=args.credentials,virtual_host=args.vhost))
    args.channel = args.connection.channel()
    args.channel.basic_qos(prefetch_count=1)
    args.channel.queue_declare(queue=args.uid+"-in")
    args.channel.basic_consume(__cmd_callback,
                               queue=args.uid+"-in",
                               no_ack=True)
    trycounter = 0
    loadedmod  = 0
    while args.connection.is_open:
        method, _, body = args.channel.basic_get(queue=args.gqueue, no_ack=True)
        if method.NAME == 'Basic.GetEmpty':
            trycounter += 1

            if trycounter > max_tries & max_tries > 0:
                break
            print "Nothing to do going to sleep for a while"
            time.sleep(5)
        else:
            trycounter = 0
            loadedmod += 1
            m = re.search('##START-CONF(.+?)##END-CONF', body, re.S)
            if m:
                conf = m.group(1).replace("##","")
                print "Starting a Task"
                start_me(conf,  body, local=True)

            if loadedmod >= max_loads & max_loads > 0:
                break
else:
    testing = True
    testmodule = moduledir+"/"+args.testmodule+".py"
    print "[INFO] testing "+testmodule
    print "\n"
    filestring = open(testmodule, 'r').read()
    m = re.search('##START-CONF(.+?)##END-CONF', filestring, re.S)
    if m:
        conf = m.group(1).replace("##","")
        try:
            start_me(conf, filestring, local=True)
        except:
            print "[ERROR]"
            sys.exit(0)
    else:
        print "[ERROR] no conf found in py file"



###################BOOTSTRAP#############################################

#if(args.workflow != None):
#     if(os.path.exists(args.workflow)):
#         g = rdflib.Graph()
#         result = g.parse(args.workflow, format='n3')
#         print len(g)
#         for stmt in g:
#            print stmt
#         sys.exit(0)
#     else:
#         print "[ERROR] workflow file "+args.workflow+" does not exist."
#         sys.exit(1)


#########################################################################




####################STUB#####################
#plug_direct("AgadirFolding")
#plug_direct("RangeGenerator")
#plug_direct("AgadirFoldingEnd")
#name, jobs, consumers = channel.queue_declare(queue=args.gqueue, passive=True)
#method, _, body = channel.basic_get(queue=args.gqueue, no_ack=False)
#channel.basic_consume(__d_callback, queue=args.gqueue,
#                               no_ack=False)
#channel.start_consuming()
#status = channel.queue_declare(queue=args.gqueue, passive=True)
#print status.method.message_count," ",status.method.consumer_count
#print status
#sys.exit(0)
#############################################

#if(args.copy_from != None):
#    src_queue = args.copy_from
#    dest_queue = args.copy_to
#    credentials = pika.PlainCredentials(args.user, args.secret)
#    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.xserver,port=int(args.xserverport),credentials=credentials,virtual_host=args.vhost))
#    channel = connection.channel()
#    while True:
#        method, properties, body = channel.basic_get(queue=src_queue, no_ack=True)
#        if method.NAME == 'Basic.GetEmpty':
#            sys.exit(0)
#        print "[DEBUG] swinging " ,body," to ",dest_queue
#        hrds = pika.BasicProperties(headers={"type": "webdav"})
#        channel.basic_publish(exchange='', routing_key=dest_queue, body=body, properties=hrds)
#
#
#if(args.testmodule == None):
#    testing = True
#    credentials = pika.PlainCredentials(args.user, args.secret)
#    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.xserver,port=int(args.xserverport),credentials=credentials,virtual_host=args.vhost))
#    channel = connection.channel()
#    channel.basic_qos(prefetch_count=1)
#    trycounter = 0
#    loadedmod  = 0
#    while connection.is_open:
#        method, _, body = channel.basic_get(queue=args.gqueue, no_ack=True)
#        if method.NAME == 'Basic.GetEmpty':
#            trycounter += 1
#
#            if trycounter > max_tries & max_tries > 0:
#                break
#            print "Nothing to do going to sleep for a while"
#            time.sleep(5)
#        else:
#            trycounter = 0
#            loadedmod += 1
#            if(args.piggyback == 'True'):
#                print "[DEBUG] in piggyback"
#                m = re.search('##START-CONF(.+?)##END-CONF', body, re.S)
#                if m:
#                    conf = m.group(1).replace("##","")
#                    print "Starting a Task"
#                    start_me(conf,  body, local=True)
#            else:
#                print "Starting a Task"
#                start_me(body)
#
#            if loadedmod >= max_loads & max_loads > 0:
#                break
#            #break
#else:
#    testing = True
#    testmodule = moduledir+"/"+args.testmodule+".py"
#    print "[INFO] testing "+testmodule
#    print "\n"
#    filestring = open(testmodule, 'r').read()
#    m = re.search('##START-CONF(.+?)##END-CONF', filestring, re.S)
#    if m:
#        conf = m.group(1).replace("##","")
#        try:
#            start_me(conf, True)
#        except:
#            print "[ERROR]"
#            sys.exit(0)
#    else:
#        print "[ERROR] no conf found in py file"




print "Exiting"
