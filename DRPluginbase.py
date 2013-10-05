import base64
import datetime
import httplib
import os
import os.path
import shutil
import string
import sys
import tarfile
import threading
import time
import urllib2
from urlparse import urlparse

import pika
import python_webdav.client as webdav_client

plugins = []

    
class PluginType(type):
    def __init__(cls, name, bases, attrs):
        super(PluginType, cls).__init__(name, bases, attrs)
        print(cls, name, cls.__module__)
        if name != "PluginBase":
            plugins.append(cls)
    

class PluginBase(object):    
    __metaclass__ = PluginType

    tesmode     = False
    exservers   = None
    dataservers = None
    module      = None
    connection  = None
    channel     = None
    routing_keys = {}
    parameters = {}
    delivery_tags = {}
    threads     = []
    tag = None
    context = None
    errorlevel = 0


    
    def __init__(self, module, exservers=None, dataservers=None, connection=None, testmode=False):
        print "Init Base"
        self.parameters     = module.parameters
        self.exservers      = exservers
        self.dataservers    = dataservers
        self.module         = module
        self.connection     = connection
        self.tesmode        = testmode
        self.context        = module.context
        self.tag            = module.tag
        
        #if "context" in self.parameters:
        #    self.context        = self.parameters["context"]
        #else:
        #    print "WARNING: no context set"

        #if "tag" in self.parameters:
        #    self.tag        = self.parameters["tag"]
        #else:
        #    print "WARNING: no tag set"


        if connection == None:
            credentials = pika.PlainCredentials(exservers[0].user, exservers[0].password)
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=exservers[0].url, port=int(exservers[0].port), credentials=credentials, virtual_host=exservers[0].vhost))
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)
        else:
            if self.connection.is_open:
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=1)
            else:
                print "[ERROR] openeing connection to xserver\n"
           
        if(self._isParent()):
            for port in module.inputports:
                #print "input: " + port.name
                #queue_name = module.context + "/" + module.partofworkflow + "/" + module.name + "/" + module.instance + "/" + port.name
                queue_name = self.__gen_queue_name(port)
                #print queue_name
                ###TESTING###
                self.channel.queue_declare(queue=queue_name)
                if port.exchange != 'none':
                    self.channel.queue_bind(exchange=port.exchange,
                                            routing_key=queue_name,
                                            queue=queue_name)
                    self.routing_keys[port.exchange] = port
                self.routing_keys[queue_name] = port
                self.delivery_tags[queue_name] = 0;
            for port in module.outputports:
                #queue_name = module.context + "/" + module.partofworkflow + "/" + module.name + "/" + module.instance + "/" + port.name
                queue_name = self.__gen_queue_name(port)
                if(self.tesmode == True):
                    print "output: " + port.name
                    self.channel.queue_declare(queue=queue_name)
                    self.channel.exchange_declare(exchange=queue_name, type='fanout')
                    self.channel.queue_declare(queue=queue_name)
                    self.channel.queue_bind(exchange=queue_name,
                                            queue=queue_name)
                self.routing_keys[queue_name] = port
                

        #connection.close()
        #print exservers[0].name
        #print dataservers[0].name
        pass

    def _isParent(self):
        if(self.module.instance == "0"):
            return True;
        else:
            return False
        pass

    def _get_dep_filepath(self, dep_name):
        for el in dependencies:
            if(el.file == dep_name):
                dpath = el.copyto + el.file
                return dpath
        return None
    
    def _get_parameter(self, param):
        if param in self.parameters:
            return self.parameters[param]
        else:
            print "WARNING: no parameter " + param
            #return None
        pass
    def __get_ds(self, name):
        for ds in self.dataservers:
            if ds.name == name:
                return ds
        return None

    def __get_first_ds(self):
        return self.dataservers[0]
    
    def _ensure_dir(self, f):
        d = os.path.dirname(f)
        print "Path: ", d
        if not os.path.exists(d):
            print "Does not exist"
            os.makedirs(d)
        pass

    def _tar_to_gz(self, source, destination):
        t = tarfile.open(name=destination, mode='w:gz')
        t.add(source, os.path.basename(source))
        t.close()
        pass

    def _write_event(self, event):
        queue_name = "global/events"
        now = datetime.datetime.now()
        snow = str(now)
        message = "[" + snow + "][" + self.module.name + "] " + event
        self.channel.basic_publish(exchange="",
                                   routing_key=queue_name,
                                   body=message)

        pass    

    def _register_callback(self, callback, port):
        #queue_name = self.module.context + "/" + self.module.partofworkflow + "/" + self.module.name + "/" + self.module.instance + "/" + port.name
        queue_name = self.__gen_queue_name(port)
        port = self.routing_keys[queue_name]
        port.callback = callback
        #x = thread.start_new_thread(self.__callback_thread,(callback,queue_name))
        th = threading.Thread(target=self.__callback_thread, args=(callback, queue_name, ))
        self.threads.append(th)
        th.start()
        pass
    def __d_callback(self, ch, method, properties, body):
        if properties.headers != None and 'cmd' in properties.headers:
            if properties.headers["cmd"] == "eom":
                ch.stop_consuming()
                ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            #print body, method, ch.consumer_tags
            #print "SHOUT: ",method.routing_key
            port = self.routing_keys[method.routing_key]
            port.callback(body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        pass
    def __callback_thread(self, callback, queue_name):
        lcredentials = pika.PlainCredentials(self.exservers[0].user, self.exservers[0].password)
        lconnection = pika.BlockingConnection(pika.ConnectionParameters(host=self.exservers[0].url, port=int(self.exservers[0].port), credentials=lcredentials, virtual_host=self.exservers[0].vhost))
        lchannel = lconnection.channel()
        lchannel.basic_qos(prefetch_count=1)
        lchannel.basic_consume(self.__d_callback,
                               queue=queue_name,
                               no_ack=False)
        lchannel.start_consuming()
        lconnection.close()
        pass
    def _get_port(self, name):
        port = None
        #print "Looking for: ", name
        for p in self.module.inputports:
            if p.name == name:
                port = p
                #print "found: ", p.name
                break
        if port == None:
            #print name
            for p in self.module.outputports:
                if p.name == name:
                    port = p
                    #print "found: ", p.name
                    break

        if port == None:
            print "ERROR: found no port: ", name
        return port
    def _get_filename_from_url(self, url):
        return url.split('/')[-1]
    def _read_file_from_port(self, port, store_path="./data_files/", renameto="none"):
        store_path = store_path + "/"
        queue_name = self.__gen_queue_name(port)
        while self.connection.is_open:
            if(self.delivery_tags[queue_name] > 0):
                #print "[DEBUG] ACK delivey_tag " + str(self.delivery_tags[queue_name]) + " for " + queue_name + "\n"
                self.channel.basic_ack(int(self.delivery_tags[queue_name]))

            try:
                method, properties, body = self.channel.basic_get(queue=self.__gen_queue_name(port), no_ack=False)
            except:
                print "[ERROR] ", sys.exc_info()[0]
                self.errorlevel += 1

            if method.NAME == 'Basic.GetEmpty':
                self.delivery_tags[queue_name] = 0
                time.sleep(10)                
            else:
                #print "BODY: "+body
                self.delivery_tags[queue_name] = method.delivery_tag
                if properties.headers.has_key("cmd"):                    
                    if properties.headers["cmd"] == "eom":
                        print "[INFO] end of message received on " + self.__gen_queue_name(port) + "\n"
                        self.channel.basic_ack(self.delivery_tags[queue_name])
                        raise ValueError("EOM received")
                        self.channel.flush()
                        sleep(1)
                        self.channel.close()
                        return None
                    
                if properties.headers["type"] == "local":
                    file_name = body.split('/')[-1]
                    if(renameto != "none"):
                        file_name = renameto
                    fl_dst = store_path + "/" + file_name
                    #shutil.copyfile(body,fl_dst)
                    print "[INFO] local file: " + body
                    shutil.copy(body, fl_dst)
                    return fl_dst
                if properties.headers["type"] == "webdav":
                    message_props = {}
                    abody = body.splitlines(True)
                    for mline in abody:
                        print "LINE: " + mline + "\n"
                        (key, val) = mline.split("=")
                        message_props[key] = val
                    self._ensure_dir(store_path)
                    #store_name = properties.headers["store_name"]
                    #ds = self.__get_ds(store_name)
                    ds = self.__get_first_ds()
                    #print "data store: "+ds.url
                    #print "data store path: "+ds.path
                    #print "filename: " + message_props["filename"]
                    file_name = str(message_props["filename"])
                    file_name = file_name.rstrip()
                    wclient = webdav_client.Client(ds.url, webdav_path=ds.path)
                    wclient.set_connection(username=ds.user, password=ds.password)
                    wclient.download_file(file_name, dest_path=store_path)
                    
                    
                    #wclient.download_file(body, dest_path=store_path)
                    #file_name = message_props["url"].split('/')[-1]
                    #file_name = body.split('/')[-1]
                    #file_name = message_props["filename"]
                    
                    file_path = store_path + file_name
                    if(renameto != "none"):
                        new_file_name = renameto
                        new_file_path = store_path + new_file_name
                        shutil.move(file_path, new_file_path)
                        file_path = new_file_path
                    #message_props["file_path"] = file_path
                    return file_path
                if properties.headers["type"] == "http":
                    self._ensure_dir(store_path)
                    url = body
                    print "URL: " + url
                    file_name = url.split('/')[-1]
                    file_path = store_path + file_name
                    u = urllib2.urlopen(url)
                    f = open(file_path, 'wb')
                    block_sz = 8192
                    while True:
                        buffer = u.read(block_sz)
                        if not buffer:
                            break
                        #file_size_dl += len(buffer)
                        f.write(buffer)
                        #status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
                        #status = status + chr(8)*(len(status)+1)
                        #print status,
                    f.close()
                    return file_path
                break
        pass
    def _read_from_port(self, port):
        queue_name = self.__gen_queue_name(port)        

        while self.connection.is_open:           
            if(self.delivery_tags[queue_name] > 0):
                #print "[DEBUG] ACK delivey_tag " + str(self.delivery_tags[queue_name]) + " for " + queue_name + "\n"
                self.channel.basic_ack(int(self.delivery_tags[queue_name]))
            try:
                method, properties, body = self.channel.basic_get(queue=self.__gen_queue_name(port), no_ack=False)
            except:
                print "[ERROR] ", sys.exc_info()[0]
                self.errorlevel += 1
                
            
            if method.NAME == 'Basic.GetEmpty':
                time.sleep(10)
                self.delivery_tags[queue_name] = 0
            else:               
                    
                self.delivery_tags[queue_name] = method.delivery_tag             
                
                if properties.headers.has_key("cmd"):
                    if properties.headers["cmd"] == "eom":
                        print "[INFO] end of message received on " + self.__gen_queue_name(port) + "\n"
                        self.channel.basic_ack(self.delivery_tags[queue_name])
                        raise ValueError("EOM received")
                        self.channel.flush()
                        sleep(1)
                        self.channel.close()                        
                        return None
                return body
                break
            #lchannel.close()
        pass
    def __send_primer(self, port, lchannel):
        lchannel.basic_publish(exchange=self.__gen_queue_name(port),
                               routing_key=self.__gen_queue_name(port),
                               #routing_key='',
                               body="Primer")

    def _write_to_port(self, port, message, headers={}):
        #lchannel = self.connection.channel()
        if((port.state == 'new') & (self._isParent())):
            self.__send_primer(port, self.channel)
        port.state = "used"
        properties = pika.BasicProperties(headers=headers)
        #print "[DEBUG] writing to " + self.__gen_queue_name(port) + "\n"
        try:
            self.channel.basic_publish(exchange=self.__gen_queue_name(port),
                                       routing_key=self.__gen_queue_name(port),
                                       properties=properties,
                                       #routing_key='',
                                       body=message)
        except:
            print "[ERROR] ", sys.exc_info()[0]
            self.errorlevel += 1
        pass
    def _write_file_to_port(self, port, path,message,grouping=''):

        if os.path.isfile(path) == True:
            f = open(path, "r")
            filedata = f.read()
            filename = os.path.basename(path)
            fileurl = None
            for ds in self.dataservers:
                if ds.protocol == "webdav":
                    path = ds.path+grouping+"/"
                    wclient = webdav_client.Client(ds.url, webdav_path=ds.path)
                    wclient.set_connection(username=ds.user, password=ds.password)
                    wclient.mkdir(grouping)
                    wclient.upload_data(filedata, "/"+grouping+'/'+filename)
                    #fileurl = ds.url+ds.path+"/"
                    #fileurl = urllib2.urlparse.urljoin(fileurl,filename)
                    data = "filename=" + filename + "\nmessage=" + message
                    data = data + "\nurl=" + ds.url + ds.path + "/" +grouping+"/"+ filename
                    self._write_to_port(port, data, {"type": "webdav", "store_name": ds.name})
                    break
                #endif
            #endfor

        pass
    def _write_file_to_store(self, port, path):

        if os.path.isfile(path) == True:
            f = open(path, "r")
            filedata = f.read()
            filename = os.path.basename(path)
            fileurl = None
            for ds in self.dataservers:
                if ds.protocol == "webdav":
                    wclient = webdav_client.Client(ds.url, webdav_path=ds.path)
                    wclient.set_connection(username=ds.user, password=ds.password)
                    wclient.upload_data(filedata, filename)
                    #fileurl = ds.url+ds.path+"/"
                    #fileurl = urllib2.urlparse.urljoin(fileurl,filename)
                    data = "url=" + filename + "\nmessage=" + message
                    url = ds.url + "/" + ds.path + "/" + filename
                    return url
                    #self._write_to_port(port, data, {"type": "webdav", "store_name": ds.name})
                    break
                #endif
            #endfor
        return None

        pass

    def _check_file_onstores(self, filename):
        message_props = {}
        for ds in self.dataservers:
            if ds.protocol == "webdav":
                url = ds.url + "/" + ds.path + "/" + filename
                print "[INFO] checking url: " + url + "\n"
                if(self._check_url(url, ds.user, ds.password) == 200):
                    message_props["type"] = "webdav"
                    message_props["url"] = url
                    message_props["filename"] = filename
                    return message_props
            if ds.protocol == "local":
                url = ds.url + "/" + filename
                print "[INFO] checking url: " + url + "\n"
                if(os.path.exists(url)):
                    message_props["type"] = "local"
                    message_props["url"] = url
                    message_props["filename"] = filename
                    return message_props

        return None
        pass

    def _check_url(self, url, username, password):
        p = urlparse(url)      

        request = urllib2.Request(url)
        base64string = base64.encodestring('%s:%s' % (username, password)).replace('\n', '')
        request.add_header("Authorization", "Basic %s" % base64string)
        result = None
        try:
            result = urllib2.urlopen(request)
        except:
            return 400

        #print "[INFO] url: "+str(result.getcode())+"\n"
        return result.getcode()       
        pass


    def _read_file_from_url(self, url):
        #i = url.rfind("/") + 1
        #filename = url[i:]
        if "webdav" in url.lower():
            #self._ensure_dir("./data_files/")
            #store_name = properties.headers["store_name"]
            #ds = self.__get_ds(store_name)
            #wclient = webdav_client.Client(ds.url, webdav_path = ds.path)
            #wclient.set_connection(username=ds.user, password=ds.password)
            #wclient.download_file(message_props["url"],dest_path="./data_files/")
            #file_name = message_props.split('/')[-1]
            #file_path = "./data_files/"+filename
            #message_props["file_path"] = file_path
            return None
        if "http" in url.lower():
            code = urlopen(url).code
            if (code / 100 >= 4):
                print "WARNING: failed to read " + url
                return None
            else:
                self._ensure_dir("./data_files/")
                print "INFO: reading url" + url
                file_name = url.split('/')[-1]
                file_path = "./data_files/" + file_name
                u = urllib2.urlopen(url)
                f = open(file_path, 'wb')
                block_sz = 8192
                while True:
                    buffer = u.read(block_sz)
                    if not buffer:
                        break
                    f.write(buffer)
                f.close()
                return file_path
        pass

    def __gen_queue_name(self, port):
        #queue_name = self.module.context + "/" + self.module.partofworkflow + "/" + self.module.name + "/" + self.module.instance + "/" + port.name
        queue_name = self.module.context + "/" + self.module.partofworkflow + "/" + self.module.name + "/" + port.name
        return queue_name
        pass
    def internal_unload(self):
        print "calling internal unload: ", self.module.name
        ##self.errorlevel = 1
        ##if(self.errorlevel == 0):
        ##    #if(self.tesmode == False):
        ##    if(self._isParent()):
        ##        for p in self.module.inputports:
        ##            print "Deleting input port: ", self.__gen_queue_name(p)
        ##            self.channel.queue_delete(queue=self.__gen_queue_name(p))
        ##            print "Deleted input port: ", self.__gen_queue_name(p)
        ##
        ##    for p in self.module.outputports:
        ##        if(p.state != 'new'):
        ##            print "Sending dead message on: ", self.__gen_queue_name(p)
        ##            #lchannel = self.connection.channel()
        ##            lproperties = pika.BasicProperties(headers={'cmd': "eom"})
        ##            self.channel.basic_publish(exchange=self.__gen_queue_name(p),
        ##                                       routing_key=self.__gen_queue_name(p),
        ##                                       properties=lproperties,
        ##                                       body="End Of Messages")
                #lchannel.close()
        self.on_unload()

        pass
    
    def on_load(self):       
        pass
    def run(self):
        pass
    def on_unload(self):
        pass