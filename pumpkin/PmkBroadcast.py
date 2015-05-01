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
AZURE_IPS = ["65.52.128.0/19","94.245.97.0/24","104.47.169.0/24","137.116.192.0/19","157.55.8.64/26","157.55.8.128/27","157.55.8.160/28","168.63.0.0/19","168.63.96.0/19","193.149.80.0/22","213.199.128.0/21","213.199.136.0/22","213.199.180.32/28","213.199.180.96/27","213.199.180.192/26","213.199.183.0/24","23.97.128.0/17","23.98.46.0/24","23.100.0.0/20","23.101.64.0/20","104.40.128.0/17","104.45.0.0/18","104.45.64.0/20","104.46.32.0/19","137.117.128.0/17","168.61.56.0/21","191.233.64.0/18","191.237.232.0/22","191.239.200.0/23","191.239.202.0/24","191.239.203.0/27","191.239.203.64/28","193.149.84.0/22","104.46.16.0/21","104.47.128.0/28","104.47.129.0/24","104.47.136.0/21","104.47.144.0/20","104.47.160.0/21","104.214.192.0/21","104.214.200.0/24","104.214.201.0/28","104.214.208.0/20","104.214.224.0/20","23.96.0.0/18","23.96.64.0/28","23.96.64.64/26","23.96.64.128/27","23.96.64.160/28","23.96.80.0/20","23.96.96.0/19","23.100.16.0/20","23.101.128.0/20","137.116.112.0/20","137.117.32.0/19","137.117.64.0/18","137.135.64.0/18","157.56.176.0/21","168.61.32.0/20","168.61.48.0/21","168.62.32.0/19","168.62.160.0/19","104.45.128.0/20","104.45.144.0/20","104.45.160.0/20","104.45.176.0/20","104.45.192.0/20","23.98.45.0/24","104.41.128.0/19","138.91.96.0/25","138.91.96.128/26","138.91.96.192/28","138.91.112.0/20","191.234.32.0/19","191.236.0.0/18","191.238.0.0/22","191.238.4.0/24","191.238.6.0/26","191.238.6.64/28","191.238.7.0/24","191.238.8.0/21","191.238.16.0/20","191.238.32.0/19","40.114.0.0/20","104.211.0.0/18","191.237.0.0/19","191.237.32.0/25","191.237.32.128/26","191.237.32.192/27","191.237.33.0/24","191.237.34.0/23","191.237.36.0/23","191.237.38.0/27","191.237.39.0/24","191.237.40.0/21","191.237.48.0/20","191.237.64.0/18","23.100.64.0/21","23.101.144.0/20","23.101.32.0/21","137.116.0.0/18","137.116.64.0/19","137.116.96.0/22","191.239.224.0/25","193.149.64.0/21","104.46.192.0/20","23.102.204.0/23","23.102.206.0/26","23.102.206.64/27","23.102.206.96/28","23.102.207.0/24","104.46.0.0/21","104.46.96.0/19","104.209.128.0/18","104.210.0.0/20","191.236.192.0/18","191.237.128.0/19","191.237.160.0/22","191.237.164.0/23","191.237.168.0/21","191.237.176.0/20","104.208.128.0/27","23.100.32.0/20","23.101.192.0/20","137.116.184.0/21","137.117.0.0/19","137.135.0.0/18","138.91.64.0/19","157.56.160.0/21","168.61.0.0/19","168.61.64.0/20","168.62.0.0/19","168.62.192.0/19","168.63.88.0/24","104.45.208.0/20","104.45.224.0/20","104.45.240.0/20","23.99.64.0/19","138.91.128.0/24","138.91.129.0/26","138.91.129.64/28","138.91.136.0/21","138.91.144.0/20","138.91.160.0/19","138.91.192.0/21","138.91.224.0/19","191.238.70.0/23","23.99.0.0/19","23.99.32.0/25","23.99.32.128/28","23.99.33.0/28","23.99.34.0/23","23.99.36.0/24","23.99.37.0/26","23.99.37.80/28","23.99.37.96/27","23.99.37.128/27","23.99.37.176/28","23.99.37.192/26","23.99.38.0/24","23.99.40.0/23","23.99.42.0/24","23.99.44.0/23","23.99.46.0/24","23.99.47.0/26","23.99.47.64/28","23.99.48.0/20","65.52.112.0/20","104.40.0.0/18","104.40.64.0/19","104.42.0.0/18","104.42.64.0/20","104.42.96.0/19","104.209.16.0/23","104.209.32.0/19","104.210.32.0/19","168.63.89.0/25","168.63.89.128/26","191.236.64.0/18","191.239.0.0/18","104.42.128.0/19","104.42.160.0/27","104.42.161.0/24","104.42.162.0/24","104.42.168.0/21","104.42.176.0/20","104.42.192.0/21","23.98.54.0/24","23.100.72.0/21","23.100.224.0/20","65.52.0.0/19","65.52.48.0/20","65.52.106.16/28","65.52.106.96/27","65.52.106.128/27","65.52.106.240/28","65.52.192.0/19","65.52.232.0/21","65.52.240.0/21","157.55.24.0/21","157.55.60.224/27","157.55.73.32/28","157.55.136.0/21","157.55.151.0/28","157.55.160.0/20","157.55.208.0/20","157.55.252.0/22","157.56.8.0/21","157.56.24.160/27","157.56.24.192/28","157.56.28.0/22","168.62.96.0/19","168.62.224.0/20","168.62.240.0/21","168.62.248.0/22","207.46.192.0/24","207.46.193.0/25","207.46.193.144/28","207.46.193.160/27","207.46.193.192/26","207.46.194.0/23","207.46.196.0/23","207.46.198.0/24","207.46.199.0/26","207.46.199.64/27","207.46.199.128/25","207.46.200.0/26","207.46.200.64/28","207.46.200.96/27","207.46.200.128/25","207.46.201.0/24","207.46.202.0/24","207.46.203.0/26","207.46.203.128/26","207.46.203.192/27","207.46.204.0/22","209.240.220.0/23","23.96.176.0/20","23.96.192.0/18","23.98.48.0/27","23.98.48.80/28","23.98.48.96/27","23.98.48.128/27","23.98.48.160/28","23.98.48.192/26","23.98.49.0/24","23.98.50.0/23","23.98.52.0/23","23.98.55.0/26","23.98.55.64/27","23.101.160.0/20","191.236.128.0/18","65.52.64.0/20","65.52.224.0/21","94.245.88.0/21","94.245.104.0/21","94.245.112.0/23","94.245.114.64/26","94.245.114.128/25","94.245.115.0/24","94.245.116.0/24","94.245.117.0/26","94.245.117.96/27","94.245.117.128/25","94.245.118.0/27","94.245.120.0/25","94.245.120.128/28","94.245.120.160/27","94.245.120.192/26","94.245.121.0/24","94.245.122.0/24","94.245.123.0/25","94.245.123.128/27","94.245.123.176/28","94.245.123.192/26","94.245.124.0/22","137.116.224.0/19","168.61.80.0/20","168.61.96.0/19","168.63.32.0/19","168.63.64.0/20","168.63.80.0/21","168.63.92.0/22","193.149.88.0/21","23.100.48.0/20","23.101.48.0/20","23.102.0.0/18","104.45.80.0/20","104.45.96.0/19","137.135.128.0/20","137.135.160.0/19","137.135.192.0/25","137.135.192.128/28","137.135.192.160/27","137.135.192.192/26","137.135.193.0/24","137.135.194.0/23","137.135.196.0/22","137.135.200.0/21","137.135.208.0/20","137.135.224.0/19","138.91.48.0/20","191.235.128.0/18","191.235.192.0/24","191.235.193.0/26","191.235.193.64/27","191.235.193.96/28","191.235.193.128/25","191.235.194.0/23","191.235.208.0/20","191.235.255.0/26","191.235.255.64/27","191.235.255.128/25","191.237.192.0/23","191.237.194.0/24","191.237.196.0/28","191.237.208.0/20","191.238.96.0/19","191.239.208.0/20","40.112.64.0/20","104.41.192.0/28","104.41.193.0/24","104.41.200.0/21","104.41.208.0/20","104.41.224.0/21","104.41.232.0/27","104.41.232.32/28","104.41.232.64/27","104.41.232.96/28","104.41.233.0/24","104.41.240.0/20","104.46.8.0/21","104.46.64.0/20","104.46.80.0/24","104.46.81.0/28","104.46.88.0/21","23.100.80.0/21","23.101.112.0/20","168.61.128.0/17","193.149.72.0/21","23.100.240.0/20","23.99.128.0/19","23.99.160.0/22","23.99.164.0/23","23.99.166.0/24","23.99.167.0/27","23.99.168.0/23","23.99.176.0/20","23.99.192.0/18","40.113.192.0/20","40.113.208.0/24","40.113.209.0/28","40.113.216.0/21","40.113.224.0/20","40.113.240.0/21","104.43.128.0/17","104.208.0.0/27","23.98.32.0/21","23.98.40.0/22","23.100.88.0/21","23.101.0.0/20","65.52.160.0/19","104.46.24.0/24","104.46.26.0/24","104.46.160.0/19","111.221.64.0/22","111.221.69.0/25","134.170.192.0/21","137.116.160.0/20","168.63.128.0/19","168.63.192.0/19","207.46.72.0/26","207.46.77.224/28","207.46.87.0/24","207.46.89.16/28","207.46.95.48/28","207.46.128.0/19","23.97.64.0/20","23.97.80.0/28","23.98.44.0/24","23.99.96.0/19","23.102.224.0/19","104.208.80.0/20","104.208.112.0/20","191.234.2.16/28","191.234.2.32/27","191.234.2.64/26","191.234.2.128/26","191.234.3.0/24","191.234.16.0/20","191.237.238.0/28","191.237.238.32/27","191.237.238.64/27","191.237.238.112/28","23.97.48.0/20","23.100.112.0/21","23.101.16.0/20","104.43.41.0/24","104.46.128.0/19","111.221.80.0/20","111.221.96.0/20","137.116.128.0/19","138.91.32.0/20","168.63.160.0/19","168.63.224.0/19","207.46.48.0/21","207.46.56.0/24","207.46.58.0/24","207.46.59.0/26","207.46.59.128/25","207.46.60.0/23","207.46.62.0/24","207.46.63.0/26","207.46.63.80/28","207.46.63.96/27","207.46.63.128/25","23.98.64.0/20","168.63.90.0/27","168.63.90.32/28","168.63.90.64/27","191.238.64.0/24","207.46.224.0/20","104.43.0.0/28","104.43.1.0/24","104.43.8.0/21","104.43.16.0/20","104.43.32.0/21","104.43.64.0/20","104.43.80.0/27","104.43.120.0/28","104.43.121.0/24","104.215.128.0/19","104.215.160.0/28","104.215.161.0/24","104.215.168.0/21","104.215.176.0/20","104.215.192.0/21","23.98.174.0/24","65.52.32.0/21","65.54.48.0/22","65.54.52.0/26","65.54.52.64/27","65.54.52.128/25","65.54.53.0/24","65.55.80.0/21","65.55.88.0/22","65.55.92.0/23","65.55.94.0/24","65.55.95.0/26","65.55.95.64/27","65.55.95.128/25","70.37.48.0/20","70.37.64.0/19","70.37.96.0/20","70.37.112.0/22","70.37.116.0/23","70.37.118.0/24","70.37.119.128/26","70.37.119.224/27","70.37.120.0/22","70.37.124.0/23","70.37.126.0/26","70.37.126.64/27","70.37.126.128/25","70.37.127.0/26","70.37.127.240/28","70.37.160.0/21","104.47.208.0/23","157.55.80.0/21","157.55.103.32/27","157.55.153.224/28","157.55.176.0/20","157.55.192.0/21","157.55.200.0/22","168.62.128.0/19","23.98.128.0/19","23.98.160.0/27","23.98.160.48/28","23.98.160.64/26","23.98.160.128/27","23.98.160.160/28","23.98.160.208/28","23.98.160.224/27","23.98.161.0/24","23.98.162.0/28","23.98.162.32/28","23.98.162.64/26","23.98.162.128/27","23.98.162.176/28","23.98.162.192/26","23.98.164.0/23","23.98.167.0/24","23.98.169.0/24","23.98.170.0/28","23.98.170.64/27","23.98.170.128/27","23.98.171.0/24","23.98.173.0/24","23.98.175.0/24","23.98.192.0/25","23.98.216.0/21","23.98.255.0/27","23.98.255.32/28","23.98.255.64/26","23.100.120.0/21","23.100.192.0/19","23.101.176.0/20","23.102.128.0/18","104.44.128.0/19","104.210.144.0/20","104.210.184.0/21","104.210.192.0/19","191.238.144.0/20","191.238.160.0/19","191.238.224.0/19","23.98.56.0/26","23.98.56.64/27","23.98.56.96/28","23.98.56.128/26","23.100.104.0/21","138.91.16.0/20","191.233.32.0/19","191.237.236.0/24","191.238.80.0/20","104.46.224.0/20","104.214.128.0/28","104.214.129.0/24","104.214.136.0/21","104.214.144.0/21","104.214.152.0/27","191.238.68.0/26","191.238.69.0/24","191.239.96.0/19","23.98.57.0/25","23.98.57.128/27","23.100.96.0/21","138.91.0.0/20","191.234.138.0/24","104.46.208.0/20","23.102.64.0/20","23.102.80.0/24","104.41.160.0/19","191.237.240.0/26","191.237.240.64/28","191.237.241.0/24","23.97.96.0/20","23.97.112.0/25","23.97.112.128/28","104.41.0.0/20","104.41.16.0/26","104.41.16.64/28","104.41.17.0/24","104.41.18.0/24","104.41.32.0/20","191.237.248.0/21","23.101.208.0/20","104.46.29.0/24","104.46.30.0/24","104.46.240.0/20","104.209.80.0/20","104.210.64.0/19","191.238.66.0/25","191.238.66.128/27","191.238.66.160/28","191.238.67.0/24","191.239.64.0/19","23.101.224.0/19","104.46.28.0/24","104.209.64.0/20","191.239.160.0/19","191.239.192.0/25","191.239.192.128/27","191.239.193.0/24"]

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
        if is_amazon(pip) or is_azure(pip):
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
    
def is_azure(ip):
    ipa = netaddr.IPAddress(ip)
    if ipa.is_private():
        return False

    if ipa.is_reserved():
        return False

    for anet in AZURE_IPS:
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

                if self.context.is_with_nocompress():
                    dataz = data
                else:
                    dataz = zlib.compress(data)

                #if self.connection.is_closed:
                #    self._connect()

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


                #if self.context.is_with_nocompress():
                #    dataz = data
                #else:
                dataz = zlib.compress(data)
                self.context.getProcGraph().ackRegistryUpdate()

                #if self.connection.is_closed:
                #    self._connect()

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

                    if self.context.is_with_nocompress():
                        data = dataz
                    else:
                        data = zlib.decompress(dataz)


                    #logging.debug("Incomming data from ["+self.queue+"]: "+data)
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


            #logging.debug("Incomming data from ["+self.zmq_endpoint+"]: "+data)
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
