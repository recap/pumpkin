import psutil
import threading
from multiprocessing import Process, Pipe, Array, Value
from greenlet import greenlet
import time, sys, os
import random
import zmq
import mmap
import struct
import ctypes


def spin(cpu_id):
    my_id = cpu_id.value
    #p = psutil.Process()
    #p.cpu_affinity([my_id])
    while True:
      pass

def shifttext(text, shift):
    strs='abcdefghijklmnopqrstuvwxyz'
    inp=text
    data=[]

    for i in inp:                     #iterate over the text not some list
        if i.strip() and i in strs:                 # if the char is not a space ""
            data.append(strs[(strs.index(i) + shift) % 26])
        else:
            data.append(i)           #if space the simply append it to data
    output = ''.join(data)
    return output

def pump(cpu_id, uexec, peers, p1, p2, mm):
  
  my_id = cpu_id.value
  uexec_id = uexec.value
  no_peers = len(peers)

  conn_peers = ["","","",""]
  p = psutil.Process()
  p.cpu_affinity([my_id])

  s = "CHUNK"+str(my_id+5)
  #mm.seek(0)
  c = 0

  i = None
  if my_id == 0:


    #assert mm[0] == '\x00'
    i = ctypes.c_int.from_buffer(mm)
    i.value = 10
    offset = struct.calcsize(i._type_)
    s_type = ctypes.c_char * len('foo')
    s = s_type.from_buffer(mm, offset)
    st = 'aaa'
    while True:

      i.value += 1
      i.value = i.value % 100
      #assert i.value == 11


      #assert mm[offset] == '\x00'
      st = shifttext(st, 1)
      s.raw = st

  else:
    ih = None
    sh = None
    time.sleep(1)
    while True:

      new_i, = struct.unpack('i', mm[:4])
      new_s, = struct.unpack('3s', mm[4:7])
      if ih != new_i or sh != new_s:
        #print ('i: %s => %d' % (ih, new_i))
        #print ('s: %s => %s' % (sh, new_s))
        #print ('Press Ctrl-C to exit')
        ih = new_i
        sh = new_s

  return
  # while True:
  #   if my_id == 0:
  #       #mm.seek(c*5)
  #       mm.write("CHUNK")
  #
  #   if c > 10:
  #     return
  #   if my_id == 1:
  #     mm.seek(0)
  #     print mm.read(5)
  #
  #   c += 1
  # return
  # #mm.seek(my_id*7)
  # #mm.
  # #mm.write(s)
  # #mm.readline()
  # print "["+str(my_id)+"] "+mm.readline()

  return

  #print "Loopinh "+str(my_id)
  #while True:
  #  shm[my_id] += 1
  #  if shm[my_id] > 10000:
  #    shm[my_id] = 0


  sock_uid = "ipc:///tmp/pumpkin/"+str(hex(uexec_id))+"/"+str(my_id)

  for i in range(0,3):
    conn_peers[i] = "ipc:///tmp/pumpkin/"+str(hex(uexec_id))+"/"+str(i)

  zmq_cntx = zmq.Context()
  socket  = zmq_cntx.socket(zmq.REP)
  socket.bind(sock_uid)

  #set my state
  peers[my_id] = 1


  def p_recv():
    c = 0
    #time.sleep(5)
    while True:
      #  Wait for next request from client
      #if c >= (no_peers - 1):
      #  break
      #print "waiting..."
      #message = socket.recv()
      m = p1.recv()

      p2.send("ok")
      #print "Received request: ", message
      #socket.send("ok")
      #c += 1


  def p_send():
    #while peers[0] != 1:
    #  print "blocked..."
    #  time.sleep(1)

    #time.sleep(1)

    #s_socket = zmq_cntx.socket(zmq.REQ)
    #s_socket.connect (conn_peers[0])

    #s_socket.send("Hi from "+str(my_id))
    while True:
      p2.send("ko")
      #print str(p1.recv())
      #s_socket.send("KO")
      #message = s_socket.recv()

    #print "sent..."+str(my_id)

   
  #def foo():
    #for i in range(1,20):
      #print "pump_g"+str(my_id)+": "+str(i)+" "+str(p.cpu_affinity())

  if my_id == 0:
    g2 = greenlet(p_recv)
    g2.switch()
  else:
    g1 = greenlet(p_send)
    g1.switch()



cpu_count = psutil.cpu_count()
#cpu_count = 4
r = random.randint(1, 2147483647)

uexec_id = Value('i', r)
peers = Array('i', range(cpu_count))
shm = Array('i', range(10))

#sock_dir = "/tmp/pumpkin/"+str(hex(r))

#if not os.path.exists(sock_dir):
#  os.mkdir(sock_dir, 0775)



peer_id = Value('i', 0)

#print hex(uexec_id)
pp1, cp1 = Pipe()
pp2, cp2 = Pipe()

#mm = mmap.mmap(-1, 100)
fd = os.open('/tmp/mmaptest', os.O_CREAT | os.O_TRUNC | os.O_RDWR)

# Zero out the file to insure it's the right size
assert os.write(fd, '\x00' * mmap.PAGESIZE) == mmap.PAGESIZE
buf = mmap.mmap(fd, mmap.PAGESIZE, mmap.MAP_SHARED, mmap.PROT_WRITE)
#mm.write("CHUNK1:CHUNK2")
#arr = [0,0]




for i in range(1,cpu_count):
  peer_id = Value('i',i)
  peers[i] = -1
  pr = Process(target=pump, args=(peer_id, uexec_id, peers, pp1, cp2, buf))
  pr.start()
#  peer_id = Value('i',i)
#  peers[i] = -1
  #pr = Process(target=spin, args=(peer_id,))
#  pr = Process(target=pump, args=(peer_id, uexec_id, peers,))
#  pr.start()

  #t = threading.Thread(target=spin, args = (peer_id,))
  #t.start()

peer_id = Value('i', 0)
pump(peer_id, uexec_id, peers, pp2, cp1, buf)

