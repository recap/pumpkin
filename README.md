Pumpkin
=========
Distributed data processing network in python
work in progress....

Download
==========

by cloning
git clone https://github.com/recap/pumpkin.git

by archive
https://github.com/recap/pumpkin/archive/master.zip

Install
==========
If you want to run pumpkin locally you can:

cd bin
ln -s ../pumpkin ./pumpkin
chmod +x pmk
cd ../

If you want to install pumpkin to the system:

sudo python setup.py install

Hellow World
=============

run:

./bin/pmk --endpoint.type zmq.IPC

If all goes well you should get something like:

INFO:Node assigned UID: slimme-783d9470
INFO:Exec context: e4a460ff
INFO:Node bound to IP: 192.168.1.50
INFO:Starting broadcast listener on port 7700
INFO:Discovered new peer: extract at ipc:///tmp/slimme-783d9470
INFO:Discovered new peer: inject at ipc:///tmp/slimme-783d9470
INFO:Discovered new peer: greet at ipc:///tmp/slimme-783d9470
Greeting: Hello World


you can exit with ctrl-c

Short Explanation
===================

What is happening is that by default Pumpkin il loading seeds from examples/helloworld. If you take a look
in that directory you notice 3 files inject.py, greet.py and extract.py. These 3 seeds form a chain to
produce the output "Hello World". inject.py is injecting "World" string as data, the greet.py attaches
the greeting "Hello" and the extract.py prints the output. These seeds are loosely coupled so much so
that they can run on different machines (more on this later).


+----------+         +----------+       +-----------+
|          |         |          |       |           |
|  inject  |-------> |  greet   |------>|  extract  |
|          |         |          |       |           |
+----------+         +----------+       +-----------+

Injecting Data
================

There are several ways of injecting data. The above demonstrates the use of an injector seed. Another way is through
files. if we rerun the above example but first:

create a tmp directory e.x. /tmp/rx

then run:

./bin/pmk --endpoint.type zmq.IPC --rx /tmp/rx

This will output the same as above and show "Hello World". At theis point we can inject more data into /tmp/rx directory.
To do this you can copy a data packet file [examples/helloworld/data.pkt] to /tmp/rx and check the console output for pmk.
If all goes well a new output should be:

Greeting: Hello Mars


