Pumpkin
=========
Pumpkin is a framework for distributed Data Transformation Network (DTN). It is similar to an actor model but data
is dynamically routed from one actor to the next. In our model actors are seeds and these can be added to the network
dynamically, transformation paths are learnt and data can automatically start flowing to the new seeds.

Download
==========

by cloning

    git clone https://github.com/recap/pumpkin.git

by archive

    https://github.com/recap/pumpkin/archive/master.zip

Install
==========
If you want to install local

    python setup.py install --prefix=~/.local

If you want to install pumpkin to the system:

    sudo python setup.py install

Hello World
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

create a tmp directory e.x. 

    mkdir /tmp/rx

then run:

    ./bin/pmk --endpoint.type zmq.IPC --rx /tmp/rx

This will output the same as above and show "Hello World". At theis point we can inject more data into /tmp/rx directory.
To do this you can copy a data packet file [examples/helloworld/data.pkt] to /tmp/rx and check the console output for pmk.
If all goes well a new output should be:

    Greeting: Hello Mars


Distributing Seeds
==================

Pumpkin allows seeds to be disperesed on different computers. To try this out lets force *greet* to run on a different host. First install Pumpkin on a differetn host we will call host **B**. Our current host is host **A**.  On **A** locate the file *greet.py* under *examples/helloworld/*. Edit the file and change the line

    ##"auto-load" : true,

to

    ##"auto-load" : false,
    
This instruct Pumpkin not to load the seed *greet* automatically. Now on host **A** run Pumpkin with:

    ./bin/pmk
    
If you notice the output only lists 2 seeds

    INFO:Discovered new peer: extract at tcp://192.168.1.50:7900
    INFO:Discovered new peer: inject at tcp://192.168.1.50:7900
    
The *greet* seed is missing. We will host *greet* on **B**. After installing Pumpkin on **B** run the following:

    ./bin/pmk --seed ./examples/helloworld/greet.py
    
This instructs Pumpkin to only load the *greet* seed. After a few seconds you should see "Hello World" 
as output on **A**. This is because Pumpkins on **A** and **B** discover each other **given they are on a LAN** and data can flow.

![Alt text](https://github.com/recap/pumpkin/blob/kakai/examples/helloworld/helloworld_dtn.jpg "Optional title")
