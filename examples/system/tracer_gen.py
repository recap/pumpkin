__author__ = 'reggie'

import uuid
import base64

with open ("tracer.pkt", "r") as myfile:
    data=myfile.read()

    new_id = str(uuid.uuid1()).split("-")[0]


    ndata = data.replace("CONT_ID", new_id)

    new_file = "tracer_"+new_id+".pkt"
    nf = open(new_file,"w")
    nf.write(ndata)
    nf.close()

    encoded = base64.b64encode(ndata)
    new_file = "tracer.b64"
    nf = open(new_file,"w")
    nf.write(encoded)
    nf.close()




