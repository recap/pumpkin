__author__ = 'reggie'

with open ("example.pkt", "r") as myfile:
    data=myfile.read()

    for i in range(1,20):
        new_str = "patient_X"+str(i)
        ndata = data.replace("patient_X", new_str)

        new_file = "example"+str(i)+".pkt"
        nf = open(new_file,"w")
        nf.write(ndata)
        nf.close()
