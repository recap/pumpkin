__author__ = 'reggie'


###START-CONF
##{
##"object_name": "receiver",
##"object_poi": "my-receiver-1234",
##"auto-load": true,
##"parameters": [
##                  {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataFile",
##                      "state" : "PRE_PROC"
##                  }
## ],
##"return": [
##              {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataFile",
##                      "state" : "PROCESSED"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class receiver(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.wd = self.context.getWorkingDir()
        self.counter = 0
        pass


    # def run(self, pkt, data):
    #
    #     if self.ifFile(data):
    #         prot, path, file, apath,_ = self.fileparts(data)
    #
    #         f = open(apath, 'r')
    #         s = f.read()
    #         f.close()
    #
    #         print "FILE DATA FROM: "+apath+": "+s
    #
    #     else:
    #
    #         print "NON FILE DATA: "+data
    #
    #
    #
    #
    #     pass

    def run(self, pkt, data):
        """
        In this case data is a tar file path. Using _untar_to_wd will untar the file to
        the working directory which u can get by self.context.getWorkingDir(). _untar_to_wd
        returns a list of the members of the tar including the folder names. The example
        iterates through the list and checking which ones are the files.
        """

        if self.ifFile(data):
            prot, path, file, apath,_ = self.fileparts(data)

            dir_list = self._untar_to_wd(apath)
            for d in dir_list:
                full_name = self.wd+d.name
                if os.path.isfile(full_name):
                    print "file: "+str(full_name)



        else:

            print "NON FILE DATA: "+data




        pass

