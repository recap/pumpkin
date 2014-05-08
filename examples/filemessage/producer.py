__author__ = 'reggie'


###START-CONF
##{
##"object_name": "producer",
##"object_poi": "my-producer-1234",
##"auto-load": true,
##"parameters": [ ],
##"return": [
##              {
##                      "name": "data",
##                      "description": "file data",
##                      "required": true,
##                      "type": "DataFile",
##                      "state" : "PRE_PROC"
##                }
##
##          ] }
##END-CONF




from pumpkin import *

class producer(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    # def run(self, pkt):
    #     """ Write to a file and send it to a
    #         consumer.
    #     """
    #     wd = self.context.getWorkingDir()
    #     fs = self.context.getFileDir()
    #     fn = "testfile.txt"
    #
    #     full_wfn = wd+"/"+fn
    #
    #     f = open(full_wfn, 'w')
    #     f.write("some data")
    #     f.close()
    #
    #     self.dispatch(pkt,"file://"+full_wfn, "PRE_PROC")
    #
    #
    #     pass

    def run(self, pkt):
        """ Write to a file and send it to a
            consumer.
        """
        wd = self.context.getWorkingDir()
        dir = "testdir"
        full_dir = wd + "/" + dir
        fn1 = "testfile.txt"
        fn2 = "testfile2.txt"

        self._ensure_dir(full_dir)

        full_wfn1 = full_dir+"/"+fn1
        full_wfn2 = full_dir+"/"+fn2

        f1 = open(full_wfn1, 'w')
        f1.write("some data for file 1")
        f1.close()

        f2 = open(full_wfn2, 'w')
        f2.write("some data for file 2")
        f2.close()

        archive = self._tar_to_gz(dir, suffix="001")

        self.logger.info("Archive: "+archive)

        self.dispatch(pkt,"file://"+archive, "PRE_PROC")


        pass

