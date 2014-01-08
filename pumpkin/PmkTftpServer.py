__author__ = 'reggie'

import tftpy
import PmkSeed
import PmkShared

from PmkShared import *

class TftpServer(SThread):

    def __init__(self, context, dir, port):
        SThread.__init__(self)
        self.context = context
        self.port = port
        self.dir = dir
        pass

    def run(self):
        PmkShared._ensure_dir(self.dir)
        tftp_server = tftpy.TftpServer(self.dir)
        tftp_server.listen('0.0.0.0', self.port)