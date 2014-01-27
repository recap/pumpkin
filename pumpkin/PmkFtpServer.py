__author__ = 'reggie'

import tftpy
import PmkSeed
import PmkShared

from PmkShared import *

try:
    from pyftpdlib.authorizers import DummyAuthorizer
    from pyftpdlib.handlers import FTPHandler
    from pyftpdlib.servers import FTPServer
except ImportError:
    from pyftpdlib.contrib.authorizers import DummyAuthorizer
    from pyftpdlib.contrib.handlers import FTPHandler
    from pyftpdlib.ftpserver import FTPServer


class FtpServer(SThread):

    def __init__(self, context, dir, port=2121, user="Test", password="Test"):
        SThread.__init__(self)
        self.context = context
        self.port = port
        self.dir = dir
        self.auth = DummyAuthorizer()

        authorizer = self.auth

        # Define a new user having full r/w permissions and a read-only
        # anonymous user
        authorizer.add_user('user', '12345', dir, perm='elradfmwM')
        authorizer.add_anonymous(dir)

        # Instantiate FTP handler class
        handler = FTPHandler
        handler.authorizer = authorizer

        # Define a customized banner (string returned when client connects)
        handler.banner = "pyftpdlib based ftpd ready."

        # Specify a masquerade address and the range of ports to use for
        # passive connections.  Decomment in case you're behind a NAT.
        #handler.masquerade_address = '151.25.42.11'
        #handler.passive_ports = range(2121)


        # Instantiate FTP server class and listen on 0.0.0.0:2121
        address = ('localhost', port)
        self.server = FTPServer(address, handler)

        # set a limit for connections
        self.server.max_cons = 256
        self.server.max_cons_per_ip = 5
        pass




    def run(self):
        PmkShared._ensure_dir(self.dir)
        self.server.serve_forever()
