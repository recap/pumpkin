import tftpy

server = tftpy.TftpServer('/tmp/tftp')
server.open("flightcees.lab.uvalight.net",4653,"1")


