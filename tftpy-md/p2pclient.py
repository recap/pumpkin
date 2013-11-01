import tftpy
import sys
import time

client = tftpy.TftpClient('flightcees', 4653)
#time.sleep(2)
client.download('test', 'local_test')

