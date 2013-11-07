__author__ = 'reggie'
import cmd

import DRShared

class Shell(cmd.Cmd):
    prompt = ">>"

    def __init__(self, context):
        cmd.Cmd.__init__(self)
        self.context = context

    def do_display(self, message):
        self.context.getProcGraph().showGraph()
        pass
    def do_quitsh(self, message):
        print "Quitting Shell"
        return(1)