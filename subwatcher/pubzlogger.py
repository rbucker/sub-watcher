#!/usr/bin/env python
#
# Copyright(c) 2011, Florida Freelance IT LLC
# Richard Bucker <richard@flafreeit.com>
# No Warranties are offered or implied. Use at your own risk.
# You Must leave this header in place. No other license offered.
#

"""This module is the publisher module. It borrows from some blogs/articles.

PS: While I was doing my testing I had also found ref#2. What I like about #2
is that the code is simpler and flatter, however, ref#1 does a nice bit with
the current module, line, traceback and so on. It's a richer implementation.
I will likely fork #1 in order to implement the ZMQ version.

I created ref #3


ref#1:  https://github.com/jedp/python-redis-log
ref#2:  http://sunilarora.org/another-redis-use-case-centralized-logging
ref#3:  https://github.com/rbucker881/python-zeromq-log
"""

import zmq
from zeromqlog import handlers, logger


iama = 'pubzlogger'


def __singleton(cls):
    """Python does not have much in the way of singleton decorators or like,
    The original author did not sign his work but it was interesting anyway,
    and it allowed me to implement a singleton against a class instead of the
    alternative of implementing the functions directly against the module. YUK!
    
    ref: http://greengiraffe.posterous.com/singleton-pattern-in-python
    """
    instances = {}
    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return getinstance

    
@__singleton
class PubLogger(object):
    """This is a wrapper around the logger class. It's meant to make things 
    easier.
    """
    def __init__(self, name=iama, channel=iama, url='tcp://localhost', port=5555):
        self.name    = name
        self.channel = channel
        self.url     = url
        self.port    = port
        self.log     = None
        
    def get_logger(self):
        """This code implements a delayed publisher. The pub is not created 
        until the first user calls this function as
        """
        if not self.log:
            self.log = logger.ZeroMQLogger(self.name)
            self.log.addHandler(handlers.ZeroMQHandler.to(self.channel))
        return self.log

# __END__
