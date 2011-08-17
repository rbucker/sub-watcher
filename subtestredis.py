#!/usr/bin/env python
#
# Copyright(c) 2011, Florida Freelance IT LLC
# Richard Bucker <richard@flafreeit.com>
# No Warranties are offered or implied. Use at your own risk.
# You Must leave this header in place. No other license offered.
#


""" This is the test program used to generate messages for the 
subwatcher to listen to. This test program uses redis as the
pubsub agent. A separate program will test ZMQ.
"""

import sys
import simplejson as json
import time
import uuid
import syslog
import logging
import traceback
from pprint import pprint

import redis
from redislog import handlers, logger
import tornado
from tornado.options import define, options

define("name",            default='subwatch',  help="override the application name when exporting logs", type=str)
define("quiet",           default=False,       help="do not display warnings, errors, or info not related to the data", type=bool)
define("debug",           default=False,       help="display debug messages overriding the quiet flag", type=bool)
define("channel",         default='subwatch',  help="one  channel name", type=str)


log = None
def get_logger(name, channel):
    global log
    if not log:
        if options.debug:
            print "name: %s, channel: %s" % (name, channel)
        log = logger.RedisLogger(name)
        log.addHandler(handlers.RedisHandler.to(channel))
    return log


if __name__ == "__main__":
    tornado.options.parse_command_line()
    if not options.quiet:
        print "starting..."

    l = get_logger(options.name, options.channel)
    #l= get_logger2()
    l.debug('test the debug message')
    l.info('test the info message')
    l.warning('test the warning message')
    l.error('test the error message')
    l.critical('test the critical message')
    l.exception('test the exception message')

    time.sleep(1)

    if not options.quiet:
        print "...DONE"

# __END__
