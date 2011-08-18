#!/usr/bin/env python
#
# Copyright(c) 2011, Florida Freelance IT LLC
# Richard Bucker <richard@flafreeit.com>
# No Warranties are offered or implied. Use at your own risk.
# You Must leave this header in place. No other license offered.
#


""" This is the test program used to generate messages for the 
subwatcher to listen to. This test program uses zeromq as the
pubsub agent.
"""

import sys
import simplejson as json
import time
import uuid
import syslog
import logging
import traceback
from pprint import pprint

from zeromqlog import handlers, logger
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
        log = logger.ZeroMQLogger(name)
        log.addHandler(handlers.ZeroMQHandler.to(channel))
    return log

if __name__ == "__main__":
    tornado.options.parse_command_line()
    if not options.quiet:
        print "starting..."

    l = get_logger(options.name, options.channel)
    tsleep = 2
    # gotta sleep for a bit, the connection latency can cause messages to be
    # dropped, so for this case... patience.
    print "sleeping %d seconds..." % (tsleep)
    time.sleep(tsleep)
    print "logging..."
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
