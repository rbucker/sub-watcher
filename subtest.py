#!/usr/bin/env python
#
# Copyright(c) 2011, Florida Freelance IT LLC
# Richard Bucker <richard@flafreeit.com>
# No Warranties are offered or implied. Use at your own risk.
# You Must leave this header in place. No other license offered.
#


""" This is the test program used to generate messages for the 
subwatcher to listen to.
"""

import sys
import json
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
# define("actions",         default='warning,error,critical,exception',  help="one or more,  comma separated - no spaces, message levels to monitor or actio
# define("host",            default='localhost', help="redis hostname", type=str)
# define("port",            default=6379,        help="redis port number", type=int)
# define("db",              default=0,           help="redis DB", type=int)
# define("password",        default=None,        help="password", type=str)
# define("socket_timeout",  default=None,        help="socket timeout", type=int)
# define("connection_pool", default=None,        help="connection pool", type=str)
# define("charset",         default='utf-8',     help="charsets", type=str)
# define("errors",          default='strict',    help="error reporting", type=str)
# define("socket_path",     default=None,        help="use the local socket path instead of TCP", type=str)
# define("redis",           default=False,       help="forward the actionable and matching to redis", type=bool)
# define("last",            default=0,           help="get the last <n> messages", type=int)
# define("all",             default=False,       help="get the all messages", type=bool)
# define("userlog",         default=False,       help="forward the actionable and matching to user log function", type=bool)
# define("syslog",          default=False,       help="forward the actionable and matching to syslog", type=bool)
# define("console",         default=True,        help="forward the actionable and matching to the console", type=bool)
# define("match",           default="",          help="regular expression(s) to match to the message", type=str)
# define("format",          default="",          help="format of the message (TBD)", type=str)
# define("jsonfail",        default="alert",     help="level to report when json fails to parse the msg data", type=str)


def now():
    """formatted time the way I like it. note that the logger does it 
    differently. At some point (ms) might be necessary
    """
    return time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())

conf = {
      'log_name'             : 'gw:log'
    , 'log_channel'          : ['gw:two']
    , '' : ''
}
# the log is a singleton
log = None
def get_logger(name, channel):
    global log
    if not log:
        if options.debug:
            print "name: %s, channel: %s" % (name, channel)
        log = logger.RedisLogger(name)
        log.addHandler(handlers.RedisHandler.to(channel))
    return log

def get_logger2():
    global log
    if not log:
	name = conf['log_name']
        channel = conf['log_channel'][0]
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
