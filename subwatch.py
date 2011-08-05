#!/usr/bin/env python
#
# Copyright(c) 2011, Florida Freelance IT LLC
# Richard Bucker <richard@flafreeit.com>
# No Warranties are offered or implied. Use at your own risk.
# You Must leave this header in place. No other license offered.
#

import re
import sys
import json
import time
import syslog
import sqlite3
import logging
import traceback
import exceptions

import redis
import tornado
from tornado.options import define, options

# TODO: replace the code from tornadoweb with my own
define("name",            default='subwatch',  help="override the application name when exporting logs", type=str)
define("quiet",           default=False,       help="do not display warnings, errors, or info not related to the data", type=bool)
define("debug",           default=False,       help="display debug messages overriding the quiet flag", type=bool)
define("channels",        default='subwatch',  help="one or more,  comma separated - no spaces, channel name(s) to monitor", type=str)
define("actions",         default='warning,error,critical,exception',  help="one or more,  comma separated - no spaces, message levels to monitor or action upon", type=str)
define("host",            default='localhost', help="redis hostname", type=str)
define("port",            default=6379,        help="redis port number", type=int)
define("db",              default=0,           help="redis DB", type=int)
define("password",        default=None,        help="password", type=str)
define("socket_timeout",  default=None,        help="socket timeout", type=int)
define("connection_pool", default=None,        help="connection pool", type=str)
define("charset",         default='utf-8',     help="charsets", type=str)
define("errors",          default='strict',    help="error reporting", type=str)
define("socket_path",     default=None,        help="use the local socket path instead of TCP", type=str)
define("redis",           default=False,       help="forward the actionable and matching to redis", type=bool)
define("last",            default=0,           help="get the last <n> messages", type=int)
define("all",             default=False,       help="get the all messages", type=bool)
define("userlog",         default=False,       help="forward the actionable and matching to user log function", type=bool)
define("syslog",          default=False,       help="forward the actionable and matching to syslog", type=bool)
define("console",         default=True,        help="forward the actionable and matching to the console", type=bool)
define("match",           default="",          help="regular expression(s) to match to the message", type=str)
define("format",          default="",          help="format of the message (TBD)", type=str)
define("jsonfail",        default="alert",     help="level to report when json fails to parse the msg data", type=str)

# SYSLOG
# Priority levels (high to low):
# LOG_EMERG, LOG_ALERT, LOG_CRIT, LOG_ERR, LOG_WARNING, LOG_NOTICE, LOG_INFO, LOG_DEBUG.
# Facilities:
# LOG_KERN, LOG_USER, LOG_MAIL, LOG_DAEMON, LOG_AUTH, LOG_LPR, LOG_NEWS, LOG_UUCP, LOG_CRON and LOG_LOCAL0 to LOG_LOCAL7.
# Log options:
# LOG_PID, LOG_CONS, LOG_NDELAY, LOG_NOWAIT and LOG_PERROR if defined in <syslog.h>.

# convert the plaintext error to the syslog integer value
priorities = {
    'debug'     : syslog.LOG_DEBUG,
    'info'      : syslog.LOG_INFO,
    'warning'   : syslog.LOG_WARNING,
    'error'     : syslog.LOG_ERR,
    'critical'  : syslog.LOG_CRIT,
    'exception' : syslog.LOG_ALERT,
}

actionable = []


if __name__ == "__main__":
    tornado.options.parse_command_line()
    if not options.quiet:
        print "starting..."
    try:
        if options.socket_path:
            r = redis.StrictRedis(unix_socket_path=options.socket_path)
        else:
            r = redis.StrictRedis(host=options.host, port=options.port, db=options.db)
        r.ping()
    except:
        (error_type, error_value, error_traceback) = sys.exc_info()
        err = "could not connect to redis:", error_type, repr(traceback.format_tb(error_traceback))
        if not options.quiet:
            print err
        sys.exit(-1)

    if options.all or options.last:
        # TODO: get the error messages from redis and dump to stdout
        pass
    else:
        try:
            # TODO: work out the exception processing. It is possible that an attempt to exit via CTL+C will
            # fail if the exception is caught in one of the inner try/except blocks.
            if options.syslog:
                # TODO: add facilities and options
                syslog.openlog(options.name)
            actionable = options.actions.split(',')
            if options.debug:
                print "actions [%s, %s]" % (options.actions, options.actions.split(','))
                print "channels [%s, %s]" % (options.channels, options.channels.split(','))
            p = r.pubsub()
            p.subscribe(options.channels.split(','))
            for msg in p.listen():
                    # the redis logger should be sending JSON conforming messages
                try:
                    jmsg = json.loads(msg['data'])
                except:
                    priority = priorities[options.jsonfail]
                    syslog.syslog(priority, 'failed to decode the message: %s' % (str(err)))
                    # next message
                    continue
                work = False
                try:
                    actionable.index(msg['level'])
                    work = True
                except:
                    # not actionable, do nothing
                    continue
                if work:
                    try:
                        if options.console:
                            print msg['data']
                        if options.syslog:
                            try:
                                priority = priorities[jmsg['level']]
                            except:
                                priority = syslog.LOG_ALERT
                                syslog.syslog(priority, 'The message level is not recognized [%s] add to the priorities map.' % (msg['level']))
                            syslog.syslog(priority, m['data'])
                        if options.redis:
                            # TODO: send the message to the appropriate redis tables based on priority
                            pass
                        if options.user:
                            # TODO: this is a callback to a user function in a user class; needs to be better defined
                            pass
                    except:
                        (error_type, error_value, error_traceback) = sys.exc_info()
                        err = "Unexpected error:", error_type, repr(traceback.format_tb(error_traceback))
                        priority = syslog.LOG_EMERG
                        syslog.syslog(priority, 'something went wrong sending the message out: %s' % (str(err)))
        except exceptions.KeyboardInterrupt:
            err = "Keyboard Interrupt..."
            if not options.quiet:
                print err
        except:
            (error_type, error_value, error_traceback) = sys.exc_info()
            err = "Unexpected error:", error_type, repr(traceback.format_tb(error_traceback))
            if not options.quiet:
                print err

    if not options.quiet:
        print "...DONE"
# __END__
