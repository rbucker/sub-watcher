#!/usr/bin/env python
#
# Copyright(c) 2011, Florida Freelance IT LLC
# Richard Bucker <richard@flafreeit.com>
# No Warranties are offered or implied. Use at your own risk.
# You Must leave this header in place. No other license offered.
#

""" This application is a general purpose subscriber for the redis publisher, it is intended
to filter the published messages and only process the ones that match the criteria. (see the 
command line options). There is even an option to rewrite the messages back into redis so
that they will be persisted and queried by other applications. This tool will also be able
to pull these records from the the tables for inspection.

Posting the message back into 

NOTE: I like some of the things in the PEP-8 and PEP-257(??) however I've been slamming
this code out and I have not dotted all of my T's yet. You are welcome to help reformat
and refactor.
"""
import re
import sys
import csv
import json
import time
import uuid
import syslog
import sqlite3
import logging
import traceback
import exceptions

import redis
import tornado
from tornado.options import define, options

# TODO: replace the code from tornadoweb with my own
""" This may seem like a long list of params. Perhanps it is, however, they are necessary.
"""
# debug and misc options
define("quiet",           default=False,       help="do not display warnings, errors, or info not related to the data", type=bool)
define("debug",           default=False,       help="display debug messages overriding the quiet flag", type=bool)
define("verbose",         default=False,       help="display debug messages overriding the verbose flag", type=bool)
define("jsonfail",        default="alert",     help="level to report when json fails to parse the msg data", type=str)
define("format",          default="csv",       help="csv, tsv, html, json", type=str)
define("header",          default=False,       help="True if the tsv, csv should have a header record", type=bool)

# REDIS common options
define("host",            default='localhost', help="redis hostname", type=str)
define("port",            default=6379,        help="redis port number", type=int)
define("db",              default=0,           help="redis DB", type=int)
define("password",        default=None,        help="password", type=str)
define("socket_timeout",  default=None,        help="socket timeout", type=int)
define("connection_pool", default=None,        help="connection pool", type=str)
define("charset",         default='utf-8',     help="charsets", type=str)
define("errors",          default='strict',    help="error reporting", type=str)
define("socket_path",     default=None,        help="use the local socket path instead of TCP", type=str)

# REDIS write options
define("max_messages",    default=100,         help="maximum number of messages to be inserted into a queue (0=unlimited)", type=int)
define("key_format",      default='%s:%s:msg', help="tags to be placed around the key for the message body", type=str)
define("que_format",      default='err:%s:que',help="this is the message queue where all the msg uuid's are stored", type=str)
define("score",           default='%s%010d',   help="format of the score element in the redis:sorted-list", type=str)
define("score_tracker",   default='score_tracker',   help="table to keep track of the daily message id number used in the score", type=str)


# REDIS read options
define("redis",           default=False,       help="forward the actionable and matching to redis", type=bool)
define("grouped",         default=False,       help="all of the messages that are actionable and filtered are stored in the same redis table", type=bool)
define("last",            default=0,           help="get the last <n> messages", type=int)
define("all",             default=False,       help="get the all messages", type=bool)

# alternate push options
define("name",            default='subwatch',  help="override the application name when exporting logs", type=str)
define("userlog",         default=False,       help="forward the actionable and matching to user log function", type=bool)
define("syslog",          default=False,       help="forward the actionable and matching to syslog", type=bool)
define("console",         default=True,        help="forward the actionable and matching to the console", type=bool)

# matching options
define("channels",        default='subwatch',  help="one or more,  comma separated - no spaces, channel name(s) to monitor", type=str)
define("actions",         default='warning,error,fatal,exception',  help="one or more,  comma separated - no spaces, message levels to monitor or action upon", type=str)
define("match",           default="",          help="regular expression(s) to match to the message", type=str)



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
    'fatal'     : syslog.LOG_CRIT,
    'exception' : syslog.LOG_ALERT,
}

actionable = []

def now():
    """formatted time the way I like it. note that the logger does it 
    differently. At some point (ms) might be necessary
    """
    return time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())

def from_redis(r, priority):
    que = options.que_format % (priority)
    if options.all:
        return r.zrange(que, 0, -1, withscores=True, score_cast_func=int)
    else:
        return r.zrange(que, -1 * options.last, -1, withscores=True, score_cast_func=int)

def to_redis(r, priority, msg):
    d = time.strftime('%y%m%d',time.gmtime())
    score = options.score % (d, r.hincrby(options.score_tracker, d, 1))
    que = options.que_format % (priority)
    if options.debug:
        print "The latest score is [%s]" % (score)
    l = r.zcount(que,'-inf','+inf')
    p = r.pipeline(transaction=True)
    #p.zadd(queue, score, msg )
    p.zadd(que, msg, score )
    p.execute()
    if options.max_messages > 0 and l >= options.max_messages:
        r.zremrangebyrank(que,0,(options.max_messages + 1) * -1)


if __name__ == "__main__":
    tornado.options.parse_command_line()
    actionable = options.actions.split(',')
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
        retval = []
        for a in actionable:
            for (data,score) in from_redis(r, a):
                t = {}
                t['score'] = score
                try:
                    jdata = json.loads(data)
                    for k, v in jdata.items():
                        t[k] = v
                except:
                    (error_type, error_value, error_traceback) = sys.exc_info()
                    err = "Unexpected error:", error_type, repr(traceback.format_tb(error_traceback))
                    t['data'] = data
                retval.append(t)
        if options.format == 'json':
            print json.dumps(retval)
        elif options.format == 'tsv': 
            writer = csv.writer(sys.stdout, delimiter='\t', quoting=csv.QUOTE_NONNUMERIC)
            if len(retval) > 0:
                if options.header:
                    writer.writerow(retval[0].keys())
                for row in retval:
                    writer.writerow(row.values())
        elif options.format == 'csv': 
            writer = csv.writer(sys.stdout, quoting=csv.QUOTE_NONNUMERIC)
            if len(retval) > 0:
                if options.header:
                    writer.writerow(retval[0].keys())
                for row in retval:
                    writer.writerow(row.values())
        elif options.format == 'html': 
            pass
        else:
            print "invalid format"
    else:
        try:
            # TODO: work out the exception processing. It is possible that an attempt to exit via CTL+C will
            # fail if the exception is caught in one of the inner try/except blocks.
            if options.syslog:
                # TODO: add facilities and options
                syslog.openlog(options.name)
            if options.debug:
                print "actions [%s, %s]" % (options.actions, options.actions.split(','))
                print "name: %s, channels [%s, %s]" % (options.name, options.channels, options.channels.split(','))
            p = r.pubsub()
            p.subscribe(options.channels.split(',')[0])
            for msg in p.listen():
                if options.debug and options.verbose:
                    print "---------------------------------------------------------------------------------"
                    print msg
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
                    actionable.index(jmsg['level'])
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
                            to_redis(r, jmsg['level'], msg['data'])
                        if options.userlog:
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
