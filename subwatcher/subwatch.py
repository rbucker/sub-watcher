#!/usr/bin/env python
#
# Copyright(c) 2011, Florida Freelance IT LLC
# Richard Bucker <richard@flafreeit.com>
# No Warranties are offered or implied. Use at your own risk.
# You Must leave this header in place. No other license offered.
#

""" This application is a general purpose subscriber for the redis publisher, it is intended
to filter the published messages and only process the ones that match the criteria. (see the 
command line options). There is even an option to rewrite the messages back into [redis, riak] so
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
import simplejson as json
import time
import uuid
import syslog
import sqlite3
import logging
import traceback
import exceptions

try:
    import zmq
except:
    print "ZMQ is not available"
    pass
try:
    import riak
except:
    print "Riak is not available"
    pass
try:
    import redis
except:
    print "Redis is not available"
    pass
try:
    import pymongo
except:
    print "Mongo (pymongo) is not available"
    pass
import tornado
from tornado.options import define, options

iama = 'subwatch'

# TODO: replace the code from tornadoweb with my own
""" This may seem like a long list of params. Perhanps it is, however, they are necessary.
"""
# debug and misc options
define("quiet",           default=False,       help="do not display warnings, errors, or info not related to the data", type=bool)
define("debug",           default=False,       help="display debug messages overriding the quiet flag", type=bool)
define("verbose",         default=False,       help="display debug messages overriding the verbose flag", type=bool)
define("jsonfail",        default="fatal",     help="level to report when json fails to parse the msg data", type=str)
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

# RIAK common options
define("bucket",          default=iama,        help="bucket name when exporting logs", type=str)
define("rhost",           default='localhost', help="riak hostname", type=str)
define("rport",           default=8098,        help="riak port number", type=int)
define("rprefix",         default='riak',      help="interface prefix", type=str)

# MONGO common options
define("phost",           default='localhost', help="mongodb hostname", type=str)
define("pport",           default=27017,       help="mongodb port number", type=int)
define("pdb",             default=iama,        help="mongodb database name", type=str)
define("pcollection",     default=iama,        help="mongodb database collection name", type=str)

# REDIS write options
define("max_messages",    default=100,         help="maximum number of messages to be inserted into a queue (0=unlimited)", type=int)
define("key_format",      default='%s:%s:msg', help="tags to be placed around the key for the message body", type=str)
define("que_format",      default='err:%s:que',help="this is the message queue where all the msg uuid's are stored", type=str)
define("score",           default='%s%010d',   help="format of the score element in the redis:sorted-list", type=str)
define("score_tracker",   default='score_tracker',   help="table to keep track of the daily message id number used in the score", type=str)
define("ttl",             default=0,           help="redis TTL/expire", type=int)

# REDIS/RIAK read options
define("source",          default='redis',     help="where to read the log entries from [redis, riak, mongodb]", type=str)
define("grouped",         default=False,       help="all of the messages that are actionable and filtered are stored in the same redis table", type=bool)
define("last",            default=0,           help="get the last <n> messages", type=int)
define("all",             default=False,       help="get the all messages", type=bool)

# ZeroMQ
define("zmq",             default=False,       help="subscribe to a zmq publisher", type=bool)
define("zurl",            default='tcp://127.0.0.1:5555', help="ZeroMQ URI", type=str)
define("ztimeout",        default=20,          help="zmq poll timeout in seconds", type=int)

# alternate push options
define("name",            default=iama,        help="override the application name when exporting logs", type=str)
define("relay",           default='console',   help="one or more,  comma separated - no spaces, message levels to monitor or action upon [console, redis, riak, mongodb, syslog, userlog]", type=str)

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
all_actions = ['debug','info','warning','error','fatal','exception']
priorities = {
    'debug'     : syslog.LOG_DEBUG,
    'info'      : syslog.LOG_INFO,
    'warning'   : syslog.LOG_WARNING,
    'error'     : syslog.LOG_ERR,
    'fatal'     : syslog.LOG_CRIT,
    'exception' : syslog.LOG_ALERT,
}

# transient data
connections = {}


def now():
    """ formatted time the way I like it. note that the logger does it 
    differently. At some point (ms) might be necessary
    """
    return time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())

timecache = {}
def ubernow():
    """ formatted time the way I like it. note that the logger does it 
    differently. At some point (ms) might be necessary
    """
    global timecache
    t = time.strftime('%Y%m%d%H%M',time.gmtime())
    try:
        x = timecache[t] + 1
        timecache[t] = x
        return "%s.%06d" % (t,x)
    except:
        timecache = {}
        timecache[t] = 0
        return ubernow()


def from_mongo(r, que_format, priority, count):
    return []

def to_mongo(r, priority, msg):
    try:
        score = ubernow()
        que = options.que_format % (priority)        
        if not options.quiet and options.verbose:
            print "mongo verbose: %s -> %s" % (que, score)
        try:
            c = r[que]
        except:
            syslog.syslog(priority, str('collection does not exist'))
            c = pymongo.collection.Collection(r, que, create=True)
            c = r[que]
        c.insert(msg) 
    except:
        (error_type, error_value, error_traceback) = sys.exc_info()
        err = "Unexpected error:", error_type, repr(traceback.format_tb(error_traceback))
        print err
        syslog.syslog(priority, str(err))
        if not options.quiet:
            print err

def from_riak(r, que_format, priority, count):
    return []
    
def to_riak(r, priority, msg):
    try:
        score = ubernow()
        que = options.que_format % (priority)
        if not options.quiet and options.verbose:
            print "riak verbose: %s -> %s" % (que, score)
        bucket = r.bucket(que)
        rec = bucket.new(score, data=msg)
        rec.store()
    except:
        (error_type, error_value, error_traceback) = sys.exc_info()
        err = "Unexpected error:", error_type, repr(traceback.format_tb(error_traceback))
        syslog.syslog(priority, str(err))
        if not options.quiet:
            print err

def from_redis(r, que_format, priority, count):
    """ query the previously captured records from redis. "level' = "priority". Priority is the name
    given to the field in the syslog docs. See the "priorities" hash in order to see which ones
    are supported.

    Arguments:
    priority - the priority level to search the redis DB for.
    count - str='all', else int=the most recent # of records to return

    returns:
    a list of records as they were captured in the DB.
    """
    que = que_format % (priority)
    if type(count) == str and count == 'all':
        return r.zrange(que, 0, -1, withscores=True, score_cast_func=int)
    elif type(count) == int:
        return r.zrange(que, -1 * count, -1, withscores=True, score_cast_func=int)
    else:
        raise Exception('count has the wrong value [%s, %s]' % (type(count), count))


def to_redis(r, priority, msg):
    try:
        d = time.strftime('%y%m%d',time.gmtime())
        score = options.score % (d, r.hincrby(options.score_tracker, d, 1))
        que = options.que_format % (priority)
        if options.debug:
            print "The latest score is [%s]" % (score)
        l = r.zcount(que,'-inf','+inf')
        p = r.pipeline(transaction=True)
        #p.zadd(que, score, msg )
        p.zadd(que, msg, score )
        try:
            # set the bucket to expire if no data inserted after the TTL (seconds)
            p.expire(que, options.ttl)
        except:
            pass
        p.execute()
        if options.max_messages > 0 and l >= options.max_messages:
            r.zremrangebyrank(que,0,(options.max_messages + 1) * -1)
    except:
        (error_type, error_value, error_traceback) = sys.exc_info()
        err = "Unexpected error:", error_type, repr(traceback.format_tb(error_traceback))
        syslog.syslog(priority, str(err))
        if not options.quiet:
            print err

def process(connections,actionable,msg):
    """ this function is going to process the incoming message. This is the
    inner-most part of the message as it was presented from the publishing
    application. Therefore all of the wrappers have been removed.
    """
    if options.debug and options.verbose:
        print "---------------------------------------------------------------------------------"
        print msg
        # the redis logger should be sending JSON conforming messages
    work = False
    try:
        actionable.index(jmsg['level'])
        work = True
    except:
        # not actionable, do nothing
        return
    if work:
        try:
            try:
                options.relay.index('console')
                print msg
            except:
                pass
            try:
                options.relay.index('syslog')
                try:
                    priority = priorities[jmsg['level']]
                except:
                    priority = syslog.LOG_ALERT
                    syslog.syslog(priority, 'The message level is not recognized [%s] add to the priorities map.' % (msg['level']))
                syslog.syslog(priority, m['data'])
            except:
                pass
            try:
                options.relay.index('redis')
                # TODO: send the message to the appropriate redis tables based on priority
                to_redis(connections['redis'], jmsg['level'], msg)
            except:
                pass
            try:
                options.relay.index('riak')
                # TODO: send the message to the appropriate redis tables based on priority
                to_riak(connections['riak'], jmsg['level'], msg)
            except:
                pass
            try:
                options.relay.index('mongodb')
                # TODO: send the message to the appropriate redis tables based on priority
                to_mongo(connections['mongodb'], jmsg['level'], msg)
            except:
                pass
            try:
                options.relay.index('userlog')
                # TODO: define and implement the plug-in strategy
            except:
                pass
        except:
            (error_type, error_value, error_traceback) = sys.exc_info()
            err = "Unexpected error:", error_type, repr(traceback.format_tb(error_traceback))
            priority = syslog.LOG_EMERG
            syslog.syslog(priority, 'something went wrong sending the message out: %s' % (str(err)))

if __name__ == "__main__":
    actionable = []
    tornado.options.parse_command_line()
    if options.actions == 'all':
        actionable = all_actions
    else:
        actionable = options.actions.split(',')
    if not options.quiet:
        print "starting..."
    if options.source.find('redis') >= 0 or options.relay.find('redis') >= 0:
        if not options.quiet and options.verbose:
            print "connecting to redis"
        try:
            if options.socket_path:
                r = redis.StrictRedis(unix_socket_path=options.socket_path)
            else:
                r = redis.StrictRedis(host=options.host, port=options.port, db=options.db)
            r.ping()
            connections['redis'] = r
        except:
            (error_type, error_value, error_traceback) = sys.exc_info()
            err = "could not connect to redis:", error_type, repr(traceback.format_tb(error_traceback))
            if not options.quiet:
                print err
            sys.exit(-1)
    if options.source.find('riak') >= 0 or options.relay.find('riak') >= 0:
        if not options.quiet and options.verbose:
            print "connecting to riak"
        try:
            rk = riak.RiakClient(host=options.rhost, port=options.rport, prefix=options.rprefix)
            connections['riak'] = rk
        except:
            (error_type, error_value, error_traceback) = sys.exc_info()
            err = "could not connect to riak:", error_type, repr(traceback.format_tb(error_traceback))
            if not options.quiet:
                print err
            sys.exit(-1)
    if options.source.find('mongodb') >= 0 or options.relay.find('mongodb') >= 0:
        if not options.quiet and options.verbose:
            print "connecting to mongodb"
        try:
            mo = pymongo.Connection(host=options.phost, port=options.pport)
            db = mo.test
            db.name.index('test')
            
            db = pymongo.database.Database(mo, options.pdb)
            db = mo[options.pdb]
            connections['mongodb'] = db
        except:
            (error_type, error_value, error_traceback) = sys.exc_info()
            err = "could not connect to mongodb:", error_type, repr(traceback.format_tb(error_traceback))
            if not options.quiet:
                print err
            sys.exit(-1)
    if options.all or options.last:
        retval = []
        for a in actionable:
            count = options.last
            if options.all:
                count = 'all'
            remote_data = []
            try:
                options.source.index('redis')
                remote_data = from_redis(r, options.que_format, a, count)
            except:
                pass
            try:
                options.source.index('riak')
                remote_data = from_riak(rk, options.que_format, a, count)
            except:
                pass
            for (data,score) in remote_data:
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
    elif options.zmq:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        print "connecting...", options.zurl
        socket.bind(options.zurl)
        # TODO: add options.channels to the subscribe filter
        socket.setsockopt(zmq.SUBSCRIBE, '')
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        while True:
            # zmq messages are formatted something like:
            #    <channel>::<json formatted message>
            # before passing the "message" to the process() function the
            # wrapper (leading parts must be removed)
            socks = dict(poller.poll(options.ztimeout))
            if socks.get(socket) == zmq.POLLIN:
                msg = socket.recv()
                msg = msg.split('::',1)[1]
                jmsg = json.loads(msg)
                process(connections,actionable,jmsg)

    else:
        try:
            # TODO: work out the exception processing. It is possible that an attempt to exit via CTL+C will
            # fail if the exception is caught in one of the inner try/except blocks.
            try:
                options.relay.index('syslog')
                # TODO: add facilities and options
                syslog.openlog(options.name)
            except:
                pass
            if options.debug:
                print "actions: %s" % (str(actionable))
                print "name: %s, channels [%s, %s]" % (options.name, options.channels, options.channels.split(','))
            p = r.pubsub()
            p.subscribe(options.channels.split(',')[0])
            for msg in p.listen():
                try:
                    jmsg = json.loads(msg['data'])
                except:
                    priority = priorities[options.jsonfail]
                    syslog.syslog(priority, 'failed to decode the message: %s' % (str(err)))
                    # next message
                    continue
                # messages coming from redis have a JSON wrapper around them.
                # since 'jmsg' represents the publisher's message then this
                # is what we should process()
                process(connections, actionable, jmsg)
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
