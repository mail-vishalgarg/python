import pymqi, CMQC, signal, cx_Oracle, time, os, daemon, sys, time, smtplib, traceback
from threading import Thread, Event
import pmclientrc
import re

def getMessage(queue, syncpoint=False, wait=True, waitInterval=1):
    try:
        gmo = pymqi.GMO(Options=CMQC.MQGMO_FAIL_IF_QUIESCING)
        gmo.Options |= wait and CMQC.MQGMO_WAIT or \
                       CMQC.MQGMO_NO_WAIT
        gmo.Options |= syncpoint and CMQC.MQGMO_SYNCPOINT or \
                       CMQC.MQGMO_NO_SYNCPOINT
        gmo.WaitInterval = waitInterval * 1
        return queue.get(None, None, gmo)
    except pymqi.MQMIError, e:
        if e.comp == CMQC.MQCC_FAILED and \
            e.reason == CMQC.MQRC_NO_MSG_AVAILABLE:
            # no message available
            return str(None)
        else: raise

def topic_value(tname):
    #run = Event()
    msg = ''
    subname = ''
    fileobj = open('mq_topic.log','a+b')
    queue_manager = pmclientrc.QA_QUEUE_MANAGER
    queue_name = pmclientrc.QA_QUEUE_NAME
    channel = pmclientrc.QA_CHANNEL
    host = pmclientrc.QA_HOST
    port = pmclientrc.QA_PORT
    conn_info = "%s(%s)" % (host, port)
    qmgr = pymqi.QueueManager()
    qmgr.connect_tcp_client(queue_manager, pymqi.CD(), channel, conn_info)
    if tname == 'RT.EXISTING_SECURITY.QA':
        subname = 'existing'
    elif tname == 'RT.NEW_SECURITY.QA':
        subname = 'new_security'
    elif tname == 'RT.COLLAT':
        subname = 'collat'
    elif tname == 'RT.TICKET.QA.TOPIC':
        subname = 'ticket_topic'
    elif tname == 'RT.ORDER':
        subname = 'order'
    topicname = tname
    topic_string = re.sub(r'\.','/',topicname)
    sub  = pymqi.Subscription(qmgr)
    sd   = pymqi.SD(Options=CMQC.MQSO_CREATE  |
                            CMQC.MQSO_RESUME  |
                            CMQC.MQSO_MANAGED |
                            CMQC.MQSO_DURABLE)
    sd.set_vs('SubName', subname)
    sd.set_vs('ObjectString', topic_string)
    sub.sub(sd)
    msg = getMessage(sub, syncpoint=True)
		
    fileobj.write(subname + '|' +  msg+"\n")
    sub.close(sub_close_options=CMQC.MQCO_KEEP_SUB,
    close_sub_queue=True)
    qmgr.disconnect()

def threadstart():
    Thread(target=topic_value,args=('RT.EXISTING_SECURITY.QA',)).start()
    Thread(target=topic_value,args=('RT.NEW_SECURITY.QA',)).start()
    Thread(target=topic_value,args=('RT.TICKET.QA.TOPIC',)).start()
    Thread(target=topic_value,args=('RT.COLLAT',)).start()
    Thread(target=topic_value,args=('RT.ORDER',)).start()

		
