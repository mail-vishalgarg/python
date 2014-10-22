import pymqi, CMQC
from threading import Thread, Event
from pmclientrc import *
import re

run = Event()

def getMessage(queue, syncpoint=False, wait=True, waitInterval=1):
    try:
        gmo = pymqi.GMO(Options=CMQC.MQGMO_FAIL_IF_QUIESCING)
        gmo.Options |= wait and CMQC.MQGMO_WAIT or \
                       CMQC.MQGMO_NO_WAIT
        gmo.Options |= syncpoint and CMQC.MQGMO_SYNCPOINT or \
                       CMQC.MQGMO_NO_SYNCPOINT
        gmo.WaitInterval = waitInterval * 2000
        return queue.get(None, None, gmo)
    except pymqi.MQMIError, e:
        if e.comp == CMQC.MQCC_FAILED and \
            e.reason == CMQC.MQRC_NO_MSG_AVAILABLE:
            # no message available
            return str(None)
        else: raise

def topic_value(tname,file):
    #run = Event()
    msg = ''
    subname = ''
    fileobj = open(file,'a+b')
    queue_manager = QA_QUEUE_MANAGER
    queue_name = QA_QUEUE_NAME
    channel = QA_CHANNEL
    host = QA_HOST
    port = QA_PORT
    conn_info = "%s(%s)" % (host, port)
    qmgr = pymqi.QueueManager()
    qmgr.connect_tcp_client(queue_manager, pymqi.CD(), channel, conn_info)
    if tname == QA_EXISTING_SECURITY:
        subname = 'existing'
    elif tname == QA_NEW_SECURITY:
        subname = 'new_security'
    elif tname == QA_COLLAT:
        subname = 'collat'
    elif tname == QA_TICKET:
        subname = 'ticket_topic'
    elif tname == QA_ORDER:
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
    while not run.isSet():
      msg = getMessage(sub, syncpoint=True)
      if msg != 'None': 
        fileobj.write("\n\n")
        fileobj.write(subname + '---' +  msg + "\n\n")
    sub.close(sub_close_options=CMQC.MQCO_KEEP_SUB, close_sub_queue=True)
    qmgr.disconnect()

def stop_thread():
	run.set()

def threadstart(sig=False):
  existing_file = mqExistingTopicFile
  new_sec_file = mqNewSecTopicFile
  ticket_file = mqTicketTopicFile
  order_file = mqOrderTopicFile
  collat_file = mqCollatTopicFile
  if not sig:
    Thread(target=topic_value,args=(QA_EXISTING_SECURITY,existing_file)).start()
    Thread(target=topic_value,args=(QA_NEW_SECURITY,new_sec_file)).start()
    Thread(target=topic_value,args=(QA_TICKET,ticket_file)).start()
    Thread(target=topic_value,args=(QA_COLLAT,collat_file)).start()
    Thread(target=topic_value,args=(QA_ORDER,order_file)).start()
  if sig:
    Thread(target=stop_thread).start()		
