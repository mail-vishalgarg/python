import pymqi, CMQC, signal, cx_Oracle, time, os, daemon, sys, time, smtplib, traceback
from threading import Thread, Event
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase

contEnv = "sand" in sys.argv[1:] and \
          ("input") or ("test_input")


run = Event()

emailFrom = "vishal.garg@pimco.com"
emailTo = ["vishal.garg@pimco.com","vishal.garg@pimco.com"]


def notifyAlert(body):
     msg            = MIMEMultipart()
     msg['Subject'] = "Test only please ignore it"
     msg['From']    = emailFrom
     msg['To']      = ','.join(emailTo)
     msg.attach(MIMEText( body ))
     s = smtplib.SMTP('mailhost.pimco.imswest.sscims.com')
     s.sendmail(emailFrom, emailTo, msg.as_string())
     s.quit()


def stopDaemon(signum=None, frame=None):
    notifyAlert('daemon stopping')
    run.set()

def getMessage(queue, syncpoint=False, wait=True, waitInterval=60):
    try:
        gmo = pymqi.GMO(Options=CMQC.MQGMO_FAIL_IF_QUIESCING)
        gmo.Options |= wait and CMQC.MQGMO_WAIT or \
                       CMQC.MQGMO_NO_WAIT
        gmo.Options |= syncpoint and CMQC.MQGMO_SYNCPOINT or \
                       CMQC.MQGMO_NO_SYNCPOINT
        gmo.WaitInterval = waitInterval * 1000
        return queue.get(None, None, gmo)
    except pymqi.MQMIError, e:
        if e.comp == CMQC.MQCC_FAILED and \
               e.reason == CMQC.MQRC_NO_MSG_AVAILABLE:
            # no message available
            return None
        else: raise




def main():
    qmgr = pymqi.QueueManager()
    sub  = pymqi.Subscription(qmgr)
    sd   = pymqi.SD(Options=CMQC.MQSO_CREATE  |
                            CMQC.MQSO_RESUME  |
                            CMQC.MQSO_MANAGED |
                            CMQC.MQSO_DURABLE)
    sd.set_vs('SubName', 'rtExistingTopic')
    sd.set_vs('ObjectString', 'RT.EXISTING_SECURITY.QA')
    sub.sub(sd)
    
    try:
         while not run.isSet():
              msg = getMessage(sub, syncpoint=True)
              if msg:
                   try:
                        --%CHANGE HERE%
                        qmgr.commit()
                   except Exception, e:
                        qmgr.backout()
                        raise
    finally:
        sub.close(sub_close_options=CMQC.MQCO_KEEP_SUB,
                  close_sub_queue=True)
        qmgr.disconnect()


if __name__ == '__main__':
    if 'default' in sys.argv:
        notifyAlert('__main__ starting in default mode')
        # allow Ctrl-C to kill it
        signal.signal(signal.SIGINT, stopDaemon)
        
        # run in console
        while not run.isSet():
             try:
                  main()
             except Exception, e:
                  notifyAlert(traceback.format_exc())
                  time.sleep(60)
    else:
        # run as daemon
        notifyAlert('__main__ starting in daemon mode')
        context = daemon.DaemonContext()
        context.signal_map = {
            signal.SIGTERM: stopDaemon
            }
        with context:
            try:
                main()
            except Exception, e:
                notifyAlert(str(e))				
				
				
