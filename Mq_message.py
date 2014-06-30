import pymqi

class mq_message(object):
	
	def __init__(self):
		self.queue_manager = 'devqamq.queue.manager'
		self.channel = 'SYSTEM.DEF.SVRCONN'
		self.host = 'devpmmq1'
		self.port = '1414'
		self.queue_name = 'QA1.TICKET.N71.QUEUE'
		self.conn_info = '{}({})'.format(self.host, self.port)
		
	def mq_connect(self):
		self.qmgr = pymqi.connect(self.queue_manager, self.channel, self.conn_info)
		return self.qmgr

	def put_msg(self,msg):
		self.queue = pymqi.Queue(self.qmgr, self.queue_name)
		self.queue.put(msg)
		self.queue.close()
	
	def mq_disconnect(self):
		self.qmgr.disconnect()

