import cx_Oracle
import pmclientrc
import sys

class oracle(object):
	def connect(self, env):
		if env == 'dev':
			dbname = pmclientrc.PM_DEVDB
			dbusr = pmclientrc.PM_DEVUSER
			dbpw = pmclientrc.PM_DEVPW
		elif env == 'prod':
			dbname = pmclientrc.PM_PRODDB
			dbusr = pmclientrc.PM_PRODUSER
			dbpw = pmclientrc.PM_PRODPW
		elif env == 'sand':
			dbname = pmclientrc.PM_SANDDB
			dbusr = pmclientrc.PM_SANDUSER
			dbpw = pmclientrc.PM_SANDPW
		try:
			self.db = cx_Oracle.connect(dbusr, dbpw, dbname)
		except cx_Oracle.DatabaseError as e:
			error, = e.args
			if error.code == 1017:
				print ('Please check your credentials.')
			else:
				print('Database connection error: %s' .format(e))
			raise
		self.cur = self.db.cursor()	

	def execute(self, query):
		self.query = query
		self.cur.execute(query)

	def fetch_all(self):
		self.cur.execute(self.query)
		return self.cur.fetchall()
	
	def fetch_one(self,query):
		self.cur.execute(query)
		return self.cur.fetchone()

	def disconnect(self):
		self.db.close()

	def rowcount(self):
		return self.cur.rowcount

	def commit(self):
		self.db.commit()
