import xml.dom.minidom
import cx_Oracle
import sys,os
import commands
import csv,re
import getopt
import operator
import pmclientrc
import pm_DbCon
#from truncate_table import *
import truncate_table

def usage():
	print 'This script will prepare input message for sand box'
	print 'Options:'
	print '-E		Environment type(sand, prod or dev)'
	print '-D		Date to get data in (DD-MON-YYYY format)'
	print '-T		(Optional) To truncate taps and stp tables'
	print '-S		Start date format <DD-MON-YYYY hh.mm.ss.zzz>'
	print '-F		End date format <DD-MON-YYYY hh.mm.ss.zzz>'
	print 'Example: ' + scriptName + ' -E <sand/prod>' + ' -D <Date format>' + ' -T'
	print 'Example: ' + scriptName + ' -E <sand/prod>' + '-S <Date and time format>' + ' -F <Date and time format>' + ' -T'
	print "NOTE: Need to provied only either -D option or (-S and -F) opiton together"
	sys.exit()

def trim(str):
	""" This function will trim any string"""
	string= re.sub(r'^\s*','',str)
	string= re.sub(r'$\s*','',string)
	return string

def main(argv):
	global envtype,validate_date,pattern_list
	global scriptName,start_flag,end_flag,date_flag,start_date, end_date
	envtype = ''
	validate_date = ''
	trunc_flag = False
	start_flag = False
	end_flag = False
	date_flag = False
	start_date = ''
	end_date = ''
	scriptName = os.path.basename(__file__)
	try:
		opts, args = getopt.getopt(argv,"hE:D:S:F:T",["db=","day=","start=","finish="])
	except getopt.GetoptError:
		usage()
	for opt, arg in opts:
		if opt == '-h':
			usage()
		elif opt in ("-E", "--db"):
				envtype = arg
		elif opt in ("-D", "--day"):
				date_flag = True
				validate_date = arg
		elif opt == "-T":
				trunc_flag = True
		elif opt in ("-S", "--start"):
				start_flag = True
				start_date = arg
		elif opt in ("-F", "--finish"):
				end_flag = True	
				end_date = arg
	#print envtype, validate_date
	if trunc_flag == True:
		clear_tab = truncate_table.Clear_Tables()
		clear_tab.truncate_tab('sand')
	##When only -T option passed

	if envtype == '' and date_flag == False and start_flag == False and end_flag == False:
		sys.exit()

	##Pattern file contain combination of Record and Event type which
	##Creat pattern list for validation criteria
	try:
		pattern_file_obj = open('fob_pattern_list.txt','r')
	except IOError as err:
		print err
		sys.exit(1)
	pattern_list = [trim(list) for list in pattern_file_obj if not list.isspace()]	

class DB_operation(object):
	def __init__(self): 
		self.act_ident_list = []  
		self.prv_ident_list = []
		self.iden_xml_list = []
		self.acc_data = []
		self.env_var = envtype 
		self.day = validate_date 
		self.date_flag = date_flag
		self.start_flag = start_flag
		self.end_flag = end_flag
		self.start_date = start_date
		self.end_date = end_date
		self.xml_id = open('Total_identifier_xml_list.txt','w')  
		self.conn = dbobj.connect(self.env_var)
		self.pattern_list = pattern_list

	def uniq(self,inlist): 
		# order preserving
		self.uniques = [] 
		for item in inlist:
			if item not in self.uniques:
				self.uniques.append(item)
		return self.uniques

	def write_mq_message(self,uniq_identifier_list,total_raw_list):
		self.act_mqip = []
		self.temp = []
		self.mqfile =open('mq_input_file.txt','w')
		#self.wf=csv.writer(self.mqfile, lineterminator='\n')
		for ssm_id in uniq_identifier_list:
			for raw in total_raw_list:
				if ssm_id == raw[0]:
#			 		self.temp.append(raw)
#					if len(self.temp) >= 1:
			 		self.string = '---'.join(raw)
			 		self.temp.append(self.string)
	 	self.temp.sort(key=operator.itemgetter(0,1)) 
	 	for row in self.temp:
			self.mqfile.write(row + "\n")
	 	self.mqfile.close() 

##To check identifier exist in the database
	def get_ssm_id(self,identifier_list):
		self.bb_idlist = []
		self.ssm_idlist = []
		self.core_matchfile =open('core_ssm_match_file.csv','w')
		self.cmf=csv.writer(self.core_matchfile, lineterminator='\n')
		for id_lst in identifier_list:
				  query1 = 'select ssm_id from ' + pmclientrc.TAPS_SSM_CORE + ' where bb_id = \'%s\' and ssm_id != bb_id' % id_lst
				  dbobj.execute(query1)
				  self.cnt = dbobj.fetch_all()
				  if len(self.cnt) != 0:
						  #print  "BB ID %s " % self.cnt[0]
						  self.ssm_idlist.append(id_lst)
						  self.bb_idlist.extend(self.cnt[0])
						  self.cmf.writerow(self.cnt[0])		 
				  query2 = 'select ssm_id from '+ pmclientrc.TAPS_SSM_CORE + ' where ssm_id = \'%s\'' % id_lst 
				  dbobj.execute(query2)
				  self.cnt = dbobj.fetch_all()
				  if len(self.cnt) != 0:
						  self.ssm_idlist.append(id_lst)
						  self.bb_idlist.extend(self.cnt[0])
						  self.cmf.writerow(self.cnt[0])		 
		self.core_matchfile.close() 
		self.ssm_bb_id_match_list = self.uniq(self.bb_idlist)
		#print "MATCH ID: %s" % self.ssm_bb_id_match_list
		self.ssm_match_list = self.uniq(self.ssm_idlist)
		self.write_mq_message(self.ssm_match_list,self.iden_xml_list)
		self.pmbcp(self.ssm_bb_id_match_list) 

##Copied data from Prod database to Sandbox for indentifier listed in three table mentioned below
	def pmbcp(self,identifier_list):
		#print "IDENT LIST: %s" % identifier_list
		get_sublist = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]
		sublist = get_sublist(identifier_list,500 )
		#print "SUBLIST: %s " % sublist
		tbl_list = [pmclientrc.TAPS_SSM_CORE,pmclientrc.TAPS_SSM_FIRMTOTAL,pmclientrc.TAPS_SSM_OPTION]
		for slist in sublist:
			jlist = '\',\''.join(slist)
			for tbl in tbl_list:
				 status, output = commands.getstatusoutput("pmbcp -d -t \"<eoc>\" -r \"<eoln>\" -T Oracle -D ORAPRDPIM %s -f \"where ssm_id in (\'%s\')\" out /tmp/pmbcp_ssm.dat -U svc_qc -P svc_qc01 -2D ORAQASBX1 -2t %s  -2U svc_cm -2P svccmsbx -nls_lang WE8MSWIN1252" % (tbl,jlist,tbl))
				 if status != 0: #this status varaible will be zero when above command execute successfully without any error.
						 print "***********Below is type of Error occured while coping the data**************** \n \n %s" % output
				 else:
						 print "*********************** %s copy success" % tbl
						 words = ['Rows successfully loaded']
						 matched_lines = [line for line in output.split('\n') if True in (i in line for i in words)]
						 if len(matched_lines) != 0:
							 for line in matched_lines:
									print "\t\t %s" % line 
						 else:
							  print "0 Rows loaded"	 
						 print "################################################################################## \n \n"


##Validate pattern for PM/BM Creation		 
	def validate(self,pat,doc):
	  #self.pattern_list = ['BM BM CORRECT', 'BM BM CREATION', 'BM MASTER TICKET CREATE', 'BM TRADE TICKET CREATE', 'CBM WI CONVERSION', 'CMR CORRECTED MASTER TICKET', 'KMR CLOSED MASTER TICKET', 'MRC NEW MASTER TICKET', 'PCBM WI CONVERSION', 'BM NEW MASTER TICKET']
		#self.pattern_list = ['BM BM CREATION', 'PM* PM CREATION','OMS NEW ALLOCATION','PM* NEW ALLOCATION','OM3 ALLOCATION' ]
	  #It will return matched data from pattern_list and passed pattern
		self.val_status = filter(lambda req_var : req_var == pat,self.pattern_list) 
		if len(self.val_status) != 0:	 
			#print "%10d %10s %s" %(doc[0],pat.ljust(25),doc[1])
			self.parser_iden(doc,pat) 

##Parse xml for the identifier and make input inbound_raw_message data for FOB
	def parser_iden(self,doc,rec_type_event):
	 self.ip_doc = doc[3]
	 try:
	  self.dom = xml.dom.minidom.parseString(self.ip_doc)
	  self.rec_var = self.dom.getElementsByTagName('IDENTIFIER')
	  self.eve_var = self.dom.getElementsByTagName('PRICE')
	  #self.pat = ' '.join([self.rec_var[0].firstChild.nodeValue, self.eve_var[0].firstChild.nodeValue])
	  self.ident = self.rec_var[0].firstChild.nodeValue.encode('ascii')
	  self.prv_ident_list.append(self.ident) 
	  #self.new_list = '---'.join([self.ident.encode('ascii'),rec_type_event.encode('ascii'), self.ip_doc.encode('ascii')]) 
	  #self.xml_id.write(self.new_list + "\n")
	  ##print self.new_list
	  #self.inbound_raw_msg = self.new_list.split('---') 
	  #print self.inbound_raw_msg
	  self.inbound_raw_msg = [self.ident.encode('ascii'),rec_type_event.encode('ascii'), self.ip_doc.encode('ascii')]
	  self.iden_xml_list.append(self.inbound_raw_msg)
	 except IndexError:
	  pass 

##Parse xml message and make PM/BM creation pattern list
	def parser(self,doc):
	 self.ip_doc = doc[3]
	 #print doc[3]
	 try:
	  self.dom = xml.dom.minidom.parseString(self.ip_doc)
	 except xml.parsers.expat.ExpatError:
	  pass
	  #self.rec_var = self.dom.getElementsByTagName(self.xml_tag1)
	  #self.eve_var = self.dom.getElementsByTagName(self.xml_tag2)
	 try:
	  self.rec_var = self.dom.getElementsByTagName('RECORDTYPE')
	  self.eve_var = self.dom.getElementsByTagName('EVENT')
	  self.pat = ' '.join([self.rec_var[0].firstChild.nodeValue, self.eve_var[0].firstChild.nodeValue])
	  #Pass PM/BM pettrn for validation to validate method
	  self.validate(self.pat,doc)
	 except IndexError:
	  pass 

##Get the data from database for date passed as argument
	def get_dbrows(self):
	 #Database query to get all data for date passed in argument
		if self.date_flag:
			self.start_flag = False
			self.end_flag = False
			query_dbrow = 'select * from '+pmclientrc.STP_INBOUND_RAW_MSG+' where to_char(RECEIVED_AT,\'DD-MON-YYYY\') = \'%s\' and rownum < 10000 order by RECEIVED_AT asc' % self.day
		elif start_flag and end_flag:
			self.date_flag = False	
	 		query_dbrow = 'select * from '+pmclientrc.STP_INBOUND_RAW_MSG+' where RECEIVED_AT BETWEEN to_timestamp(\'%s\',\'DD-MM-YYYY HH24.MI.SS.FF9\') and to_timestamp(\'%s\',\'DD-MM-YYYY HH24.MI.SS.FF9\')' % (self.start_date,self.end_date)

##Don't delete below commented queries, they could be useful
	 #query_dbrow = 'select * from '+pmclientrc.STP_INBOUND_RAW_MSG+' where to_char(RECEIVED_AT,\'DD-MON-YYYY\') = \'%s\'' % self.day
	 #query_dbrow = 'select * from '+pmclientrc.STP_INBOUND_RAW_MSG+' where to_char(RECEIVED_AT,\'DD-MON-YYYY\') = \'%s\' order by RECEIVED_AT asc' % self.day
	 #query_dbrow = 'select * from '+pmclientrc.STP_INBOUND_RAW_MSG+' where to_char(RECEIVED_AT,\'DD-MON-YYYY\') = \'%s\' and rownum < 3000' % self.day
	 #query_dbrow = 'select * from '+pmclientrc.STP_INBOUND_RAW_MSG+' where to_char(RECEIVED_AT) BETWEEN \'04-FEB-2014 10.00.00.000\' and \'04-FEB-2014 16.00.00.000\''

		dbobj.execute(query_dbrow)
	 	fetch_all_row = dbobj.fetch_all()
	 	print "starting data validation"
	 	#Fetch all data and pass one be one to parser method
	 	for self.row in fetch_all_row:
			self.parser(self.row)
		print "end of data validation"
	 	self.uniq_identifier_list = self.uniq(self.prv_ident_list)  
	 	self.get_ssm_id(self.uniq_identifier_list)
	 	self.xml_id.close()
	 	dbobj.disconnect()
	 	#for item in self.iden_xml_list:
	 	#	 print item

if __name__ == '__main__':
		main(sys.argv[1:])
		dbobj = pm_DbCon.oracle()
		obj = DB_operation() 
		obj.get_dbrows()
