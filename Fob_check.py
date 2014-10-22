import xml.dom.minidom
import cx_Oracle
import sys,os,time,shutil
import commands
import csv,re
import getopt
import operator
from pmclientrc import *
import pm_DbCon
#from truncate_table import *
import truncate_table
import pprint
import logging.config
import traceback
from time import sleep

logging.config.fileConfig(fobCheckLoggingFile)
my_logger = logging.getLogger(' ')

def usage():
	print 'This script will prepare input message for sand box'
	print 'Options:'
	print '-E		Environment type(sand, prod or dev)'
	print '-D		Date to get data in (DD-MON-YYYY format)'
	print '-T		(Optional) To truncate taps and stp tables'
	print '-S		Start date format <DD-MON-YYYY HH24:MM:SS:FF9>'
	print '-F		End date format <DD-MON-YYYY HH24:MM:SS:FF9>'
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
	global bm_dict,pm_dict, alloc_dict
	global final_dict

	bm_dict = {}
	pm_dict = {}
	alloc_dict = {}
	final_dict = {}

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
		my_logger.error(err)
		sys.exit(1)
	pattern_list = [trim(list) for list in pattern_file_obj if not list.isspace()]	

class DB_operation(object):
	def __init__(self): 
		self.act_ident_list = []  
		self.prv_ident_list = []
		#self.prv_msgId_list = []
		self.msg_identifier_dict = {}
		self.iden_xml_list = []
		self.acc_data = []
		self.env_var = envtype 
		self.day = validate_date 
		self.date_flag = date_flag
		self.start_flag = start_flag
		self.end_flag = end_flag
		self.start_date = start_date
		self.end_date = end_date
		self.pid = str(os.getpid())
		self.currentDate = str(time.strftime("%d-%m-%Y"))
		self.identifier_file_name = fobInputDir + 'Total_identifier_xml_list.' + self.currentDate +'.'+ self.pid + '.txt' 
		self.xml_id = open(self.identifier_file_name,'w')  
		self.all_msg_file_name = fobInputDir + 'Fob_all_msg.' + self.currentDate + '.' + self.pid + '.txt'
		self.all_msg = open(self.all_msg_file_name,'w')
		self.conn = dbobj.connect(self.env_var)
		self.pattern_list = pattern_list
		#self.write_count = 0
		self.summary_fob_check_file = fobInputDir + 'summary_fob_check.' + self.currentDate + '.' + self.pid + '.txt'
		self.summary_file_obj = open(self.summary_fob_check_file,'w')	
		self.match_xml_msg_list = []
		self.before_bm_count = 0
		self.before_bm_correct_count = 0
		self.before_trade_ticket_count = 0
		self.before_new_mst_tk_count = 0
		self.before_alloc_sent_count = 0
		self.before_p_change_count = 0
		self.before_pm_count = 0
		self.before_alloc_count = 0
		self.before_trade_rel_count = 0
		self.before_mst_trade_tkt_count = 0
		self.before_trade_conf_count = 0
		self.before_pm_trade_tkt_count = 0
		self.before_pm_wi_conv_count = 0
		self.before_pm_accept_count = 0 
		self.before_pm_error_count = 0 
		self.before_pm_reject_count = 0
		self.before_pm_manual_count = 0 
		self.before_pm_inquiry_count = 0 
		self.before_pm_inventor_count = 0 
		self.before_pm_presplit_count = 0 
		self.before_pm_assetse_count = 0 
		self.before_pm_sent_count = 0 
		self.before_pm_pending_count = 0 
		self.before_pm_saved_count = 0 
		self.before_pm_new_count = 0 
		self.before_pm_correct_count = 0

		self.after_bm_count = 0
		self.after_bm_correct_count = 0
		self.after_trade_tkt_count = 0
		self.after_new_mst_tkt_count = 0
		self.after_alloc_sent_count = 0
		self.after_p_change_count = 0
		self.after_trade_rel_count = 0	
		self.after_pm_count = 0
		self.after_alloc_count = 0
		self.after_mst_trade_tkt_count = 0
		self.after_trade_conf_count = 0
		self.after_pm_trade_tkt_count = 0
		self.after_pm_wi_conv_count = 0
		self.after_pm_accept_count = 0
		self.after_pm_error_count = 0
		self.after_pm_reject_count = 0
		self.after_pm_manual_count = 0
		self.after_pm_inquiry_count = 0
		self.after_pm_inventor_count = 0
		self.after_pm_presplit_count = 0
		self.after_pm_assetse_count = 0
		self.after_pm_sent_count = 0
		self.after_pm_pending_count = 0
		self.after_pm_saved_count = 0
		self.after_pm_new_count = 0
		self.after_pm_correct_count = 0
		self.uniq_xml_msg_list = []

	def uniq(self,inlist): 
		# order preserving
		self.uniques = [] 
		for item in inlist:
			if item not in self.uniques:
				self.uniques.append(item)
		return self.uniques

	def write_mq_message(self,uniq_identifer_list,total_raw_list):
		self.temp = []
		self.match_ticket_msg = []
		self.write_count = 0

		mq_file_name = fobInputDir + 'mq_input_file.txt'
		summary_file_name = fobInputDir + 'summary_fob_check.txt'
		self.mqfile =open(mq_file_name,'w')
		self.backup_mqfile = fobInputDir + 'mq_input_file.' + self.currentDate + '.' + self.pid + '.txt'

		#print "LLLL: XML _MSG:",len(total_raw_list)
		#print "FFFF: len for final dic:", len(final_dict)

		##DO NOT DELETE BELOW PARENT  CHILD CODE
		##Get Message using Parent Child relations using ticket no.
		'''
		for key, value in sorted(final_dict.items()):
			for raw in total_raw_list:
				if key == raw[1]:
					self.match_ticket_msg.append(raw)

		print "No. of msg get after parent child filter: %s" % len(self.match_ticket_msg)	
		self.summary_file_obj.write("No. of message After Pareent Child Filter:%s "% len(self.match_ticket_msg) + "\n")
		my_logger.info("No. of msg get after parent child filter: %s" %len(self.match_ticket_msg))
		'''

		for identifier in uniq_identifer_list:
			#for raw_msg in self.match_ticket_msg:
			for raw_msg in total_raw_list:
				if identifier == raw_msg[0]:
					self.string = '---'.join(raw_msg)
					if self.string not in self.temp:
						self.write_count += 1
						self.temp.append(self.string)

		#Get the count for Event type
		for msg in self.temp:
			#identifier, ticket,record_event,msg,ssm_id,cstc= msg.split('---')
			match = re.search(r'(.*?)---(.*?)---(.*?)---(.*)',msg)
			match_string = match.group(3)	
			record_type,event_name = match_string.split(' ',1)	
			if record_type == 'BM':
				if event_name == 'BM CREATION':
					self.after_bm_count += 1
				elif event_name == 'BM CORRECT':
					self.after_bm_correct_count += 1
				elif event_name == 'TRADE TICKET CREATE':
					self.after_trade_tkt_count += 1
				elif event_name == 'NEW MASTER TICKET':
					self.after_new_mst_tkt_count += 1
				elif event_name == 'ALLOCATION SENT':
					self.after_alloc_sent_count += 1
				elif event_name == 'PRICE CHANGE':
					self.after_p_change_count += 1
				elif event_name == 'TRADE RELEASED':
					self.after_trade_rel_count += 1
				elif event_name == 'TRADE CONFIRMED':
					self.after_trade_conf_count += 1
				elif event_name == 'MASTER TICKET CREATE':
					self.after_mst_trade_tkt_count += 1

			if record_type == 'PM*' or record_type == 'PM':
				if event_name == 'PM CREATION':
					self.after_pm_count += 1
				elif event_name == 'TRADE TICKET CREATE':
					self.after_pm_trade_tkt_count += 1
				elif event_name == 'WI CONVERSION':
					self.after_pm_wi_conv_count += 1
				elif event_name == 'ACCEPT':
					self.after_pm_accept_count += 1
				elif event_name == 'ERROR':
					self.after_pm_error_count += 1
				elif event_name == 'REJECT':
					self.after_pm_reject_count += 1
				elif event_name == 'MANUAL':
					self.after_pm_manual_count += 1
				elif event_name == 'INQUIRY':
					self.after_pm_inquiry_count += 1
				elif event_name == 'INVENTOR':
					self.after_pm_inventor_count += 1
				elif event_name == 'PRESPLIT':
					self.after_pm_presplit_count += 1
				elif event_name == 'ASSET-SE':
					self.after_pm_assetse_count += 1
				elif event_name == 'SENT':
					self.after_pm_sent_count += 1
				elif event_name == 'PENDING':
					self.after_pm_pending_count += 1
				elif event_name == 'SAVED':
					self.after_pm_saved_count += 1
				elif event_name == 'NEW':
					self.after_pm_new_count += 1
				elif event_name == 'PM CORRECT':
					self.after_pm_correct_count += 1

			if event_name == 'ALLOCATION' or event_name == 'NEW ALLOCATION':
				self.after_alloc_count += 1
			 
		print "BM CREATION Count after ssm_id :%s" % self.after_bm_count
		print "PM CREATION Count after ssm_id :%s" % self.after_pm_count
		print "ALLOCATION Count after ssm_id :%s" % self.after_alloc_count
		print "BM CORRECT Count after ssm_id :%s" % self.after_bm_correct_count
	
		print "No. of msg with ssm_id : %s" % self.write_count
		self.summary_file_obj.write("No. of message after ssm_id Filter:%s " % self.write_count + "\n")
		my_logger.info("No. of msg with ssm_id: %s" % self.write_count)

		self.summary_file_obj.write("BM CREATION count after ssm_id filter: %s" % self.after_bm_count + "\n")
		self.summary_file_obj.write("PM CREATION count after ssm_id filter: %s" % self.after_pm_count + "\n")
		self.summary_file_obj.write("ALLOCATION count after ssm_id filter: %s" % self.after_alloc_count + "\n")
		self.summary_file_obj.write("BM CORRECT count after ssm_id filter: %s" % self.after_bm_correct_count + "\n")
		self.summary_file_obj.write("BM TRADE TICKET count after ssm_id filter: %s" % self.after_trade_tkt_count + "\n")
		self.summary_file_obj.write("BM NEW MASTER TICKET after ssm_id filter: %s" % self.after_new_mst_tkt_count + "\n")
		self.summary_file_obj.write("BM ALLOCATION SENT after ssm_id filter: %s" % self.after_alloc_sent_count + "\n")
		self.summary_file_obj.write("BM PRICE CHANGE after ssm_id filter: %s" % self.after_p_change_count + "\n")
		self.summary_file_obj.write("BM TRADE RELEASE after ssm_id filter: %s" % self.after_trade_rel_count + "\n")
		self.summary_file_obj.write("BM TRADE CONFIRMED after ssm_id filter: %s" % self.after_trade_conf_count + "\n")
		self.summary_file_obj.write("BM MASTER TICKET CREATE after ssm_id filter: %s" % self.after_mst_trade_tkt_count + "\n")
		self.summary_file_obj.write("PM TRADE TICKET CREATE after ssm_id filter: %s" % self.after_pm_trade_tkt_count + "\n")
		self.summary_file_obj.write("PM WI CONVERSION after ssm_id filter: %s" % self.after_pm_wi_conv_count + "\n")
		self.summary_file_obj.write("PM ACCEPT after ssm_id filter: %s" % self.after_pm_accept_count + "\n")
		self.summary_file_obj.write("PM ERROR after ssm_id filter: %s" % self.after_pm_error_count + "\n")
		self.summary_file_obj.write("PM REJECT after ssm_id filter: %s" % self.after_pm_reject_count + "\n")
		self.summary_file_obj.write("PM MANUAL after ssm_id filter: %s" % self.after_pm_manual_count + "\n")
		self.summary_file_obj.write("PM INQUIRY after ssm_id filter: %s" % self.after_pm_inquiry_count + "\n")
		self.summary_file_obj.write("PM INVENTOR after ssm_id filter: %s" % self.after_pm_inventor_count + "\n")
		self.summary_file_obj.write("PM PRESPLIT after ssm_id filter: %s" % self.after_pm_presplit_count + "\n")
		self.summary_file_obj.write("PM ASSET-SE after ssm_id filter: %s" % self.after_pm_assetse_count + "\n")
		self.summary_file_obj.write("PM SENT after ssm_id filter: %s" % self.after_pm_sent_count + "\n")
		self.summary_file_obj.write("PM PENDING after ssm_id filter: %s" % self.after_pm_pending_count + "\n")
		self.summary_file_obj.write("PM SAVED after ssm_id filter: %s" % self.after_pm_saved_count + "\n")
		self.summary_file_obj.write("PM NEW after ssm_id filter: %s" % self.after_pm_new_count + "\n")
		self.summary_file_obj.write("PM PM CORRECT after ssm_id filter: %s" % self.after_pm_correct_count + "\n")

		my_logger.info("BM CREATION count after ssm_id filter: %s" % self.after_bm_count)
		my_logger.info("PM CREATION count after ssm_id filter: %s" % self.after_pm_count)
		my_logger.info("ALLOCATION count after ssm_id filter: %s" % self.after_alloc_count)
		my_logger.info("BM CORRECT count after ssm_id filter: %s" % self.after_bm_correct_count)
		my_logger.info("BM TRADE TICKET count after ssm_id filter: %s" % self.after_trade_tkt_count)
		my_logger.info("BM NEW MASTER TICKET after ssm_id filter: %s" % self.after_new_mst_tkt_count)
		my_logger.info("BM ALLOCATION SENT after ssm_id filter: %s" % self.after_alloc_sent_count)
		my_logger.info("BM PRICE CHANGE after ssm_id filter: %s" % self.after_p_change_count)
		my_logger.info("BM TRADE RELEASE after ssm_id filter: %s" % self.after_trade_rel_count)
		my_logger.info("BM TRADE CONFIRMED after ssm_id filter: %s" % self.after_trade_conf_count)
		my_logger.info("BM MASTER TICKET CREATE after ssm_id filter: %s" % self.after_mst_trade_tkt_count)
		my_logger.info("PM TRADE TICKET CREATE after ssm_id filter: %s" % self.after_pm_trade_tkt_count)
		my_logger.info("PM WI CONVERSION after ssm_id filter: %s" % self.after_pm_wi_conv_count)
		my_logger.info("PM ACCEPT after ssm_id filter: %s" % self.after_pm_accept_count)
		my_logger.info("PM ERROR after ssm_id filter: %s" % self.after_pm_error_count)
		my_logger.info("PM REJECT after ssm_id filter: %s" % self.after_pm_reject_count)
		my_logger.info("PM MANUAL after ssm_id filter: %s" % self.after_pm_manual_count)
		my_logger.info("PM INQUIRY after ssm_id filter: %s" % self.after_pm_inquiry_count)
		my_logger.info("PM INVENTOR after ssm_id filter: %s" % self.after_pm_inventor_count)
		my_logger.info("PM PRESPLIT after ssm_id filter: %s" % self.after_pm_presplit_count)
		my_logger.info("PM PRESPLIT after ssm_id filter: %s" % self.after_pm_presplit_count)
		my_logger.info("PM ASSET-SE after ssm_id filter: %s" % self.after_pm_assetse_count)
		my_logger.info("PM SENT after ssm_id filter: %s" % self.after_pm_sent_count)
		my_logger.info("PM PENDING after ssm_id filter: %s" % self.after_pm_pending_count)
		my_logger.info("PM SAVED after ssm_id filter: %s" % self.after_pm_saved_count)
		my_logger.info("PM NEW after ssm_id filter: %s" % self.after_pm_new_count)
		my_logger.info("PM PM CORRECT after ssm_id filter: %s" % self.after_pm_correct_count)

	 	for row in self.temp:
			self.mqfile.write(row + "\n")
	 	self.mqfile.close() 

		self.summary_file_obj.close()
		shutil.copy(mq_file_name,self.backup_mqfile)
		shutil.copy(self.summary_fob_check_file,summary_file_name)

##To check identifier exist in the database
	def get_ssm_id(self,msg_iden_dict):
		self.ssm_idlist = []
		self.msg_ssm_dict = {}	
		self.identifier_list = []
		self.upload_ssm_id_list = []
		self.cstc_ssm_id = {}
		self.ssm_identifier_dict = {}
		self.msg_ssm_cstc_dict = {}
		self.xml_msg_list = []

		uniq_ssm_id_file = fobInputDir + 'Fob_uniq_ssmId.' + self.currentDate + '.' + self.pid + '.txt'
		uniq_ssm_obj = open(uniq_ssm_id_file,'w')
		#print "Total no. of pattern match message: %s" % len(self.iden_xml_list)
		#self.summary_file_obj.write("No. of message After Pattern Match: %s"% len(self.iden_xml_list) + "\n")
		my_logger.info("Total no. of pattern match message: %s" % len(self.iden_xml_list))
		my_logger.info("Get SSM_ID for all matched message is Running")
		
		# Below query will get all the ssm_id for all the msg_id
		query = '''
						SELECT r.msg_id,bt.ssm_id FROM stp_own.rt_inbound_raw_message r, stp_own.rt_event e,
						stp_own.rt_block_trade bt
					  WHERE  r.msg_id = e.raw_msg_id
 					  AND e.tn = bt.tn
						'''
		dbobj.execute(query)
		self.cnt = dbobj.fetch_all()	

		#Create  dictionary of msg_id and ssm_id
		for item in self.cnt:
			if item:
				self.msg_ssm_dict[item[0]] = item[1]
		
		for k, v in msg_iden_dict.items():
			for mk, mv in self.msg_ssm_dict.items():
				if k == mk:
					my_logger.info("matched msg_id : %s" % k)
					my_logger.info("ssm_id for matched msg_id : %s" % mv)
					my_logger.info("Identifer for matched msg_id : %s" % v)
					self.identifier_list.append(v)
					self.ssm_idlist.append(mv)
					## Create a dictionary of ssm_id and identifer
					self.ssm_identifier_dict[mv] = v

		#pprint.pprint(self.ssm_identifier_dict)

		self.ssm_uniq_list = self.uniq(self.ssm_idlist)
		# To remove None value form the list
		self.ssm_uniq_list = [item for item in self.ssm_uniq_list if item is not None]

		self.identifier_uniq_list = self.uniq(self.identifier_list)
		# to remove None value from the list
		self.identifier_uniq_list = [x for x in self.identifier_uniq_list if x is not None]
		#print "length of ssm id list:", len(self.ssm_uniq_list)
		#print "LLL:", len(self.ssm_idlist), len(self.identifier_list)

		###To get cstc value for all the ssm_id from database
		my_logger.info("Get cstc value is in process")
		get_sublist = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]
		sublist = get_sublist(self.ssm_uniq_list,500)
		my_logger.info("SUBLIST: %s" % sublist)
		cstc_query_str = 'select ssm_id, comp_sec_type_code from taps_own.ssm_core where ssm_id in '
		query_cstc = ''
		for slist in sublist:
			slist_str = ''
			slist = [x for x in slist if x is not None]
			slist_str = '\',\''.join(slist)
			query_cstc = cstc_query_str + '(\'' + slist_str + '\')'
			#print "QQQ:",query_cstc
			my_logger.info("CSTC QUERY: %s" % query_cstc)
			try:
				dbobj.execute(query_cstc)
			except:
				pass
			self.cstc_value = dbobj.fetch_all()

			#Create a dictionary of ssm_id and cstc value
			for item in self.cstc_value:
				if item not in self.cstc_ssm_id:
					self.cstc_ssm_id[item[0]] = item[1]

		#pprint.pprint(self.msg_ssm_dict)
		#pprint.pprint(self.cstc_ssm_id)	

		#Create a list of ssm_id and cstc if ssm_id is equal in msg_ssm_dict add ssm_id and cstc value to the xml message list
		cstc_list = []
		for sk, sv in self.ssm_identifier_dict.items():
			for ck,cv in self.cstc_ssm_id.items():		
				if sk == ck:
					#print "MATCHED:", sk, ck
					add_str = [str(sv),str(sk),str(cv)]
					cstc_list.append(add_str)

		## Add ssm_id and cstc value at the end of each xml message
		for item in self.iden_xml_list:
			for cstc_val in cstc_list:
				if item[0] == cstc_val[0]:
					item.append(cstc_val[1])
					item.append(cstc_val[2])
					self.xml_msg_list.append(item)

		self.uniq_xml_msg_list = self.uniq(self.xml_msg_list)

		xml_file = open("/home/vgarg/pimco/scripts/input/fob_message_list.txt",'w')
		for i in self.uniq(self.uniq_xml_msg_list):
			xml_file.write(str(i) + "\n")
		
		for id in self.ssm_uniq_list:	
				uniq_ssm_obj.write(str(id)+"\n")
		uniq_ssm_obj.close()
		
		##Uniq ssm id list to upload into taps_own tables
		for item in self.ssm_uniq_list:
			self.upload_ssm_id_list.extend((item,))
	
		self.pmbcp(self.upload_ssm_id_list)
		print "Count after get ssm_id filter: %s"% len(self.ssm_uniq_list)
		self.summary_file_obj.write("No. of Uniq ssm_id found: %s" % len(self.ssm_uniq_list) + "\n")
		self.summary_file_obj.write("No. of ssm_id uploaded: %s" % len(self.upload_ssm_id_list) + "\n")

		#self.write_mq_message(self.identifier_uniq_list,self.iden_xml_list)
		self.write_mq_message(self.identifier_uniq_list,self.uniq_xml_msg_list)

		my_logger.info("Count after get ssm_id filter: %s" % len(self.ssm_uniq_list))
		my_logger.info("No. of ssm_id Uploaded: %s" % len(self.upload_ssm_id_list))
		my_logger.info("=================End Data Processing=================")

##Copied data from Prod database to Sandbox for indentifier listed in three table mentioned below
	def pmbcp(self,identifier_list):
		get_sublist = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]
		sublist = get_sublist(identifier_list,500 )
		tbl_list = [TAPS_SSM_CORE,TAPS_SSM_FIRMTOTAL,TAPS_SSM_OPTION]
		for slist in sublist:
			slist = [x for x in slist if x is not None]
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
	def validate(self,record_type, event, xml_message,msg_id):
	  #It will return matched data from pattern_list and passed pattern
		xmlMsgId = msg_id
		pattern_string = record_type + ' ' + event
		self.val_status = filter(lambda req_var : req_var == pattern_string,self.pattern_list) 
		if len(self.val_status) != 0:	 
			self.parser_identifier(xml_message,record_type, event,xmlMsgId) 

##Parse xml for the identifier and make input inbound_raw_message data for FOB
	def parser_identifier(self,xmlmsg,record_type,event,xmlmsg_id):
		msg_id = xmlmsg_id
		bm_mst_tkt_num = ''
		pm_mst_tkt_num = ''
		alloc_mst_tk_num = ''
		ticket_num = ''
		ticket_list = []
		try:
			self.dom = xml.dom.minidom.parseString(xmlmsg)
			identifier = self.dom.getElementsByTagName('IDENTIFIER')[0].firstChild.nodeValue.encode('ascii')
			#self.prv_ident_list.append(identifier) 
			#self.prv_msgId_list.append(msg_id)
			self.msg_identifier_dict[msg_id] = identifier
			ticket_num = self.dom.getElementsByTagName('TKTNUM')[0].firstChild.nodeValue.encode('ascii')
			rec_event_type = record_type + ' ' + event
			self.inbound_raw_msg = [identifier,ticket_num,rec_event_type,xmlmsg]
			self.iden_xml_list.append(self.inbound_raw_msg)
			my_logger.info('Parsing Identifier: ' + str(identifier))
			my_logger.info('Parsing Record Type and Event: ' + rec_event_type)

			# Put all BM CREATION ticket no. into a dictiory
			if record_type == 'BM': 
				if event == 'BM CREATION':
					self.before_bm_count += 1												
				elif event == 'BM CORRECT':		
					self.before_bm_correct_count += 1
				elif event == 'TRADE TICKET CREATE':
					self.before_trade_ticket_count += 1
				elif event == 'NEW MASTER TICKET':
					self.before_new_mst_tk_count += 1
				elif event == 'ALLOCATION SENT':
					self.before_alloc_sent_count += 1
				elif event == 'PRICE CHANGE':
					self.before_p_change_count += 1
				elif event == 'TRADE RELEASED':
					self.before_trade_rel_count += 1
				elif event == 'TRADE CONFIRMED':
					self.before_trade_conf_count += 1
				elif event == 'MASTER TICKET CREATE':
					self.before_mst_trade_tkt_count += 1
					'''
					bm_mst_tkt_num = ticket_num
					if bm_mst_tkt_num not in bm_dict:
						bm_dict[bm_mst_tkt_num] = ticket_num		
						my_logger.info('Parsing Master ticket: ' + str(bm_mst_tkt_num))
						my_logger.info('Parsing Ticket: ' + str(ticket_num))

				if event == 'TRADE RELEASED':
					bm_trade_rel_mst_tkt_num = ticket_num
					if bm_trade_rel_mst_tkt_num not in bm_dict:
						pass
				'''				
			# Put all PM Creation Master ticket no. and ticket no. into a dictionary
			if record_type == 'PM*' or record_type == 'PM':
				if event == 'PM CREATION':
					self.before_pm_count += 1
				elif event == 'TRADE TICKET CREATE':
					self.before_pm_trade_tkt_count += 1
				elif event == 'WI CONVERSION':
					self.before_pm_wi_conv_count += 1
				elif event == 'ACCEPT':
					self.before_pm_accept_count += 1
				elif event == 'ERROR':
					self.before_pm_error_count += 1
				elif event == 'REJECT':
					self.before_pm_reject_count += 1
				elif event == 'MANUAL':
					self.before_pm_manual_count += 1
				elif event == 'INQUIRY':
					self.before_pm_inquiry_count += 1
				elif event == 'INVENTOR':
					self.before_pm_inventor_count += 1
				elif event == 'PRESPLIT':
					self.before_pm_presplit_count += 1
				elif event == 'ASSET-SE':
					self.before_pm_assetse_count += 1
				elif event == 'SENT':
					self.before_pm_sent_count += 1
				elif event == 'PENDING':
					self.before_pm_pending_count += 1
				elif event == 'SAVED':
					self.before_pm_saved_count += 1
				elif event == 'NEW':
					self.before_pm_new_count += 1
				elif event == 'PM CORRECT':
					self.before_pm_correct_count += 1

				'''
				pm_mst_tkt_num = self.dom.getElementsByTagName('MTKTNUM')[0].firstChild.nodeValue.encode('ascii')
				if pm_mst_tkt_num not in pm_dict:
					pm_dict[pm_mst_tkt_num] = ticket_num
					my_logger.info('Parsing Master ticket: ' + str(pm_mst_tkt_num))
					my_logger.info('Parsing Ticket: ' + str(ticket_num))
				'''	
			#Put all ALLOCATION master ticket no. and ticket no. into a dictionary
			elif record_type == 'OMS' or record_type == 'OM3':
				match_var = ''
				if event == 'NEW ALLOCATION' or event == 'ALLOCATION':
					self.before_alloc_count += 1
					'''	
					alloc_mst_tk_num = self.dom.getElementsByTagName('MTKTNUM')[0].firstChild.nodeValue.encode('ascii')
					if alloc_mst_tk_num not in alloc_dict:
						alloc_dict[alloc_mst_tk_num] = ticket_num
						my_logger.info('Parsing Master ticket: ' + str(alloc_mst_tk_num))
						my_logger.info('Parsing Ticket: ' + str(ticket_num))
					'''
			#my_logger.info("BM CREATION Count after pattern match: %s"% self.bm_count)
			#my_logger.info("PM CREATION Count after pattern match: %s"% self.pm_count)
			#my_logger.info("ALLOCATION Count after pattern match: %s"% self.alloc_count)
		except IndexError:
			pass 

	# This method check parent child relation and creat a final dictionary
	def validate_ticket(self):
		#pprint.pprint(bm_dict)
		#pprint.pprint(pm_dict)
		#pprint.pprint(alloc_dict)
		
		# For BM CREATION record
		for k, v in bm_dict.items():
			final_dict[k] = v
		
		#for PM* PM CREATION record, if its master equals to BM ticket no.
		for pm_k,pm_v in pm_dict.items():
			for bm_k, bm_v in bm_dict.items():
				if pm_k == bm_k:
					#print "PM:", pm_k,bm_k, pm_v
					final_dict[pm_v] = pm_k

		# for ALLOCATION record, if its master equal to PM Ticket and PM master 
		# equal to BM ticket
		for al_k, al_v in alloc_dict.items():
			for pm_k, pm_v in pm_dict.items():
				if al_k == pm_v:
					#print "ALLOC:", al_k,pm_v,pm_k
					pmkey = pm_k
					for bm_t, bm_c in bm_dict.items():
						if pm_k == bm_t:
							#print "FFFF:",pm_k, bm_t, al_v,al_k
							final_dict[al_v] = al_k
						
		#pprint.pprint(final_dict)
						
		
##Parse xml message and make PM/BM creation pattern list
	def parser(self,doc):
		self.msg = doc[3]
		self.msgId = doc[0]
		try:
			self.dom = xml.dom.minidom.parseString(self.msg)
		except xml.parsers.expat.ExpatError:
			pass
		try:
			self.rec_type = self.dom.getElementsByTagName('RECORDTYPE')[0].firstChild.nodeValue.encode('ascii')
			self.event = self.dom.getElementsByTagName('EVENT')[0].firstChild.nodeValue.encode('ascii')
			#Pass PM/BM pettrn for validation to validate method
			self.validate(self.rec_type,self.event,self.msg,self.msgId)
		except IndexError:
			pass 

##Get the data from database for date passed as argument
	def get_dbrows(self):
		my_logger.info("=================Preparing Data starts====================")
	 #Database query to get all data for date passed in argument
		if self.date_flag:
			self.start_flag = False
			self.end_flag = False
			query_dbrow = 'select * from '+STP_INBOUND_RAW_MSG+' where to_char(RECEIVED_AT,\'DD-MON-YYYY\') = \'%s\' order by RECEIVED_AT asc' % self.day
			print query_dbrow
			#query_dbrow = 'select * from ' +STP_INBOUND_RAW_MSG+' where msg_id = 67086720'
			#print "QUERY:", query_dbrow
			#query_dbrow = 'select * from ' +STP_INBOUND_RAW_MSG+' where msg_id in (60747862,60747599,60747863,60747874,60747892,60747912)'
			my_logger.info('QUERY: ' + query_dbrow)
			my_logger.info('Date Flag: ' + str(self.date_flag) + ' and Date: ' + str(self.day))
		elif start_flag and end_flag:
			self.date_flag = False	
	 		query_dbrow = 'select * from '+STP_INBOUND_RAW_MSG+' where RECEIVED_AT BETWEEN to_timestamp(\'%s\',\'DD-MM-YYYY HH24:MI:SS:FF9\') and to_timestamp(\'%s\',\'DD-MM-YYYY HH24:MI:SS:FF9\')' % (self.start_date,self.end_date)
			print "QUERY: ",query_dbrow
			my_logger.info('QUERY: ' + query_dbrow)
			my_logger.info('Date Start Flag: ' + str(start_flag))
			my_logger.info('Date End Flag: ' + str(end_flag))
		dbobj.execute(query_dbrow)
	 	fetch_all_row = dbobj.fetch_all()
	 	print "starting data validation"
		my_logger.info('Starting data Parsing')

	 	#Fetch all data and pass one be one to parser method
		count = 0
	 	for self.row in fetch_all_row:
			self.all_msg.write(str(self.row[0]) + ' ' + str(self.row[3]) + "\n")
			self.parser(self.row)
			count += 1
			if count == 2000:
				break
		my_logger.info('Total no. of record fetch: ' + str(count))
		print "Total Message fetch from database: %s" % count
		self.summary_file_obj.write("Total Message get from Database: %s" % count + "\n")

		my_logger.info("End of data Parsing")
		#pprint.pprint(final_dict)
		#self.validate_ticket()
	 	#self.uniq_msgId_list = self.uniq(self.prv_msgId_list)  
	 	#self.get_ssm_id(self.uniq_identifier_list)
		print "Total no. of pattern match message: %s" % len(self.iden_xml_list)
		self.summary_file_obj.write("No. of message After Pattern Match: %s"% len(self.iden_xml_list
) + "\n")
		self.summary_file_obj.write("BM CREATION count after pattern match: %s" % self.before_bm_count + "\n")
		self.summary_file_obj.write("BM CORRECT count after pattern match: %s" % self.before_bm_correct_count + "\n")
		self.summary_file_obj.write("BM TRADE TICKET after pattern match: %s" % self.before_trade_ticket_count + "\n")
		self.summary_file_obj.write("BM NEW MASTER TICKET after pattern match: %s" % self.before_new_mst_tk_count + "\n")
		self.summary_file_obj.write("BM ALLOCATION SENT after pattern match: %s" % self.before_alloc_sent_count + "\n")
		self.summary_file_obj.write("BM PRICE CHANGE after pattern match: %s" % self.before_p_change_count + "\n")
		self.summary_file_obj.write("BM TRADE RELEASE after pattern match: %s" % self.before_trade_rel_count + "\n")
		self.summary_file_obj.write("BM TRADE CONFIRMED after pattern match: %s" % self.before_trade_conf_count + "\n")
		self.summary_file_obj.write("BM MASTER TICKET CREATE after pattern match: %s" % self.before_mst_trade_tkt_count + "\n")
		self.summary_file_obj.write("PM CREATION count after pattern match: %s" % self.before_pm_count + "\n")
		self.summary_file_obj.write("PM TRADE TICKET CREATE after pattern match: %s" % self.before_pm_trade_tkt_count + "\n")
		self.summary_file_obj.write("PM WI CONVERSION after pattern match: %s" % self.before_pm_wi_conv_count + "\n")
		self.summary_file_obj.write("PM ACCEPT after pattern match: %s" % self.before_pm_accept_count + "\n")
		self.summary_file_obj.write("PM ERROR after pattern match: %s" % self.before_pm_error_count + "\n")
		self.summary_file_obj.write("PM REJECT after pattern match: %s" % self.before_pm_reject_count + "\n")
		self.summary_file_obj.write("PM MANUAL after pattern match: %s" %  self.before_pm_manual_count + "\n")
		self.summary_file_obj.write("PM INQUIRY after pattern match: %s" %  self.before_pm_inquiry_count + "\n")
		self.summary_file_obj.write("PM INVENTOR after pattern match: %s" %  self.before_pm_inventor_count + "\n")
		self.summary_file_obj.write("PM PRESPLIT after pattern match: %s" % self.before_pm_presplit_count + "\n")
		self.summary_file_obj.write("PM ASSET SE after pattern match: %s" % self.before_pm_assetse_count + "\n")
		self.summary_file_obj.write("PM SENT after pattern match: %s" % self.before_pm_sent_count + "\n")
		self.summary_file_obj.write("PM PENDING after pattern match: %s" % self.before_pm_pending_count + "\n")
		self.summary_file_obj.write("PM SAVED after pattern match: %s" % self.before_pm_saved_count  + "\n")
		self.summary_file_obj.write("PM NEW after pattern match: %s" % self.before_pm_new_count  + "\n")
		self.summary_file_obj.write("PM PM CORRECT after pattern match: %s" % self.before_pm_correct_count + "\n") 

		self.summary_file_obj.write("ALLOCATION count after pattern match: %s" % self.before_alloc_count + "\n")

		my_logger.info("BM CREATION Count after pattern match: %s"% self.before_bm_count)
		my_logger.info("BM CORRECT count after pattern match: %s"%self.before_bm_correct_count)
		my_logger.info("BM TRADE TICKET after pattern match: %s" % self.before_trade_ticket_count)
		my_logger.info("BM NEW MASTER TICKET after pattern match: %s" % self.before_new_mst_tk_count)
		my_logger.info("BM ALLOCATION SENT after pattern match: %s" % self.before_alloc_sent_count)
		my_logger.info("BM PRICE CHANGE after pattern match: %s" % self.before_p_change_count)
		my_logger.info("BM TRADE RELEASE after pattern match: %s" % self.before_trade_rel_count)
		my_logger.info("BM TRADE CONFIRMED after pattern match: %s" % self.before_trade_conf_count)
		my_logger.info("BM MASTER TICKET CREATE after pattern match: %s" % self.before_mst_trade_tkt_count)

		my_logger.info("PM CREATION Count after pattern match: %s"% self.before_pm_count)
		my_logger.info("PM TRADE TICKET CREATE after pattern match: %s" % self.before_pm_trade_tkt_count)
		my_logger.info("PM WI CONVERSION after pattern match: %s" % self.before_pm_wi_conv_count)
		my_logger.info("PM ACCEPT after pattern match: %s" % self.before_pm_accept_count)
		my_logger.info("PM ERROR after pattern match: %s" % self.before_pm_error_count)
		my_logger.info("PM REJECT after pattern match: %s" % self.before_pm_reject_count)
		my_logger.info("PM MANUAL after pattern match: %s" % self.before_pm_manual_count)
		my_logger.info("PM INQUIRY after pattern match: %s" % self.before_pm_inquiry_count)
		my_logger.info("PM PRESPLIT after pattern match: %s" % self.before_pm_presplit_count)
		my_logger.info("PM ASSET SE after pattern match: %s" % self.before_pm_assetse_count)
		my_logger.info("PM SENT after pattern match: %s" % self.before_pm_sent_count)
		my_logger.info("PM PENDING after pattern match: %s" % self.before_pm_pending_count)
		my_logger.info("PM SAVED after pattern match: %s" %  self.before_pm_saved_count)
		my_logger.info("PM NEW after pattern match: %s" % self.before_pm_new_count)
		my_logger.info("PM PM CORRECT after pattern match: %s" %  self.before_pm_correct_count)
		my_logger.info("ALLOCATION Count after pattern match: %s" % self.before_alloc_count)

		self.get_ssm_id(self.msg_identifier_dict)
		self.xml_id.close()

	 	dbobj.disconnect()
		#self.summary_file_obj.close()
if __name__ == '__main__':
		main(sys.argv[1:])
		dbobj = pm_DbCon.oracle()
		obj = DB_operation() 
		obj.get_dbrows()
