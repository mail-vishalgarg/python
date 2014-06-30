import sys
import pmclientrc
import pm_DbCon
import commands
import re
import os,getopt
import xml.dom.minidom
from Mq_message import *
from datetime import datetime
from time import sleep
from xlwt import *
from fob_email import notify
import logging.config
import traceback
import pprint
from MQ_topic import *

send_from = 'vishal.garg@pimco.com'
send_to = 'vishal.garg@pimco.com'
subject = 'FOB Report Before and After'
text = 'FOB Report'


def Usage():
	print 'This script will validate data inserted in stp_own schema and compare'
	print 'attribute values in prod, qa and MQ topics.'
	print 'Usage: '+ scriptName + ' -d sand -f <Input File Name>'
	print '-d	Database name (e.g. sand)'
	print '-f	Input file name'
	print '-t to truncate tables'
	sys.exit()

def before(file):
	row = heading_row + 1
	if os.path.getsize(file):
		count = int(os.popen('wc -l %s'% file).read().split( )[0])
		sheet1.write(row,2,file)
		sheet1.write(row,3,count)
	return count

def xml_parsing(msg):
	data_value = {}
	try:
		dom = xml.dom.minidom.parseString(msg.encode("utf-8"))
	except xml.parsers.expat.ExpatError:
		pass

	try:
		audit_id_var = dom.getElementsByTagName('AUDITID')[0].firstChild.nodeValue
		ticket_var = dom.getElementsByTagName('TKTNUM')[0].firstChild.nodeValue
		user_var = dom.getElementsByTagName('USER')[0].firstChild.nodeValue
		qty_temp = dom.getElementsByTagName('QUANTITY')[0].firstChild.nodeValue
		## to convert unicode to str
		qty_var = qty_temp.encode('ascii', 'ignore')
		price_temp = dom.getElementsByTagName('PRICE')[0].firstChild.nodeValue
		price_var = price_temp.encode('ascii', 'ignore')
		ccy_var = dom.getElementsByTagName('CURRENCY')[0].firstChild.nodeValue
		record_type_var = dom.getElementsByTagName('RECORDTYPE')[0].firstChild.nodeValue
		event_var = dom.getElementsByTagName('EVENT')[0].firstChild.nodeValue
	except IndexError:
		pass

	data_value['audit_id'] = audit_id_var
	data_value['ticket_num'] = ticket_var
	data_value['user'] = user_var
	data_value['qty'] = qty_var
	data_value['price'] = price_var
	data_value['cur'] = ccy_var
	data_value['record_type'] = record_type_var
	data_value['event'] = event_var
	my_logger.info('xml_msg returns ' + audit_id_var + ' ' + ticket_var + ' ' + user_var + ' ' + qty_var + ' ' + price_var + ' ' + ccy_var + ' ' + record_type_var + ' ' + event_var)
	return data_value

def after(file,db):
	xml_value = {}
	fileObj = open(file, 'r')
	start_row = 1
	qty = ''
	table_name = ''
	for line in fileObj:
		sand_value={}
		match = re.search('(.*?)---(.*?)---(.*)',line)
		identifier = match.group(1)
		record_event_type= match.group(2)
		xml_msg = match.group(3)
		sheet2.write(start_row,0,identifier)
		sheet2.write(start_row,1,record_event_type)

		##Entry for sheet prod vs QA
		# All xml msg values stored in dictionary
		xml_value = xml_parsing(xml_msg)
		xml_value['ssmId'] = identifier
		xml_final_data[start_row] = xml_value
		pprint.pprint(xml_final_data)

		##put xml message to MQ using MQ object in main 
		mq_obj.put_msg(xml_msg)

		# call MQ_topic fuction to get topic values
		threadstart()
		sleep(2)
		my_logger.info('No. of message inject to FOB : ' + str(start_row))
		col_index = 2

		#put table count to sheet2 in Excelsheet
		for tab in table_list:
			rowcount = tableCount(tab)
			sheet2.write(start_row,col_index,rowcount)
			col_index += 1

		#Get MSG_ID from rt_raw_inbound_msg table
		if trunc_flag:
			msgId = msg_id(start_row)
		else:
			msgId = msg_id(raw_inbound_count + 1)
		sand_value['msgid'] = msgId
		
		# Get value of record_type, Event and TicketNo. from rt_event table
		record_type,event,tn = rt_event(msgId)
		sand_value['record_type'] = record_type
		sand_value['event'] = event
		sand_value['ticket'] = tn
		
		# Get the value of ssm_id, quantity, price and currency	from rt_block_trade and
		# rt_allocation and rt_distribution table
		if event == 'BM CREATION':
			ssm_id,qty,pimco_price,ccy,duration = getdata(tn,event)
			table_name = 'RT_BLOCK_TRADE'

		if event == 'ALLOCATION' or event == 'NEW ALLOCATION':
			ssm_id,qty,pimco_price,ccy,duration = getdata(tn,event)
			pimco_price = int(0)
			table_name = 'RT_ALLOCATION'
		elif event == 'PM CREATION':
			ssm_id,qty,pimco_price,ccy,duration = getdata(tn,event)
			table_name = 'RT_DISTRIBUTION'

		sand_value['table_name'] = table_name
		sand_value['qty'] = qty
		#print "QQQ:", sand_value['qty']
		sand_value['pimco_price'] = pimco_price
		sand_value['ssm_id'] = ssm_id
		sand_value['ccy'] = ccy
		sand_value['duration'] = duration 
			
		##	get comp_sec_type_code value from TAPS_OWN for all ssm_id
		cstc = comp_sec_type_code(ssm_id)
		sand_value['cstc'] = cstc
		sand_final_data[start_row] = sand_value
		pprint.pprint(sand_final_data)	
		pimco_price = int(0)
		ssm_id = ''
		table_name = ''
		start_row += 1

def trim(str):
	""" This function will trim any string"""
	string= re.sub(r'^\s*','',str)
	string= re.sub(r'$\s*','',string)
	return string

def before_db_count(table_list):
	row = 1
	for tab in table_list:
		truncate_tab(tab)
		rowcount = tableCount(tab)
		sheet1.write(row,0,tab)
		sheet1.write(row,1,rowcount)
		row += 1

def db_count(table_list):
	row = 1
	for tab in table_list:
		rowcount = tableCount(tab)
		sheet1.write(row,0,tab)
		sheet1.write(row,1,rowcount)
		row += 1
	return tableCount(table_list[0])
										
def truncate_tab(table_name):
	dbobj.execute('truncate table %s'% table_name)
	#dbobj.comit()

def tableCount(table_name):
	query = 'select count(1) from %s'% table_name
	rowcount = dbobj.fetch_one(query)
	return rowcount[0]

def msg_id(rownum):
	query = 'SELECT msg_id FROM ( SELECT msg_id, ROWNUM RN FROM stp_own.rt_inbound_raw_message) WHERE RN = ' + str(rownum)
	print "MSG: ",query
	msgId = dbobj.fetch_one(query)
	if msgId is not None:
		my_logger.info('msg_id() returns '+ str(msgId))
		return msgId[0]
	else:
		my_logger.info('msg_id() returns 0')
		return 0 
	
def rt_event(msgid):
	'''It will return record_type, event and ticket no. for a msg_id '''
	query = 'SELECT record_type, event, TN from STP_OWN.rt_event WHERE raw_msg_id = ' + str(msgid)
	row = dbobj.fetch_one(query)
	if row is not None:
		my_logger.info('rt_event() returns ' + str(row))
		return row
	else:
		my_logger.info('rt_event() returns NA, NA, 0')
		return ('NA','NA',0)

def getdata(tn,event):
	if event == 'BM CREATION':
		query = 'SELECT ssm_id,qty,pimco_price,ccy,duration from STP_OWN.rt_block_trade where tn = ' + str(tn)
	elif event == 'ALLOCATION' or event == 'NEW ALLOCATION':
		query = 'SELECT bt.ssm_id,a.qty, bt.pimco_price, bt.ccy, bt.duration  FROM stp_own.rt_allocation a, stp_own.rt_block_trade bt WHERE a.block_tn = bt.tn AND a.tn = %s' % tn
	elif event == 'PM CREATION':
		query = 'SELECT bt.ssm_id,d.qty, bt.pimco_price, bt.ccy, bt.duration FROM stp_own.rt_distribution d, stp_own.rt_block_trade bt WHERE d.block_tn = bt.tn AND d.tn = %s' % str(tn)
	#print query
	row = dbobj.fetch_one(query)
	if row is not None:
		my_logger.info('Event: ' + event + ': getdata() returns ' + str(row))
		return row
	else:
		my_logger.info('Event: ' + event + ': getdata() returns (None,0,0,None,None)')
		return (None,0,0,None,None)

def comp_sec_type_code(ssm_id):
	query = "SELECT comp_sec_type_code from TAPS_OWN.ssm_core where ssm_id = '%s'" % ssm_id
	row = dbobj.fetch_one(query)
	if row is not None:
		return row[0]
	else:
		return None

def report_format(count):
	for row in range(1,count+1):
		if xml_final_data[row]['ssmId'] == sand_final_data[row]['ssm_id']:
			sheet3.write(row+1,0,xml_final_data[row]['ssmId'])
			sheet3.write(row+1,11,sand_final_data[row]['ssm_id'])
		else:
			sheet3.write(row+1,0,xml_final_data[row]['ssmId'])
			sheet3.write(row+1,11,sand_final_data[row]['ssm_id'],style2)

		if xml_final_data[row]['record_type'] == sand_final_data[row]['record_type']:
			sheet3.write(row+1,1,xml_final_data[row]['record_type'])
			sheet3.write(row+1,12,sand_final_data[row]['record_type'])
		else:
			sheet3.write(row+1,1,xml_final_data[row]['record_type'])
			sheet3.write(row+1,12,sand_final_data[row]['record_type'],style2)

		if xml_final_data[row]['event'] == sand_final_data[row]['event']:
			sheet3.write(row+1,2,xml_final_data[row]['event'])
			sheet3.write(row+1,13,sand_final_data[row]['event'])
		else:
			sheet3.write(row+1,2,xml_final_data[row]['event'])
			sheet3.write(row+1,13,sand_final_data[row]['event'],style2)

		if str(xml_final_data[row]['ticket_num']) == str(sand_final_data[row]['ticket']):
			sheet3.write(row+1,3,xml_final_data[row]['ticket_num'])
			sheet3.write(row+1,14,sand_final_data[row]['ticket'])
		else:
			sheet3.write(row+1,3,xml_final_data[row]['ticket_num'])
			sheet3.write(row+1,14,sand_final_data[row]['ticket'],style2)
	
		sheet3.write(row+1,4,xml_final_data[row]['audit_id'])

		sheet3.write(row+1,5,xml_final_data[row]['user'])
		sheet3.write(row+1,6,xml_final_data[row]['qty'])
		sheet3.write(row+1,7,factor)
		if not sand_final_data[row]['qty']:
			sand_final_data[row]['qty'] = int(0)
			
		if float(xml_final_data[row]['qty']) * factor == float(sand_final_data[row]['qty']):
			sheet3.write(row+1,15,sand_final_data[row]['qty'])
		else:
			sheet3.write(row+1,15,sand_final_data[row]['qty'],style2)
		if float(trim(str(xml_final_data[row]['price']))) == float(trim(str(sand_final_data[row]['pimco_price']))):
			sheet3.write(row+1,8,xml_final_data[row]['price'])
			sheet3.write(row+1,16,sand_final_data[row]['pimco_price'])
		else:
			sheet3.write(row+1,8,xml_final_data[row]['price'])
			sheet3.write(row+1,16,sand_final_data[row]['pimco_price'],style2)

		if str(xml_final_data[row]['cur']) == str(sand_final_data[row]['ccy']):
			sheet3.write(row+1,9,xml_final_data[row]['cur'])
			sheet3.write(row+1,17,sand_final_data[row]['ccy'])
		else:
			sheet3.write(row+1,9,xml_final_data[row]['cur'])
			sheet3.write(row+1,17,sand_final_data[row]['ccy'],style2)

		sheet3.write(row+1,10,sand_final_data[row]['msgid'])
		sheet3.write(row+1,18,sand_final_data[row]['duration'])
		sheet3.write(row+1,19,sand_final_data[row]['cstc'])
		sheet3.write(row+1,20,sand_final_data[row]['table_name'])
				
def read_topic_file(file,count):
	topicFile = open(file, 'r')
	result = []
	ntopic = 5
	nlines = 0
	row = 1
	for line in topicFile:
		line = re.sub(r'\n$','',line)
		result.append(line)
		nlines += 1
		if nlines >= ntopic:
			col_index = 21
			for i in range(len(result)):
				topic_name,topic_value = result[i].split('|')
				if topic_name == 'existing':
						if topic_value == 'None':
							sheet3.write(row+1,21,'N')
						else:
							sheet3.write(row+1,21,'Y',style4)
				if topic_name == 'new_security': 
					if topic_value == 'None':
						sheet3.write(row+1,22,'N')
					else:
						sheet3.write(row+1,22,'Y',style4)
				if topic_name == 'ticket_topic':
					if topic_value == 'None':
						sheet3.write(row+1,23,'N')
					else:
						sheet3.write(row+1,23,'Y',style4)
				if topic_name == 'collat':
					if topic_value == 'None':
						sheet3.write(row+1,24,'N')
					else:
						sheet3.write(row+1,24,'Y',style4)
				if topic_name == 'order':
					if topic_value == 'None':
						sheet3.write(row+1,25,'N')
					else:
						sheet3.write(row+1,25,'Y',style4)
			row += 1
			nlines = 0
			result = []
			
def remove_file(filename):
	with open(os.path.expanduser(filename)) as existing_file:
		existing_file.close()
		os.remove(os.path.expanduser(filename))

def main():
	global scriptName, table_list,scriptbase_name,mq_obj
	global sheet1, sheet2, sheet3,font1, style1,style2,style4,style3
	global mainInputFile,heading_row,dbtype,trunc_flag
	global dbobj, conn ,my_logger 
	global sand_final_data, xml_final_data, factor,raw_inbound_count
	
	
	sand_final_data = {}
	xml_final_data = {}
	factor = int(1000)
	raw_inbound_count = 0
	
	table_list = ['STP_OWN.RT_INBOUND_RAW_MESSAGE','STP_OWN.RT_SECURITY','STP_OWN.RT_BLOCK_TRADE','STP_OWN.RT_ALLOCATION','STP_OWN.RT_DISTRIBUTION','STP_OWN.RT_ORDER','STP_OWN.RT_COLLATERAL','STP_OWN.RT_ORDER_ALLOC']
	scriptName = os.path.basename(__file__)
	scriptbase_name = scriptName.split('.')[0]
	dbtype = ''
	inputfile_name = ''
	heading_row = 0
	trunc_flag = 0
		
	## Logging in mq_report.log file
	logging.config.fileConfig('fob_logging.conf')
	my_logger = logging.getLogger(' ')

	my_logger.info("=================Test session starts====================")
	## Remove mq_topic.log file each time before processing
	try:
		remove_file('/home/vgarg/pimco/scripts/mq_topic.log')
	except:	
		my_logger.error('', exc_info=1)

	##Connect with MQ
	mq_obj = mq_message()
	mq_obj.mq_connect()

	##Object to make Excel file
	book = Workbook(encoding="utf-8")
	font1 = Font()
	font1.bold = True

	##bold style of text
	style1 = XFStyle()
	style1.font = font1
	style2 = easyxf('pattern: pattern solid,fore_colour red;font: color white;align: horiz center')	
	style3 = easyxf('pattern: pattern solid,fore_colour yellow;font: color black;align: horiz center')
	
	style4 = easyxf('pattern: pattern solid,fore_colour green;font: color white;align: horiz center')
	sheet1 = book.add_sheet("Before",cell_overwrite_ok=True)
	sheet2 = book.add_sheet("After", cell_overwrite_ok=True)
	sheet3 = book.add_sheet("Prod vs QA", cell_overwrite_ok=True)
	
	if len(sys.argv[1:]) < 4:
		#print 'Please provide valid options!!'+"\n"
		my_logger.error('Please provide valid options!!')
		Usage()
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'htd:f:')
	except getopt.GetoptError as err:
		#print err
		my_logger.error('',exc_info=1)
		Usage()
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			usage()
		elif opt in ("-d", "--db"):
			if arg != 'sand':
				#print "Specified database is not correct, Please check it"
				my_logger.error("Specified database is not correct, Please check it")
				Usage()
			else:
				dbtype = arg
		elif opt in ("-f", "--file"):
			if arg == '':
				#print "Please enter input file name!!"
				my_logger.error("Please enter input file name!!")
				Usage()
			else:
				mainInputFile = arg
				if 	os.path.isfile(mainInputFile):
					my_logger.info('%s file exist' % mainInputFile)
					#print '%s file exist' % mainInputFile
				else:
					my_logger.error('File does not exist : %s'% mainInputFile)
					sys.exit(1)
					#print 'File does not exist : %s'% mainInputFile
		elif opt in ("-t", "--trunc"):
			trunc_flag = 1
		else:
			#print "Please check input arguments!!\n"
			my_logger.error('Please check input arguments!!')
			Usage()

	# Database connectivity
	dbobj = pm_DbCon.oracle()
	conn = dbobj.connect(dbtype)

	##Write Excel sheet1 Heading
	sheet1.write(heading_row,0,"Table Name",style1)
	sheet1.write(heading_row,1,"No. of Records",style1)
	sheet1.write(heading_row,2,"InputFileName", style1)
	sheet1.write(heading_row,3,'No. of Records', style1)
	

	##Write Excel sheet2 Heading
	sheet2.write(heading_row,0,'ssm_id',style4)
	sheet2.write(heading_row,1,'Record & EventType',style4)
	col = 2
	for tablename in table_list:
		tablename = re.sub(r'STP_OWN.','',tablename)
		sheet2.write(0,col,tablename,style4)
		col += 1
	sheet3.write_merge(heading_row, heading_row,0,9,'Prod Reference Message',style2)
	sheet3.write(heading_row+1,0,'SSM_ID',style1)
	sheet3.write(heading_row+1,1,'RECORD_TYPE',style1)
	sheet3.write(heading_row+1,2,'EVENT',style1)
	sheet3.write(heading_row+1,3,'TICKET NO.', style1)
	sheet3.write(heading_row+1,4,'AUDITID',style1)
	sheet3.write(heading_row+1,5,'USER',style1)
	sheet3.write(heading_row+1,6,'QUANTITY',style1)
	sheet3.write(heading_row+1,7,'FACTOR',style1)
	sheet3.write(heading_row+1,8,'PRICE',style1)
	sheet3.write(heading_row+1,9,'CURRENCY',style1)
	# Entry for QA in 3rd worksheet
	sheet3.write_merge(heading_row, heading_row,10,20,'SANDBOX Database',style3)
	sheet3.write(heading_row+1,10,'MSG ID',style1)
	sheet3.write(heading_row+1,11,'SSM_ID',style1)
	sheet3.write(heading_row+1,12,'RECORD_TYPE',style1)
	sheet3.write(heading_row+1,13,'EVENT',style1)
	sheet3.write(heading_row+1,14,'TICKET NO.',style1)
	sheet3.write(heading_row+1,15,'QUANTITY', style1)
	sheet3.write(heading_row+1,16,'PRICE', style1)
	sheet3.write(heading_row+1,17,'CURRENCY',style1)
	sheet3.write(heading_row+1,18,'DURATION',style1)
	sheet3.write(heading_row+1,19,'COMP_SEC_TYPE_CODE',style1)
	sheet3.write(heading_row+1,20,'TABLE NAME',style1)
	sheet3.write_merge(heading_row, heading_row,21,25,'MQ TOPICS',style4)
	sheet3.write(heading_row+1,21,'RT.EXISTING_SECURITY.QA',style1)
	sheet3.write(heading_row+1,22,'RT.NEW_SECURITY.QA',style1)
	sheet3.write(heading_row+1,23,'RT.TICKET.QA.TOPIC',style1)
	sheet3.write(heading_row+1,24,'RT.COLLAT',style1)
	sheet3.write(heading_row+1,25,'RT.ORDER',style1)	
		

	if trunc_flag:
		before_db_count(table_list)
	else:
		raw_inbound_count = db_count(table_list)
	
	total_count = before(mainInputFile)
	after(mainInputFile,dbtype)
	report_format(total_count)
	read_topic_file('mq_topic.log',total_count)
	book.save("Mq_report_final.xls")
	notify(send_from, send_to, subject,text,files=['Mq_report_final.xls'])
	mq_obj.mq_disconnect()
	my_logger.info("====================Test session end===================")
if __name__ == '__main__':
	try:
		main()
	except:
		my_logger.error('',exc_info=1)
