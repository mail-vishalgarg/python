import sys
from pmclientrc import *
import pm_DbCon
import commands
import re
import os,getopt,shutil
import xml.dom.minidom
from Mq_message import *
from datetime import datetime
from time import sleep
from xlsxwriter import *
from fob_email import notify
import logging.config
import traceback
import pprint
from MQ_topic import *
from ExcelFormat import *
from fob_xml_parser import *
#from fob_comparison import *

#subject = 'FOB Report Before and After'
#text = 'FOB Report'

logging.config.fileConfig(fobLoggingFile)
my_logger = logging.getLogger(' ')


def before(file,heading_row,sheet2):
	row = heading_row + 1
	if os.path.getsize(file):
		count = int(os.popen('wc -l %s'% file).read().split( )[0])
		sheet2.write(row,2,file)
		sheet2.write(row,3,count)
	return count

def xml_data(file):
	start_row = 1
	xml_final_data = {}
	fileobj = open(file,'r')
	for line in fileobj:
		match = re.search('(.*?)---(.*?)---(.*?)---(.*?)---(.*?)---(.*)',line)
		identifier = match.group(1)
		xml_msg = match.group(4)
		xml_ssm_id = match.group(5)
		xml_cstc_val = match.group(6)
		xml_value = xml_parse(xml_field_list,xml_msg)
		xml_value['xml_identifier'] = identifier
		xml_value['xml_ssm_id'] = xml_ssm_id
		xml_value['xml_cstc'] = xml_cstc_val
		xml_final_data[start_row] = xml_value
		start_row += 1
		#pprint.pprint(xml_final_data)
	return xml_final_data
				

def put_mq_message(mq_obj,file,dbobj,table_list):
	start_row = 1
	tableCountData = []
	msg_file = open(file,'r')
	for msg in msg_file:
		sheet3data = []
		match = re.search('(.*?)---(.*?)---(.*?)---(.*?)---(.*?)---(.*)',msg)
		xml_msg = match.group(4)
		identifier = match.group(1)
		record_event_type= match.group(3)
		
		#Push sheet3 data into a list	
		sheet3data.append(identifier)
		sheet3data.append(record_event_type)
		mq_obj.put_msg(xml_msg)
		sleep(.5)
		my_logger.info('No. of message inject to FOB : ' + str(start_row))

		for tab in table_list:
			rowcount = tableCount(tab,dbobj)
			sheet3data.append(rowcount)
		tableCountData.append(sheet3data)
		start_row += 1	
	return tableCountData

def after(xml_final_data,dbobj,*args):
	sand_final_data = {}
	ticket_count_dict = {}
	event_ssm_dict = {}
	cstc_event_list = []
	newSecFlag = ''
	uniq_ssm_list = []
	for ar in args:
		newSecFlag = ar

	row = 1
	while row <= len(xml_final_data):
		price_pimco = int(0)
		trade_price = int(0)
		ssm_id = ''
		qty = ''
		table_name = ''
		ccy = ''
		duration = ''
		msgId = ''
		record_type = ''
		event = ''
		tn = ''
		cstc = ''
		if newSecFlag:
			notified_as = ''

		sand_value={}
		my_logger.debug('No. of record Processed from database: %s' % row)

		msgId,ssm_id,qty,trade_price,price_pimco,ccy,duration,record_type,event,tn = getDbValue(dbobj,xml_final_data[row]['tktnum'],xml_final_data[row]['event'],xml_final_data[row]['recordtype'])
		sleep(1)
		sand_value['db_msgid'] = msgId
		sand_value['db_recordtype'] = record_type
		sand_value['db_event'] = event
		sand_value['db_ticket'] = tn
		ticket_count_dict[row] = tn

		if record_type == 'BM':
			table_name = 'RT_BLOCK_TRADE'
		elif record_type == 'PM' or record_type == 'PM*':
			table_name = 'RT_ALLOCATION'
		elif event == 'ALLOCATION' or event == 'NEW ALLOCATION':
			table_name = 'RT_DISTRIBUTION'

		sand_value['db_table_name'] = table_name
		sand_value['db_qty'] = qty
		sand_value['db_tr_price'] = trade_price
		sand_value['db_ssm_id'] = ssm_id
		sand_value['db_ccy'] = ccy
		sand_value['db_duration'] = duration
		if newSecFlag:
			if ssm_id not in uniq_ssm_list:
				if event == 'BM CREATION' or event == 'TRADE TICKET CREATE' \
																	or event == 'MASTER TICKET CREATE' \
																	or event == 'NEW MASTER TICKET':
					uniq_ssm_list.append(ssm_id)
					if ssm_id:
						notified_as = rt_security_val(ssm_id,dbobj)
						sand_value['db_notified_as'] = notified_as
					else:
						sand_value['db_notified_as'] = 'None'
			else:
				sand_value['db_notified_as'] = 'None'		
		##	get comp_sec_type_code value from TAPS_OWN for all ssm_id
		if ssm_id:
			cstc = comp_sec_type_code(ssm_id,dbobj)
			if cstc:
				sand_value['db_cstc'] = cstc
			else:
				sand_value['db_cstc'] = 'NA'
		else:
			sand_value['db_cstc'] = 'NA'
		sand_final_data[row] = sand_value

		if ssm_id:	
			topic_existing_string = str(ssm_id) + '|' + event + '|' + str(trade_price)+ '|' + str(price_pimco)
			event_ssm_dict[row] = topic_existing_string
		
		## For QA Summary sheet table
		if cstc:
			if event == 'NEW ALLOCATION':
				event = 'ALLOCATION'
				cstc_event_str = cstc + '|' + event
			else:
				cstc_event_str = cstc + '|' + record_type + ' ' + event
			cstc_event_list.append(cstc_event_str)
		row += 1
		sleep(.2)
	return (sand_final_data,ticket_count_dict,event_ssm_dict,cstc_event_list)	

def trim(str):
	""" This function will trim any string"""
	string= re.sub(r'^\s*','',str)
	string= re.sub(r'$\s*','',string)
	return string

def getDbValue(dbobj,ticket_num,event, record_type):
	query_str = ''
	query = '''
					 SELECT ad.recordtype, ad.query
					 FROM stp_own.app_data ad, stp_own.app_master am
					 WHERE ad.app_id = am.id
					 and am.id = '1' and am.active = 'Y' 
					'''
	dbobj.execute(query)
	fetch_all_row = dbobj.fetch_all()
	for row in fetch_all_row:
		if record_type == row[0]:
			query_str = row[1]
		
		if row[0] == 'ALLOCATION' or row[0] == 'NEW ALLOCATION':
			if row[0] == event:
				query_str = row[1]	
	query = query_str + '=' + ticket_num + ' and e.event = \'' + event + '\''
	#print "QQQ:", query
	row = dbobj.fetch_one(query)
	if row is not None:
		my_logger.debug('getDbValue() returns ' + str(row))
		return row
	else:
		my_logger.debug('getDbValue() returns (0,None,0,0,0,None,None,None,None,0)')
		return (0,'NA',0,0,0,'NA','NA','NA','NA',0)
		

def before_db_count(table_list,dbobj,sheet2):
	row = 1
	for tab in table_list:
		truncate_tab(tab,dbobj)
		rowcount = tableCount(tab,dbobj)
		tabname = re.sub(r'(.*?)\.(.*)',r'\2',tab)
		sheet2.write(row,0,tabname)
		sheet2.write(row,1,rowcount)
		row += 1

def db_count(table_list,dbobj,sheet2):
	row = 1
	for tab in table_list:
		rowcount = tableCount(tab,dbobj)
		sheet2.write(row,0,tab)
		sheet2.write(row,1,rowcount)
		row += 1
	return tableCount(table_list[0],dbobj)
										
def truncate_tab(table_name,dbobj):
	dbobj.execute('truncate table %s'% table_name)
	my_logger.info("Truncate table: " + table_name)
	#dbobj.comit()

def tableCount(table_name,dbobj):
	query = 'select count(1) from %s'% table_name
	rowcount = dbobj.fetch_one(query)
	my_logger.info("RowCount in  table " + table_name +":"+ str(rowcount))
	return rowcount[0]

def comp_sec_type_code(ssm_id,dbobj):
	query_str = ''
	query = '''
           SELECT ad.recordtype, ad.query
           FROM stp_own.app_data ad, stp_own.app_master am
           WHERE ad.app_id = am.id
           and am.id = '1' and am.active = 'Y'
					 and recordtype = 'cstc'
          '''
	dbobj.execute(query)
	fetch_row = dbobj.fetch_all()
	for val in fetch_row:
		query_str = val[1] + " = '" + ssm_id + "'"
		#query = datadict['cstc'] + " = '" + ssm_id + "'"
	row = dbobj.fetch_one(query_str)
	if row is not None:
		return row[0]
		my_logger.info('comp_sec_type_code() returns %s' % row[0])
	else:
		return None
		my_logger.info('comp_sec_type_code() returns None')

def get_heading(dbobj):
	heading_dict = {}
	index_dict = {}
	query_heading = '''
                  SELECT ah.workbook_name, ah.heading,ah.heading_index
                  FROM stp_own.app_heading ah, stp_own.app_master am
                  WHERE ah.app_id = am.id
                  AND am.app_name = 'FOB'
                  '''
	dbobj.execute(query_heading)
	row = dbobj.fetch_all()
	for item in row:
		heading_dict[item[0]] = item[1].split(',')
		index_dict[item[0]] = item[2]
	return heading_dict,index_dict
	
def put_headings(excel,sheet1,sheet2,sheet3,sheet4,heading_dict,index_dict,heading_format,table_list,merge_style_list,bold,*args):
	arg_list = []
	for ar in args:
		arg_list.append(ar)
	lenOfarg_list = len(arg_list)
	if lenOfarg_list == 1:
		newSecFlag = arg_list[0]
	else:
		newSecFlag = arg_list[0]
		sheet5 = arg_list[1]

	##Sheet2 Headings
	excel.addHeading(sheet2,index_dict['sheet2'],heading_dict['sheet2'],heading_format)
	excel.addHeading(sheet2,index_dict['sheet2_1'],heading_dict['sheet2_1'],heading_format)
	sheet2.set_column(reportdict['sheet2_set_col'],reportdict['sheet2_col_width'])
		
	##Write Excel sheet3 Headings
	for tablename in table_list:
		tablename = re.sub(r'(.*?)\.(.*)',r'\2',tablename)
		heading_dict['sheet3'].append(tablename)
	excel.addHeading(sheet3,index_dict['sheet3'],heading_dict['sheet3'],heading_format)
	
	#Write Excel sheet4 Headings
	if newSecFlag:
		heading_sheet4 = heading_dict['sheet4_newsecflag']
	else:
		heading_sheet4 = heading_dict['sheet4']

	for key,value in mergedict.items():
		if key == 'sheet4':
			i = 0
			for col,heading in value.items():
				excel.merge(sheet4,col,heading,merge_style_list[i])
				i += 1
		if key == 'sheet1':
			for col, heading in value.items():
				excel.merge(sheet1,col,heading,merge_style_list[3])

	excel.addHeading(sheet4,index_dict['sheet4'],heading_sheet4,heading_format)	

	#Headings for Summary sheet1
	#excel.merge(sheet1,'B23:T23','Intraday Trade Summary',merge_style_list[3])
	excel.addHeading(sheet1,index_dict['sheet1_11'],heading_dict['sheet1_11'],heading_format)
	excel.addHeading(sheet1,index_dict['sheet1_10'],heading_dict['sheet1_10'],heading_format)

	sheet1.set_column('B:C',15)
	sheet1.add_table('B28:C30',{'header_row': 0})
	excel.addHeading(sheet1,index_dict['sheet1_1'],heading_dict['sheet1_1'],heading_format)
	excel.addHeading(sheet1,index_dict['sheet1'],heading_dict['sheet1'],heading_format)
	excel.addHeading(sheet1,index_dict['sheet1_2'],heading_dict['sheet1_2'])
	excel.addHeading(sheet1,index_dict['sheet1_3'],heading_dict['sheet1_3'])
	excel.addHeading(sheet1,index_dict['sheet1_4'],heading_dict['sheet1_4'],bold)

	sheet1.set_column('E:F',15)
	sheet1.add_table('E28:F30',{'header_row': 0})
	excel.addHeading(sheet1,index_dict['sheet1_5'],heading_dict['sheet1_5'],heading_format)
	excel.addHeading(sheet1,index_dict['sheet1_6'],heading_dict['sheet1_6'],heading_format)
	excel.addHeading(sheet1,index_dict['sheet1_7'],heading_dict['sheet1_7'])
	excel.addHeading(sheet1,index_dict['sheet1_8'],heading_dict['sheet1_8'])
	excel.addHeading(sheet1,index_dict['sheet1_9'],heading_dict['sheet1_9'],bold)
	
	#Headings for sheet5
	if newSecFlag:
		for key,val in mergedict.items():
			if key == 'sheet5':
				i = 0
				for col, heading in val.items():
					excel.merge(sheet5,col,heading,merge_style_list[i])
					i += 1
					if i >3:
						i = 1
		#excel.addHeading(sheet5,index_dict['sheet5'],reportdict['sheet5'],heading_format)
		excel.addHeading(sheet5,index_dict['sheet5'],heading_dict['sheet5'],heading_format)

def report_sheet3(tableCountData,sheet3):
	start_row = 1
	table_range = 'A1:' +str(chr(65 + len(tableCountData[0]) - 1)) + str(len(tableCountData)+1)
	sheet3.add_table(table_range,{'header_row':0})
	sheet3.set_column(table_range,18)
	for i in range(len(tableCountData)):
		sheet3.write(start_row,0,tableCountData[i][0])
		sheet3.write(start_row,1,tableCountData[i][1])
		sheet3.write(start_row,2,tableCountData[i][2])
		sheet3.write(start_row,3,tableCountData[i][3])
		sheet3.write(start_row,4,tableCountData[i][4])
		sheet3.write(start_row,5,tableCountData[i][5])
		sheet3.write(start_row,6,tableCountData[i][6])
		sheet3.write(start_row,7,tableCountData[i][7])
		sheet3.write(start_row,8,tableCountData[i][8])
		sheet3.write(start_row,9,tableCountData[i][9])
		start_row += 1	
	

def column_name(dbobj,sheetname):
	dict = {}
	query = "SELECT ap.field_name,ap.position_col FROM stp_own.app_field_position ap, stp_own.app_master am where ap.app_id = am.id and am.app_name = 'FOB' and ap.workbook_name='%s'" % sheetname
	dbobj.execute(query)
	fetch_row = dbobj.fetch_all()
	for item in fetch_row:
		dict[item[0]] = item[1]
		
	return dict
	
def compare_field(dbobj,sheetname):
	compare = {}
	query = "SELECT afm.field, afm.mapfield FROM stp_own.app_field_mapping afm, stp_own.app_master am where am.id = afm.id and am.app_name = 'FOB' and afm.mapflag = 'Y' and afm.workbook_name = '%s' " % sheetname
	dbobj.execute(query)
	fetch_row = dbobj.fetch_all()
	for item in fetch_row:
		compare[item[0]] = item[1]
	
	return compare
						
def noncompare_field(dbobj,sheetname):
	noncompare =[]
	query = "SELECT afm.field from stp_own.app_field_mapping afm, stp_own.app_master am where am.id = afm.id and am.app_name = 'FOB' and afm.newsecflag = 'N' and afm.workbook_name = '%s' "% sheetname

	dbobj.execute(query)
	row = dbobj.fetch_all()
	for item in row:
		noncompare.append(item[0])
	return noncompare

def newsec_value(dbobj,sheetname):
	query = "SELECT afm.field from stp_own.app_field_mapping afm, stp_own.app_master am where am.id = afm.id and am.app_name = 'FOB' and afm.newsecflag = 'Y' and afm.workbook_name = '%s'" % sheetname
	dbobj.execute(query)
	row = dbobj.fetch_all()
	return row[0][0]	
	

def report_format(count,sheet4,sheet4_col_name,xml_final_data,sand_final_data,redcolor,graycolor,compare,noncompare,dbobj,*args):
	for ar in args:
		newSecFlag = ar
	for row in range(1,count+1):
		sheet4.write(row+1,sheet4_col_name['xml_identifier'],xml_final_data[row]['xml_identifier'])
		
		for key,value in compare.items():
			if str(xml_final_data[row][key]) == str(sand_final_data[row][value]):
				sheet4.write(row+1,sheet4_col_name[key],xml_final_data[row][key])
				sheet4.write(row+1,sheet4_col_name[value],sand_final_data[row][value])
			else:
				sheet4.write(row+1,sheet4_col_name[key],xml_final_data[row][key])
				sheet4.write(row+1,sheet4_col_name[value],sand_final_data[row][value],redcolor)

		for item in noncompare:
			if item in sand_final_data[row].keys():
				sheet4.write(row+1,sheet4_col_name[item],sand_final_data[row][item])
			else:
				sheet4.write(row+1,sheet4_col_name[item],xml_final_data[row][item])

		if newSecFlag:
			newsecfield = newsec_value(dbobj,'sheet4')
			sheet4.write(row+1,sheet4_col_name[newsecfield],sand_final_data[row][newsecfield])

def read_topic_file(existing_file,new_sec_file,ticket_file,order_file,collat_file):
	existFile = open(existing_file,'r')
	newSecFile = open(new_sec_file,'r')
	ticketFile = open(ticket_file,'r')
	orderFile = open(order_file,'r')
	collatFile = open(collat_file,'r')
	ticket_topic_dict = {}
	existing_topic_dict = {}
	new_sec_topic_dict = {}
	order_topic_dict = {}
	collat_topic_dict = {}
	
	for line in ticketFile:
		#To process other than blank lines
		if not line.isspace():
			line = re.sub(r'\n$','',line)
			topic_name, topic_value = line.split('---')
			if topic_name == 'ticket_topic':
				match = re.search('{(.*?)ticket(.*?)tn":(.*?),"(.*)',topic_value)
				if match:
					tk_val = match.group(2)
					tn_val = match.group(3)
					#print "TTTT:", tk_val
				tk_match = re.search('{\"com.pimco.stp.fob.message.avro.ticket.(.*?)\":{\"td(.*)',tk_val)
				if tk_match:
					ticket_val = tk_match.group(1)
				ticket_topic_dict[tn_val] = ticket_val
	sleep(3)
	for line in existFile:
		if not line.isspace():
			line = re.sub(r'\n$','',line)
			topic_name, topic_value = line.split('---')
			if topic_name == 'existing':
				exist_match = re.search('(.*?)ssmId":"(.*?)"(.*)',topic_value)
				match_price_and_value = re.search('(.*?)value":(.*?),"(.*?)price":{"value":(.*?),"type(.*)',topic_value)
				ssmid_val = exist_match.group(2)
				exist_quoteprice = match_price_and_value.group(2)
				exist_price = match_price_and_value.group(4)
				exist_str = str(exist_quoteprice) + '|' + str(exist_price)
				existing_topic_dict[ssmid_val] = exist_str
	#pprint.pprint(existing_topic_dict)
	sleep(3)
	for line in newSecFile:
		if not line.isspace():
			line = re.sub(r'\n$','',line)
			topic_name, topic_value = line.split('---')
			if topic_name == 'new_security':
				new_sec_match = re.search('(.*?)ssmId":"(.*?)"(.*)',topic_value)
				#match_price_and_val = re.search('(.*?)value":(.*?),"(.*?)price":{"value":(.*?),"type (.*)',topic_value)
				match_price_and_val = re.search('(.*?)value":(.*?),"type(.*?)value":(.*?),"type"(.*)',topic_value)
				new_ssm_id_val = new_sec_match.group(2)
				new_sec_quoteprice = match_price_and_val.group(2)
				new_sec_price = match_price_and_val.group(4)
				new_sec_str = str(new_sec_quoteprice) + '|' + str(new_sec_price)
				new_sec_topic_dict[new_ssm_id_val] = new_sec_str

	for line in orderFile:
		if not line.isspace():
			line = re.sub(r'\n$','',line)
			topic_name, topic_value = line.split('---')
			if topic_name == 'order':
				order_match = re.search('(.*?)ssmId":"(.*?)"(.*)',topic_value)
				order_ssm_id_val = order_match.group(2)
				order_topic_dict[order_ssm_id_val] = 'order'

	for line in collatFile:
		if not line.isspace():
			line = re.sub(r'\n$','',line)
			topic_name, topic_value = line.split('---')
			if topic_name == 'collat':
				collat_match = re.search('(.*?)ssmId":"(.*?)"(.*)',topic_value)
				collat_ssm_id_val = collat_match.group(2)
				collat_topic_dict[collat_ssm_id_val] = 'collat'
	return (ticket_topic_dict,existing_topic_dict,new_sec_topic_dict,order_topic_dict,collat_topic_dict)
			
def topic_format(event_ssm_dict,ticket_topic_dict,existing_topic_dict,new_sec_topic_dict,order_topic_dict,collat_topic_dict,ticket_count_dict,sheet4,sheet4_col_name,sheet1):
	# for Topic - ticket_topic 
	ticket_topic_N_count = 0
	ticket_topic_Y_count = 0
	existing_N_count = 0
	existing_Y_count = 0
	new_sec_N_count = 0
	new_sec_Y_count = 0
	order_N_count = 0
	order_Y_count = 0
	collat_N_count = 0
	collat_Y_count = 0
	Total_count = len(ticket_count_dict)	
	start_point = 1
	exist_Y_ssmid = {}
	new_sec_Y_ssm_id = {}

	if len(collat_topic_dict) == 0:
		for i in range(start_point,Total_count+1):
			sheet4.write(i+1,sheet4_col_name['topic_collat'],'N')
		collat_N_count = Total_count
		collat_Y_count = 0

	if len(order_topic_dict) == 0:
		for y in range(start_point,Total_count+1):
			sheet4.write(y+1,sheet4_col_name['topic_order'],'N')
		collat_N_count = Total_count
		collat_Y_count = 0

	if len(ticket_topic_dict) == 0:
		for z in range(start_point,Total_count+1):
			sheet4.write(z+1,sheet4_col_name['topic_ticket'],'N')
		ticket_topic_N_count = Total_count
		ticket_topic_Y_count = 0
	else:	
		for k, v in ticket_count_dict.items():
			if str(v) not in ticket_topic_dict.keys():
				ticket_topic_N_count += 1
				sheet4.write(k+1,sheet4_col_name['topic_ticket'],'N')
			else:
				ticket_topic_Y_count += 1
				sheet4.write(k+1,sheet4_col_name['topic_ticket'],'Y')

	#print "len of exsting topic dic:", len(existing_topic_dict),len(event_ssm_dict)
	#pprint.pprint(ticket_topic_dict)
	#pprint.pprint(ticket_count_dict)	

	##For RT_EXISTING_SECURITY.QA Topic log file
	#pprint.pprint(existing_topic_dict)
	#pprint.pprint(event_ssm_dict)
	existing_ssmid_list = []
	if len(existing_topic_dict) == 0:
		for e in range(start_point,Total_count+1):
			sheet4.write(e+1,sheet4_col_name['topic_existing'],'N')
		existing_N_count = Total_count
		existing_Y_count = 0	
		my_logger.info("TOPIC: RT.EXISTING_SECURITY.QA,No Count = %s" % existing_N_count)
	else:
		for sk, sv in event_ssm_dict.items():
			s_id,event_type,tr_price,pm_price = sv.split('|')
			for tk, tv in existing_topic_dict.items():
				quote_price,price = tv.split('|')
				if (str(tk) == str(s_id) and (event_type == 'BM CREATION' or \
																			event_type == 'BM CORRECT' or \
																			event_type == 'TRADE TICKET CREATE' or \
																			event_type == 'ALLOCATION SENT' or \
																			event_type == 'TRADE RELEASED' or \
																			event_type == 'TRADE CONFIRMED' or \
																			event_type == 'MASTER TICKET CREATE' or \
																			event_type == 'BLOCK SENT TO VCON' or \
																			event_type == 'BM CANCEL' or \
																			event_type == 'NEW MASTER TICKET' or \
																			event_type == 'WI CONVERSION' or \
																			event_type == 'PRICE CHANGE')) and (str(quote_price) == str(tr_price) and str(price) == str(pm_price)):
					if s_id not in existing_ssmid_list:
						existing_ssmid_list.append(s_id)
						sheet4.write(sk+1,sheet4_col_name['topic_existing'],'Y')
						existing_Y_count += 1
						#Below dictionary contains ssm_id having Y value 
						exist_Y_ssmid[sk] = tk
						my_logger.info("TOPIC: RT.EXISTING_SECURITY.QA = Y for ssm_id:%s"%s_id)
		existing_N_count = Total_count - existing_Y_count
	my_logger.info("TOPIC: RT.EXISTING_SECURITY.QA, Yes count = %s" % existing_Y_count)
	#pprint.pprint(matched_Y_ssmid)

	# Print N value for existing topics
	for sk, sv in event_ssm_dict.items():
		if sk not in exist_Y_ssmid.keys():
			sheet4.write(sk+1,sheet4_col_name['topic_existing'],'N')
	new_sec_ssmid_list = []
	if len(new_sec_topic_dict) == 0:
		for n in range(start_point,Total_count+1):
			sheet4.write(n+1,sheet4_col_name['topic_new_sec'],'N')
		new_sec_N_count = Total_count
		new_sec_Y_count = 0	
		my_logger.info("TOPIC: RT.NEW_SECURITY.QA,No Count = %s" % new_sec_N_count)
	else:
		for sk, sv in event_ssm_dict.items():
			s_id,event_type,tr_price,pm_price = sv.split('|')
			for tk, tv in new_sec_topic_dict.items():
				quote_price,price = tv.split('|')
				if (str(tk) == str(s_id) and (event_type == 'BM CREATION' or \
																			event_type == 'BM CORRECT' or \
																			event_type == 'TRADE TICKET CREATE' or \
																			event_type == 'PRICE CHANGE' or \
																			event_type == 'TRADE RELEASED' or \
																			event_type == 'TRADE CONFIRMED' or \
																			event_type == 'MASTER TICKET CREATE' or \
																			event_type == 'BLOCK SENT TO VCON' or \
                                      event_type == 'BM CANCEL' or \
                                      event_type == 'NEW MASTER TICKET' or \
                                      event_type == 'WI CONVERSION' or \
																			event_type == 'ALLOCATION SENT')) and (str(quote_price) == str(tr_price) and str(price) == str(pm_price)):
					if s_id not in new_sec_ssmid_list:
						new_sec_ssmid_list.append(s_id)
						sheet4.write(sk+1,sheet4_col_name['topic_new_sec'],'Y')
						new_sec_Y_count += 1	
						new_sec_Y_ssm_id[sk] = tk
						my_logger.info("TOPIC: RT.NEW_SECURITY.QA = Y for ssm_id: %s" % s_id)
						my_logger.info("TOPIC: RT.NEW_SECURITY.QA, Yes Count = %s" % new_sec_Y_count)
		new_sec_N_count = Total_count - new_sec_Y_count
	my_logger.info("TOPIC: RT.NEW_SECURITY.QA, No Count = %s" % new_sec_N_count)
	
	#print N value for New Security topic
	for sk, sv in event_ssm_dict.items():
		if sk not in new_sec_Y_ssm_id.keys():
			sheet4.write(sk+1,sheet4_col_name['topic_new_sec'],'N')

	print "Existing N:", existing_N_count
	print "Existing Y:", existing_Y_count

	# Write RT.EXISTING_SECURITY.QA Value to Summary sheet
	sheet1.write(27,2,existing_N_count)
	sheet1.write(28,2,existing_Y_count)
	sheet1.write(29,2,Total_count)

	print "New sec N:", new_sec_N_count
	print "New sec Y:", new_sec_Y_count
	sheet1.write(27,5,(Total_count - new_sec_Y_count))
	sheet1.write(28,5,new_sec_Y_count)
	sheet1.write(29,5,Total_count)
	
def add_chart_existing_topic(excel,book,sheet1):
	chart_title = 'INTRADAY: MQ Message for Existing Security'
	chart = excel.addChart(book,'pie')	
	category = '=QA Summary!B28:B29'
	values = '=QA Summary!C28:C29'
	excel.chartData(chart,'Total',category,values)
	chart.set_title({'name':chart_title})
	chart.set_x_axis({'num_font':  {'name':'Calibri', 'size': 12}})
	chart.set_style(10)
	sheet1.insert_chart('I2',chart,{'x_scale':0.90, 'y_scale':0.90})

def add_chart_new_sec(excel,book,sheet1):
	chart_title = 'INTRADAY: MQ Message for New Security'
	chart = excel.addChart(book,'pie')
	category = '=QA Summary!E28:E29'
	values = '=QA Summary!F28:F29'
	excel.chartData(chart,'Total',category,values)
	chart.set_title({'name':chart_title})
	chart.set_x_axis({'num_font':  {'name':'Calibri', 'size': 12}})
	chart.set_style(10)
	sheet1.insert_chart('O2',chart,{'x_scale':0.90, 'y_scale':0.90})	
 
def add_summary_data(fob_check_summary_file,sheet2,excel,merge_style_list,heading_format):
	summary_file_obj = open(fob_check_summary_file,'r')
	for key, val in mergedict.items():
		if key == 'sheet2':
			i = 1
			for col, heading in val.items():
				excel.merge(sheet2,col,heading,merge_style_list[i])
				i += 1
	after_pat = []
	after_ssmid = []
	row = 11
	for line in summary_file_obj:
		if re.search(r'after pattern', line):
			after_pat.append(line)
		elif re.search(r'after ssm_id', line):
			after_ssmid.append(line)
		else:
			sheet2.write(row,0,line)
		row += 1
	
	bmrow = 16
	pmrow = 16
	allocrow = 16
	col = 0
	for item in after_pat:
		val = re.sub(r'(.*?)(after|count)(.*?):(.*)',r'\1:\4',item)
		if re.search(r'^BM',item):
			sheet2.write(bmrow,col,val)
			bmrow += 1
		if re.search(r'^PM',item):
			sheet2.write(pmrow,col+1, val)
			pmrow += 1
		if re.search(r'^ALLOC',item):
			sheet2.write(allocrow,col+2, val)
			allocrow += 1

	bmrow = 16
	pmrow = 16
	allocrow = 16
	for item in after_ssmid:
		tempval= re.sub(r'(.*?)(count|after)(.*?):(.*)',r'\1:\4',item)
		if re.search(r'^BM',item):
			sheet2.write(bmrow,col+3,tempval)
			bmrow += 1
		if re.search(r'^PM',item):
			sheet2.write(pmrow,col+4,tempval)
			pmrow += 1
		if re.search(r'^ALLOC',item):
			sheet2.write(allocrow, col+5, tempval)
			allocrow += 1
		
def occurCount(listName):
	''' To count occurrences of the items in a list '''
	tempdict = {}
	for item in listName:
		if item in tempdict:
			tempdict[item] = tempdict[item] + 1
		else:
			tempdict[item] = 1
	return tempdict

def cstc_summary(cstc_event_list,sheet1,matchdict,excel,heading_format,bold,book):
	cstc_dict = occurCount(cstc_event_list)
	#pprint.pprint(cstc_dict)
	full_list = []
	cstc_list = []
	for key, value in cstc_dict.items():
		templist = []
		cstc_name, event_name = key.split('|')
		#print "CCCCC:", cstc_name, event_name
		if cstc_name not in cstc_list:
			cstc_list.append(cstc_name)
	
		templist.append(cstc_name)
		templist.append(event_name)
		templist.append(value)
		full_list.append(templist)
	arr = []
	for c in cstc_list:
		t_value = []
		t_value.append(c)
		for f in full_list:
			if c == f[0]:
				t_value.append(f[1])
				t_value.append(f[2])
		arr.append(t_value)

	#print "MATCH ARR:", match_arr
	#Prepare list for the cstc tabuler to print into ExcelSheet
	final = []
	for item in arr:
		temp = [0]*int(len(matchdict) + 1)
		i = 1
		del temp[0]
		temp.insert(0,item[0])
		while i < len(item):
			for key, value in matchdict.items():
				if item[i] == key:
					del temp[value]
					temp.insert(value,item[i+1])
			i += 2
		total = sum(temp[1:])
		temp.append(total)
		final.append(temp)

	cstc_category = '=QA Summary!B36:B'+str(len(final)+35)
	bm_value = '=QA Summary!C36:C' + str(len(final)+35)
	pm_value = '=QA Summary!D36:D' + str(len(final)+35)
	al_value = '=QA Summary!E36:E' + str(len(final)+35)
	bc_value = '=QA Summary!F36:F' + str(len(final)+35)
	ttc_value = '=QA Summary!G36:G' + str(len(final)+35)
	nmt_value = '=QA Summary!H36:H' + str(len(final)+35)
	as_value = '=QA Summary!I36:I' + str(len(final)+35)
	pc_value = '=QA Summary!J36:J' + str(len(final)+35)
	tr_value = '=QA Summary!K36:K' + str(len(final)+35)
	tc_value = '=QA Summary!L36:L' + str(len(final)+35)
	mtc_value = '=QA Summary!M36:M' + str(len(final)+35)
	bsv_value = '=QA Summary!N36:N' + str(len(final)+35)
	bcancel_value = '=QA Summary!O36:O' + str(len(final)+35)
	wc_value = '=QA Summary!P36:P' + str(len(final)+35)
	pm_crt_val = '=QA Summary!Q36:Q' + str(len(final)+35)
	pm_ttc_val = '=QA Summary!R36:R' + str(len(final)+35)
	pm_wc_val = '=QA Summary!S36:S' + str(len(final)+35)
	pm_acc_val = '=QA Summary!T36:T' + str(len(final)+35)
	pm_err_val = '=QA Summary!U36:U' + str(len(final)+35)
	pm_rej_val = '=QA Summary!V36:V' + str(len(final)+35)
	pm_man_val = '=QA Summary!W36:W' + str(len(final)+35)
	pm_inq_val = '=QA Summary!X36:X' + str(len(final)+35)
	pm_inv_val = '=QA Summary!Y36:Y' + str(len(final)+35)
	pm_presp_val = '=QA Summary!Z36:Z' + str(len(final)+35)
	pm_asset_val = '=QA Summary!AA36:AA' + str(len(final)+35)
	pm_sent_val = '=QA Summary!AB36:AB' + str(len(final)+35)
	pm_pend_val = '=QA Summary!AC36:AC' + str(len(final)+35)
	pm_sav_val = '=QA Summary!AD36:AD' + str(len(final)+35)
	pm_new_val = '=QA Summary!AE36:AE' + str(len(final)+35)
	pm_correct_val = '=QA Summary!AF36:AF' + str(len(final)+35)
	
	table_range = 'B36:AG' + str(len(final)+ 35 + 1)
	#print "TTT:", table_range
	sheet1.add_table(table_range,{'header_row' : 0})
	row_no = 35
	for data in final:
		col_no = 1
		for i in data:
			sheet1.write(row_no,col_no,i)
			col_no += 1
		row_no += 1	

	###Value of Grand Total field
	g_total_row = 35 + len(final)
	#print "G TOTAL ROW = ", g_total_row
	sheet1.write(g_total_row,1,'Grand Total',bold)
	col = 2
	chr_val = 67
	next_col = 65
	while col < int(len(matchdict) + 1) + 2:
		if chr_val < 91:
			total_formula = '=SUM(' + chr(chr_val) +'36:' + chr(chr_val) + str(len(final)+35) + ')'
		else:
			total_formula = '=SUM(A' +chr(next_col) +'36:A' + chr(next_col) + str(len(final)+35) + ')'
			next_col += 1
		sheet1.write(g_total_row,col,total_formula)		
		col += 1
		chr_val += 1

	#Adding Stacked chart for cstc 	
	cstc_chart = book.add_chart({'type': 'column', 'subtype': 'stacked'})
	excel.chartData(cstc_chart,'BM BM Creation',cstc_category,bm_value)
	excel.chartData(cstc_chart,'PM* PM Creation',cstc_category,pm_value)
	excel.chartData(cstc_chart,'Allocation',cstc_category,al_value)
	excel.chartData(cstc_chart,'BM BM Correct ',cstc_category,bc_value)
	excel.chartData(cstc_chart,'BM TRADE TICKET CREATE',cstc_category,ttc_value)
	excel.chartData(cstc_chart,'BM NEW MASTER TICKET',cstc_category,nmt_value)
	excel.chartData(cstc_chart,'BM ALLOCATION SENT',cstc_category,as_value)
	excel.chartData(cstc_chart,'BM PRICE CHANGE',cstc_category,pc_value)
	excel.chartData(cstc_chart,'BM TRADE RELEASED',cstc_category,tr_value)
	excel.chartData(cstc_chart,'BM TRADE CONFIRMED',cstc_category,tc_value)
	excel.chartData(cstc_chart,'BM MASTER TICKET CREATE',cstc_category,mtc_value)
	excel.chartData(cstc_chart,'BM BLOCK SENT TO VCON',cstc_category,bsv_value)
	excel.chartData(cstc_chart,'BM BM CANCEL',cstc_category,bcancel_value)
	excel.chartData(cstc_chart,'BM WI CONVERSION',cstc_category,wc_value)
	
	cstc_chart.set_title({'name':'Product Distribution By comp_sec_type_code'})
	cstc_chart.set_style(10)
	sheet1.insert_chart('B2',cstc_chart,{'x_scale':1.2, 'y_scale':1})	
	
def rt_security_val(ssmId,dbobj):
	query_str = '''
					SELECT  ad.recordtype,ad.query 
					FROM stp_own.app_data ad, stp_own.app_master am
					WHERE ad.app_id = am.id
					AND am.id = '1' AND ad.recordtype = 'Notified_as' AND am.active = 'Y'
					'''
	#query = datadict['Notified_as'] + " = '" + ssmId + "'"
	value = dbobj.fetch_one(query_str)
	query = value[1] + " = '" + ssmId + "'"
	row = dbobj.fetch_one(query)
	if row:
		my_logger.debug('RT_SECURITY: Notified_as returns: ' + str(row[0]))
		return row[0]
	else:
		my_logger.debug('RT_SECURITY: Notified_as returns: (None)')
		return ('NA')

def new_sec_case(dbobj,sheet5):
	ssm_core_ssmid = []
	ftotal_qty_zero = []
	ftotal_qty_not_zero = []
	core_sysdate_change = []

	sysdate_query = 'select to_char(sysdate,\'DD-MON-YYYY\') from dual'
	sysdate = dbobj.fetch_one(sysdate_query)

	back_sysdate_query = 'select to_char(sysdate-2,\'DD-MON-YYYY\') from dual'
	back_date = dbobj.fetch_one(back_sysdate_query)

	select_ssm_core = 'select ssm_id from taps_own.ssm_core order by ssm_id asc'
	dbobj.execute(select_ssm_core)
	fetch_all_row = dbobj.fetch_all()
	# Divide total ssm_core count to 5 testcases
	ssmCoreCount = len(fetch_all_row)
	countPerTestcase = ssmCoreCount / 5
	first_count = 0
	for row in fetch_all_row:
		ssm_core_ssmid.append(row[0])
		first_count += 1
		if first_count == countPerTestcase:
			break

	##first testcase- update entry_date to sysdate and remove data from firm_total
	sys_update_query = 'update taps_own.ssm_core set entry_date = sysdate where ssm_id in'+ str(tuple(ssm_core_ssmid))
	#print "TEST1:", sys_update_query
	dbobj.execute(sys_update_query)
	dbobj.commit()

	delete_ftotal_query = 'delete from taps_own.ssm_firm_total where ssm_id in' + str(tuple(ssm_core_ssmid))
	#print "TEST1Q:", delete_ftotal_query
	dbobj.execute(delete_ftotal_query)
	dbobj.commit()
	test1_sysdate = [str(sysdate[0])] * len(ssm_core_ssmid)
	test1_ftotal = ['N'] * len(ssm_core_ssmid)

	sheet5.write_column('A3',ssm_core_ssmid)
	sheet5.write_column('B3',test1_sysdate)
	sheet5.write_column('C3',test1_ftotal)

	#### End of first testcase

	###Second test case- entry_date = sysdate and qty = 0

	remain_ssmid_list = [row[0] for row in fetch_all_row if row[0] not in ssm_core_ssmid]
	#print "RRRRR:", remain_ssmid_list
	ftotal_qty_zero = remain_ssmid_list[0:countPerTestcase]
	#print "FTOOTOOTOOTOO:", ftotal_qty_zero
	test2_core_query = 'update taps_own.ssm_core set entry_date = sysdate where ssm_id in' + str(tuple(ftotal_qty_zero))
	#print "TEST2:", test2_core_query
	dbobj.execute(test2_core_query)
	dbobj.commit()
	test2_qty_zero = 'update taps_own.ssm_firm_total set qty = 0 where ssm_id in' + str(tuple(ftotal_qty_zero))
	#print "TEST2Q:", test2_qty_zero
	dbobj.execute(test2_qty_zero)
	dbobj.commit()

	test2_sysdate = [str(sysdate[0])] * len(ftotal_qty_zero)
	test2_qty = [0]* len(ftotal_qty_zero)
	sheet5.write_column('D3',ftotal_qty_zero)
	sheet5.write_column('E3',test2_sysdate)
	sheet5.write_column('F3',test2_qty)

	### End of second testcase

	##Third testcase: entry_date = sysdate and qty != 0
	remain_ssmid_list2 = [row for row in remain_ssmid_list if row not in ftotal_qty_zero]
	ftotal_qty_not_zero = remain_ssmid_list2[0:countPerTestcase]

	test3_core_query = 'update taps_own.ssm_core set entry_date = sysdate where ssm_id in' + str(tuple(ftotal_qty_not_zero))
	#print "TEST3:", test3_core_query
	dbobj.execute(test3_core_query)
	dbobj.commit()

	test3_qty_query = 'update taps_own.ssm_firm_total set qty = 100 where ssm_id in ' + str(tuple(ftotal_qty_not_zero))
	#print "TEST32:", test3_qty_query
	dbobj.execute(test3_qty_query)
	dbobj.commit()

	test3_sysdate = [str(sysdate[0])] * len(ftotal_qty_not_zero)
	test3_qty = [100] * len(ftotal_qty_not_zero)

	sheet5.write_column('G3',ftotal_qty_not_zero)
	sheet5.write_column('H3',test3_sysdate)
	sheet5.write_column('I3',test3_qty)

	##Fourth Testcase- entry_date should be sysdate - 2 and qty = 0
	remain_ssmid_list3 = [item for item in remain_ssmid_list2 if item not in ftotal_qty_not_zero]
	core_sysdate_change = remain_ssmid_list3[0:countPerTestcase]
	test4_core_query = 'update taps_own.ssm_core set entry_date = trunc(sysdate - 2) where ssm_id in ' + str(tuple(core_sysdate_change))
	#print "TEST4 CORE:", test4_core_query
	dbobj.execute(test4_core_query)
	dbobj.commit()

	test4_qty_zero_query = 'update taps_own.ssm_firm_total set qty = 0 where ssm_id in ' +str(tuple(core_sysdate_change))
	#print "TEST4:QTY: ", test4_qty_zero_query
	dbobj.execute(test4_qty_zero_query)
	dbobj.commit()

	test4_sysdate = [str(back_date[0])] * len(core_sysdate_change)
	test4_qty = [0] * len(core_sysdate_change)
	sheet5.write_column('J3',core_sysdate_change)
	sheet5.write_column('K3', test4_sysdate)
	sheet5.write_column('L3', test4_qty)

##FIFTH TESTCASE- entry_date should not be sysdate and ssm_id does not exist in firm_total table- it should be activated in rt_security table

	remain_ssmid_list4 = [item for item in remain_ssmid_list3 if item not in core_sysdate_change]
	core_sysdate_newdate = remain_ssmid_list4[0:countPerTestcase]
	test5_core_query = 'update taps_own.ssm_core set entry_date = trunc(sysdate - 10) where ssm_id in ' + str(tuple(core_sysdate_newdate))
	#print "TEST5 CORE:", test5_core_query
	dbobj.execute(test5_core_query)
	dbobj.commit()

	test5_qty_del_query = 'delete from taps_own.ssm_firm_total where ssm_id in ' + str(tuple(core_sysdate_newdate))
	#print "TEST5 QTY:", test5_qty_del_query
	dbobj.execute(test5_qty_del_query)
	dbobj.commit()

	test5_sysdate = [str(back_date[0])] * len(core_sysdate_newdate)
	test5_ftotal = ['N'] * len(core_sysdate_newdate)

	sheet5.write_column('M3', core_sysdate_newdate)
	sheet5.write_column('N3',test5_sysdate)
	sheet5.write_column('O3', test5_ftotal)
