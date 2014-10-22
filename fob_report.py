from mq_report_new import *
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
from fob_backup_remove import *

def Usage():
	print 'This script will validate data inserted in stp_own schema and compare'
	print 'attribute values in prod, qa and MQ topics.'
	print 'Usage: '+ scriptName + ' -d sand -f <Input File Name>'
	print '-d Database name (e.g. sand)'
	print '-f Input file name'
	print '-t to truncate tables'
	sys.exit()


def main():
	global scriptName, scriptbase_name,mq_obj
	global excel, book
	global heading_format,heading_format_gray, bold,redcolor,greencolor,graycolor, merge_style1
	global merge_style2,merge_style3, merge_style4,sheet4, sheet3, sheet2, sheet1
	global mainInputFile,heading_row,dbtype,trunc_flag
	global dbobj, conn
	global raw_inbound_count
	global mqTopicFile,fobReportName
	global newSecFlag

	
	raw_inbound_count = 0
	newSecFlag = 0
	
	scriptName = os.path.basename(__file__)
	scriptbase_name = scriptName.split('.')[0]

  #Get current pid and current date
	get_pid = str(os.getpid())
	current_date = str(time.strftime("%d-%m-%Y"))
	dbtype = ''
	inputfile_name = ''
	heading_row = 0
	trunc_flag = 0

	if len(sys.argv[1:]) < 4:
		#print 'Please provide valid options!!'+"\n"
		my_logger.error('Please provide valid options!!')
		Usage()
		threadstart(True)
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'htnd:f:')
	except getopt.GetoptError as err:
		#print err
		my_logger.error('',exc_info=1)
		Usage()
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			Usage()
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
				if  os.path.isfile(mainInputFile):
					my_logger.info('%s file exist' % mainInputFile)
					#print '%s file exist' % mainInputFile
				else:
					my_logger.error('File does not exist : %s'% mainInputFile)
					sys.exit(1)
					#print 'File does not exist : %s'% mainInputFile
		elif opt in ("-t", "--trunc"):
			trunc_flag = 1
		elif opt in ("-n", "--newsec"):
			newSecFlag = 1
		else:
			#print "Please check input arguments!!\n"
			my_logger.error('Please check input arguments!!')
			Usage()

	if newSecFlag:
		fobReportName = exist_vs_new_reportName + '.' +current_date + '.' + get_pid + '.xlsx'
	else:
		fobReportName = fobReportFileName + '.' +current_date + '.' + get_pid + '.xlsx'

	my_logger.info("=================Test session starts====================")
	## Remove mq_topic.log file each time before processing

	## backup of all MQ files
	try:
		for file in mq_topic_file_list:
			backupFile(file,fobOutputDir)
	except:
		my_logger.error('', exc_info=1)

	##To remove existing topic files if any
	try:
		for file in mq_topic_file_list:
			removeFile(file)
	except:
		my_logger.error('', exc_info=1)

	##Connect with MQ
	mq_obj = mq_message()
	mq_obj.mq_connect()
	threadstart()
	
	##Object to make Excel file
	excel = ExcelFormat(fobReportName)
	book = excel.createExcel()
	heading_format = excel.heading_format(book)
	heading_format_gray = excel.heading_format_gray(book)
	bold = excel.bold(book)
	redcolor = excel.colorFormat(book,'red','white')
	greencolor = excel.colorFormat(book,'green','white')
	graycolor = excel.colorFormat(book,'gray','black')
	merge_style1 = excel.merge_format(book,'red','white')
	merge_style2 = excel.merge_format(book,'yellow','black')
	merge_style3 = excel.merge_format(book,'green','white')
	merge_style4 = excel.merge_format(book,'blue','white')

	merge_style_list = [merge_style1,merge_style2,merge_style3,merge_style4]

	sheet1 = excel.addSheet("QA Summary")
	sheet2 = excel.addSheet("Before")
	sheet3 = excel.addSheet("After")
	sheet4 = excel.addSheet("Prod vs QA")
	if newSecFlag:
		sheet5 = excel.addSheet("Testcases")

	# Database connectivity
	dbobj = pm_DbCon.oracle()
	conn = dbobj.connect(dbtype)

	# Excel sheets heading
	heading_dict,index_dict = get_heading(dbobj)
	#pprint.pprint(index_dict)
	if newSecFlag:
		put_headings(excel,sheet1,sheet2,sheet3,sheet4,heading_dict,index_dict,heading_format,table_list,merge_style_list,bold,newSecFlag,sheet5)
	else:
		put_headings(excel,sheet1,sheet2,sheet3,sheet4,heading_dict,index_dict,heading_format,table_list,merge_style_list,bold,newSecFlag)
		
	if trunc_flag:
		before_db_count(table_list,dbobj,sheet2)
	else:
		raw_inbound_count = db_count(table_list,dbobj,sheet2)

	total_count = before(mainInputFile,heading_row,sheet2)
	if newSecFlag:
		new_sec_case(dbobj,sheet5)
		subject = 'FOB New Security Report'
		text = 'FOB Report for New Security'
	else:
		subject = 'FOB Before and After Report'
		text = 'FOB Report' 

	##xml data file
	xml_final_data = xml_data(mainInputFile)
	xml_data_file = fobOutputDir + 'xml_input_data' + current_date + '_' + get_pid + '.log'
	file_xml_data = open(xml_data_file,'w')
	pprint.pprint(xml_final_data,file_xml_data)
	
	##First Step
	##Put all the xml messages to MQ Queue for processing
	tableCountData = put_mq_message(mq_obj,mainInputFile,dbobj,table_list)

	##Second Step
	##get all database value and write to a file
	sand_final_data,ticket_count_dict,event_ssm_dict,cstc_event_list = 	after(xml_final_data,dbobj,newSecFlag)
	database_data_file = fobOutputDir + 'database_final_data' + current_date + '_' + get_pid + '.log'
	database_final_data = open(database_data_file,'w')
	pprint.pprint(sand_final_data,database_final_data)	
	my_logger.info("after() function is completed")

	## Step three
	## Start reporting all the data to worksheets
	report_sheet3(tableCountData,sheet3)

	##sheet4_col_name is variable having all the column name for Excel file
	sheet4_col_name = column_name(dbobj,'sheet4')
	compare = compare_field(dbobj,'sheet4')
	noncompare = noncompare_field(dbobj,'sheet4')
	
	report_format(total_count,sheet4,sheet4_col_name,xml_final_data,sand_final_data,redcolor,graycolor,compare,noncompare,dbobj,newSecFlag)
	threadstart(True)

	my_logger.info("Thread closed")

	ticket_topic_dict,existing_topic_dict,new_sec_topic_dict,order_topic_dict,collat_topic_dict = read_topic_file(mqExistingTopicFile,mqNewSecTopicFile,mqTicketTopicFile,mqOrderTopicFile,mqCollatTopicFile)

	my_logger.info("read_topic_file() function is done")
  #pprint.pprint(ticket_topic_dict)
  #pprint.pprint(existing_topic_dict)
  #pprint.pprint(ticket_count_dict)
  #pprint.pprint(event_ssm_dict)

	topic_format(event_ssm_dict,ticket_topic_dict,existing_topic_dict,new_sec_topic_dict,order_topic_dict,collat_topic_dict,ticket_count_dict,sheet4,sheet4_col_name,sheet1)
	my_logger.info("Topic_format() is completed")
	add_summary_data(fobCheckSummaryFile,sheet2,excel,merge_style_list,heading_format)
	my_logger.info("add_summary_data() function is done")

	add_chart_existing_topic(excel,book,sheet1)
	my_logger.info("add_chart_existing() function is done")
	add_chart_new_sec(excel,book,sheet1)
	my_logger.info("add_chart_new_sec() function is done")

	sheet1_col = column_name(dbobj,'sheet1')
	cstc_summary(cstc_event_list,sheet1,sheet1_col,excel,heading_format,bold,book)
	my_logger.info("cstc_smmary() function is done")
	excel.saveExcel()
	mq_obj.mq_disconnect()
	notify(send_from, send_to, subject,text,files=[fobReportName])
	my_logger.info('Saved in Excel file ' + fobReportName)
	my_logger.info('Email send to :' + send_to)
	my_logger.info("====================Test session end===================")
if __name__ == '__main__':
	try:
		main()
	except:
		my_logger.error('',exc_info=1)
		subject = 'Error in fob_report.py script'
		notify(send_from,send_to,subject,str(traceback.format_exc()),files=[])

