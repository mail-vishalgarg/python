import xlsxwriter

class ExcelFormat(object):
	def __init__(self,WorkbookName):
		self.excelname = WorkbookName

	def createExcel(self):
		self.workbook = xlsxwriter.Workbook(self.excelname)
		return self.workbook	
	
	def addSheet(self, sheetName):
		self.worksheet = self.workbook.add_worksheet(sheetName)
		return self.worksheet
	
	def heading_format(self,workbookname):
		return workbookname.add_format({'bold': True,
						'align': 'center',
						'valign': 'vcenter',
						'fg_color': 'green',
						'color' : 'white',})

	def heading_format_gray(self,workbookname):
		return workbookname.add_format({'bold': True,
						'align': 'center',
						'valign': 'vcenter',
						'fg_color': 'gray',
						'color' : 'black',})

	def merge_format(self,workbookname,fg_color, fontcolor):
		return workbookname.add_format({'bold': 1,
						'align': 'center',
						'valign' : 'vcenter',
						'fg_color': fg_color,
						'color': fontcolor,})
	
	def merge(self,sheetname,range,text,mergeFormat):
		sheetname.merge_range(range,text,mergeFormat)						
	
	def colorFormat(self,workbookname,forecolor, fontcolor):
		return workbookname.add_format({'fg_color': forecolor,
						'color': fontcolor})

	def bold(self, workbookname):
		return workbookname.add_format({'bold': True})

	def addHeading(self,sheetName,row,headings,style=False):
		sheetName.write_row(row,headings,style)

	##Add a Chart	
	def addChart(self,workbookname,type):
		return workbookname.add_chart({'type': 'pie'})
	
	def chartData(self, chart, chartname, category, value):
		chart.add_series({
				  'name': chartname,
				  'categories': category,
				  'values': value})

	def chartProperty(self, chart, titlename,style):
		chart.set_title({'name': titlename})
		chart.set_style(style)

	def insertChart(self,sheetname,cellname,chart,x_offset,y_offset):
		sheetname.insert_chart(cellname,chart,{'x_offset': x_offset, 'y_offset': y_offset})

	def saveExcel(self):
		self.workbook.close()
