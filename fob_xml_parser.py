import xml.dom.minidom

def xml_parse(list,msg):
	plist = {}
	try:
		dom = xml.dom.minidom.parseString(msg.encode("utf-8"))
	except xml.parsers.expat.ExpatError:
		pass

	try:
		for item in list:
			plist[item.lower()] = dom.getElementsByTagName(item)[0].firstChild.nodeValue.encode('ascii')
		return plist
	except IndexError:
		pass
