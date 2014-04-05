#!/home/wxg/.virtualenvs/genpy/bin/python


# Nominatim web based geocoder 

import simplejson
import csv, urllib2, urllib, pickle, os
import pdb
import time
import shelve
import sys

project_path = '/home/wxg/Documents/Projects/21196_tribal_edu/geocode'
address_file = project_path + '/hd2012.csv'
geocoded_file = project_path + '/hd2012_geocoded_foo.csv'

resume_file = project_path + '/hd2012_geocoded.csv'

ident_field = 'UNITID'


def nominatim_geocode_csv(resume=False, resume_from_file=False):
	dictlist = []
	klist = []
	try:
		os.remove(project_path + '/shelve_db')
	except:
		pass
	dshelve = shelve.open(project_path + '/shelve_db', flag='c', writeback=True)
	with open(address_file, 'rw') as csvfile:
		filereader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
		for row in filereader:
			dictlist.append(row)
	if resume_from_file:
		with open(resume_file, 'r') as csvfile:
			filereader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
			for row in filereader:
				klist.append(row[ident_field])
	if resume:
		klist = dshelve.keys()
	if resume or resume_from_file:
		dictlist[:] = [x for x in dictlist if x[ident_field] not in klist]				
	answer = []
	try:
		for loc in dictlist:
			time.sleep(0)
			tup1 = (loc['Address'], loc['City'], loc['State'], loc['Zip'])
			formated = 'street=%s&city=%s&state=%s&postalcode=%s&countrycodes=US&format=json&addressdetails=0&limit=1&addressdetails=1' % tup1
			base = 'http://nominatim.openstreetmap.org/search?'
			url = base + formated
			url = urllib.quote_plus(url, '=&/:?')	
			print 'Requesting: ' + url
			response = urllib2.urlopen(url)
			response = response.read()
			geocode = simplejson.loads(response)
			if (geocode == []):
				tup2 = (loc['Address'], loc['City'], loc['State'])
				formated = 'street=%s&city=%s&state=%s&countrycodes=US&format=json&addressdetails=0&limit=1&addressdetails=1' % tup2
				base = 'http://nominatim.openstreetmap.org/search?'
				url = base + formated
				url = urllib.quote_plus(url, '=&/:?')	
				print 'Requesting: ' + url
				response = urllib2.urlopen(url)
				response = response.read()
				geocode = simplejson.loads(response)
			print 'Response: '
			ident_dict = {'ident_field' : loc[ident_field]}
			if len(geocode) < 1:
				geocode.append(dict())
			geocode[0].update(ident_dict)
			print geocode
			finding = pickle.dumps(geocode)
			dshelve[ident_dict['ident_field']] = finding
			dshelve.sync()
	except:
		dshelve.close()
		print "Unexpected error:", sys.exc_info()[0]
		pass
	try:	
		dshelve.close()
	except:
		print "Unexpected error:", sys.exc_info()[0]
		pass
	print 'Done Requesting Geocodes'




def geocode_outputer():
	import codecs
	dshelve = shelve.open(project_path + '/shelve_db', flag='r')
	with codecs.open(geocoded_file, mode='w', encoding='utf-8') as csvfile:
		klist = dshelve.keys()
		writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		dwriter = csv.DictWriter(csvfile, ['ident_field', 'display_name', 'importance', 'place_id', 'lon', 'lat', 'type', 'class', 'boundingbox'], extrasaction='ignore', delimiter=',', quotechar='"')
		writer.writerow([ident_field, 'display_name', 'importance', 'place_id', 'lon', 'lat', 'type', 'class', 'boundingbox'])
		for k in klist:
			data = dshelve[k]
			findingdict = pickle.loads(data)
			try:
				del findingdict[0]['address']
				del findingdict[0]['licence']
				for x in findingdict[0].keys():
					content = findingdict[0][x]
					if content == type('unicode'):
						findingdict[0][x] = u' '.join(content).encode('utf-8').strip()
					print content
					print type(content)
				dwriter.writerow(findingdict[0])
			except:
				print "Unexpected error:", sys.exc_info()[0]
				#pass
	csvfile.close()
	dshelve.close()



### Run the trap!
#nominatim_geocode_csv(resume_from_file=True)
geocode_outputer()






