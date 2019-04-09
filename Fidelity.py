#stdlib
from json import load as _load, dumps as _dumps, loads as _loads, decoder as _decoder
from logging import getLogger as _getLogger, Formatter as _Formatter, basicConfig as _basicConfig, config as _config, INFO as _INFO
from argparse import ArgumentParser as _ArgumentParser
from re import compile as _compile, I as _I, S as _S
from sys import exc_info as _exc_info
from copy import deepcopy as _deepcopy
from os import getenv as _getenv, path as _path
from traceback import print_exc as _print_exc
from calendar import timegm as _timegm
from datetime import datetime as _datetime
from time import gmtime as _gmtime, sleep as _sleep
from uuid import uuid4 as _uuid4
from inspect import currentframe as currentframe, getargvalues as getargvalues
from threading import current_thread, local as _local

#3rd party libs
import pandas as _pandas
import numpy as _numpy
from mechanize import Browser as _Browser
from bs4 import BeautifulSoup as _BeautifulSoup
from requests import Session as _Session

#used by RequestFormatter
threadLocal = _local()

#define decorator to print all args for auditing purposes
def make_args_available_deco(f):
	def inner(*args, **kwargs):
		inner.actual_kwargs = kwargs
		logger = _getLogger(__name__)
		logger.info("in make_args_available_deco to output kwargs: %s"%(inner.actual_kwargs))
		frame = _currentframe()
		logger.info("current frame level data %s"%(str(_getargvalues(frame))))
		return f(*args,**kwargs)
	inner.actual_kwargs = None
	return inner

class RequestFormatter(_Formatter):
 def format(self, record):
    record.requestId =  getattr(threadLocal, 'requestId', "Unassigned")
    return super().format(record)

formatter = RequestFormatter(
 '[%(asctime)s] rid=%(requestId)s: '
 '%(levelname)s in %(module)s: message=%(message)s'
)

_basicConfig(level=_INFO)

logger = _getLogger()
logger.info('module level initialization of logger formatter')
for handler in logger.handlers:
	print('setting found handler formatter %s'%(str(handler)))
	handler.setFormatter(formatter)


@make_args_available_deco
def RequestViaMechanize(startPage = None, formName = None, formAction = None, targetPage = None, pin = None, customerID = None):
	logger = _getLogger(__name__)
	logger.info("In mainRestProcessor"%())
	if startPage is None:
		startPage = 'https://login.fidelity.com/ftgw/Fidelity/RtlCust/Login/Init'
	if formName is None:
		formName = 'Login'
	if formAction is None:
		formAction = 'https://login.fidelity.com/ftgw/Fas/Fidelity/RtlCust/Login/Response'
	if targetPage is None:
		targetPage = 'https://oltx.fidelity.com/ftgw/fbc/oftop/portfolio#summary'
	br = _Browser()
	br.set_handle_robots(False)
	br.set_handle_refresh(True, 10, True)
	OWNER = ''
	br.set_handle_redirect(True)
	br.addheaders = [
		('User-agent',
		'Mozilla/5.0 (X11; U; Linux i686; en-US; rv 1.0) %s' % OWNER),
	]
	try:
		br.open(startPage)
	except Exception as e:
		type_, value_, traceback_ = _exc_info()
		logger.error("problem opening page %s"%(str(e)))
		raise(e)
	try:
		br.select_form(formName)
	except Exception as e:
		type_, value_, traceback_ = _exc_info()
		logger.error("problem selecting form %s"%(str(e)))
		raise(e)
	br.form.action = formAction
	br.form.set_all_readonly(False)
	for control in br.form.controls:
			logger.debug(control.name)
			logger.debug(control.value)
			if control.name == "PIN":
					logger.info('found PIN')
					control.value = pin
			if control.name == "SSN" and control.type=="hidden":
					logger.info('found SSN')
					control.value = customerID
	try:
		r0 = br.submit()
	except Exception as e:
		type_, value_, traceback_ = _exc_info()
		logger.error("problem submitting form to %s : %s"%(br.form.action, str(e)))
		raise(e)
	t1 = None
	try:
		r = br.open(targetPage)
		r.get_data()
		t1 = r.get_data()
	except Exception as e:
		type_, value_, traceback_ = _exc_info()
		logger.error("problem opening target page %s : %s"%(targetPage, str(e)))
		raise(e)
	returnData = None
	if t1 is not None:
		returnData = t1.decode(br.encoding())
		
	return returnData, br

@make_args_available_deco
def MainResponseProcessor(inputData=None):
	logger = _getLogger(__name__)
	logger.info("In mainRestProcessor"%())
	strip_script = _compile(r'<script\s+.*?</script>', _I + _S)
	soup = _BeautifulSoup(strip_script.sub('', inputData),features="html5lib")
	elements = soup.findAll("div", {"class": "account-selector--tab account-selector--tab-all js-portfolio"})
	removeCurrencyMarks = _compile(r'[$,]')
	amount = 0
	for currentElement in elements:
		b1 = currentElement.get("data-total-balance")
		if b1 is not None: #found this value
			logger.info("found value %s"%(b1))
			b1 = removeCurrencyMarks.sub('',b1)
			try:
				amount = float(b1)
			except Exception as e:
				type_, value_, traceback_ = _exc_info()
				logger.error(str(e))
				raise(e)
			logger.info("after ETL %f"%(amount))
	return amount

def StoreResults(dirStorage = None, database = None, inputData = None, parsedValue = None, runTimeStamp = None, delim= '\x1e' ):
	logger = _getLogger(__name__)
	fileTimeStamp = _datetime.strftime(runTimeStamp,'%Y-%m-%d-T-%H-%M-%S')
	logger.info("StoreResults with %s and %s with filestamp as %s "%(dirStorage, database,runTimeStamp))
	fileName = '%sFidelity%s.csv'%(dirStorage,fileTimeStamp)
	with open(fileName,'w') as fid:
		fid.write(_dumps({'data':inputData}))
	with open(database,'a') as fid:
		fid.write("%s%s%s\n"%(runTimeStamp,delim,parsedValue))

@make_args_available_deco
def RequestViaSession(startPage = None, formAction = None, targetPage = None, pin=None, customerID = None):
	logger = _getLogger(__name__)
	s = _Session()
	if startPage is None:
		startPage = 'https://login.fidelity.com/ftgw/Fidelity/RtlCust/Login/Init'
	if formAction is None:
		formAction = 'https://login.fidelity.com/ftgw/Fas/Fidelity/RtlCust/Login/Response'
	if targetPage is None:
		targetPage = 'https://oltx.fidelity.com/ftgw/fbc/oftop/portfolio#summary'
	d1 = {"PIN":pin,"SSN":customerID,"SavedIdInd":"N"}
	logger.info("requesting landing page via get")
	r0 = s.get(startPage)
	if r0.status_code >= 200 and r0.status_code < 300:
		logger.info("form requests stats code %d"%(r0.status_code))
	logger.info("requesting form action with supplied data")
	r1 = s.post(formAction, data=d1)
	if r1.status_code >= 200 and r1.status_code < 300:
		logger.info("form requests stats code %d"%(r1.status_code))
	r2 = s.get(targetPage)
	returnData = None
	if r2.status_code >= 200 and r2.status_code < 300:
		logger.info("targetPage requests stats code %d"%(r2.status_code))
		returnData = r2.text
	return returnData, s

if __name__ == "__main__":
	currentutc = _datetime.utcnow()
	logger = _getLogger(__name__)
	logger.info("running as script at %s"%(currentutc))
	parser = _ArgumentParser(description='Retrieve and Process Data')
	parser.add_argument('--dirStorage', dest='dirStorage', default="c:/tmp",
											help='directory to hold raw dump of files')
	parser.add_argument('--database', dest='database', default="c:/tmp/db.csv",
											help='database to hold extracted data (e.g. csv file)')
	parser.add_argument('--pin', dest='pin', default="none",
											help='pin for user')
	parser.add_argument('--customer_id', dest='customerID', default="none",
											help='customerID for user')
	args = parser.parse_args()
	data = None
	try:
		pin = _getenv("PIN")
		if pin is None:#get from parser
			logger.info("pulling pin from args")
			pin = args.pin
		customerID = _getenv("CUSTOMER_ID")
		if customerID is None:#get from parser
			logger.info("pulling customerID from args")
			customerID = args.customerID
		data, browser = RequestViaMechanize(pin=pin, customerID=customerID)
		logger.info("response data:%s"%(data[:1000]))
	except Exception as e:
		type_, value_, traceback_ = _exc_info()
		logger.error(str(e))
	amount = None
	try:
		amount = MainResponseProcessor(inputData=data)
		logger.info("response amount:%s"%(amount))
	except Exception as e:
		type_, value_, traceback_ = _exc_info()
		logger.error(str(e))
	if data is not None and amount is not None:
		StoreResults(dirStorage = args.dirStorage, database = args.database, inputData = data, parsedValue = amount, runTimeStamp = currentutc)
	else:
		logger.info("data or amount is none")
	
	


