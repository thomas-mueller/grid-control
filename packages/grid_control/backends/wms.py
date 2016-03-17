#-#  Copyright 2007-2016 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

# Generic base class for workload management systems

import os, logging
from grid_control import utils
from grid_control.backends.access import AccessToken
from grid_control.backends.storage import StorageManager
from hpfwk import AbstractError, NamedPlugin, NestedException

class BackendError(NestedException):
	pass

class WMS(NamedPlugin):
	configSections = NamedPlugin.configSections + ['wms', 'backend']
	tagName = 'wms'

	def __init__(self, config, wmsName, oslayer):
		self._log = utils.getNamedLogger('WMS', wmsName, self)
		NamedPlugin.__init__(self, config, wmsName)
		wmsName = utils.QM(wmsName, wmsName, self.__class__.__name__).upper().replace('.', '_')
		(self.config, self.wmsName, self._oslayer) = (config, wmsName, oslayer)
		self._wait_idle = config.getInt('wait idle', 60, onChange = None)
		self._wait_work = config.getInt('wait work', 10, onChange = None)

	def getTimings(self): # Return (waitIdle, wait)
		return utils.Result(waitOnIdle = self._wait_idle, waitBetweenSteps = self._wait_work)

	def getBrokerList(self):
		raise AbstractError

	def canSubmit(self, neededTime, canCurrentlySubmit):
		raise AbstractError

	def getAccessToken(self, wmsId):
		raise AbstractError # Return access token instance responsible for this wmsId

	def checkJobs(self, ids): # ids = [(WMS-61226, 1), (WMS-61227, 2), ...]
		raise AbstractError # Return (jobNum, wmsId, state, info) for active jobs

	def retrieveJobs(self, ids):
		raise AbstractError # Return (jobNum, retCode, data, outputdir) for retrived jobs

	def cancelJobs(self, ids):
		raise AbstractError # Return (jobNum, wmsId) for cancelled jobs

	def _splitId(self, wmsId):
		if wmsId.startswith('WMSID'): # local wms
			return tuple(wmsId.split('.', 2)[1:])
		elif wmsId.startswith('http'): # legacy support
			return ('grid', wmsId)

	def _getRawIDs(self, ids):
		return map(lambda (wmsId, jobNum): self._splitId(wmsId)[1], ids)

	def parseJobInfo(fn):
		if not os.path.exists(fn):
			return utils.eprint('Warning: "%s" does not exist.' % fn)
		try:
			info_content = open(fn, 'r').read()
		except Exception, ex:
			return utils.eprint('Warning: Unable to read "%s"!\n%s' % (fn, str(ex)))
		if not info_content:
			return utils.eprint('Warning: "%s" is empty!' % fn)
		try:
			data = utils.DictFormat().parse(info_content, keyParser = {None: str})
			return (data['JOBID'], data['EXITCODE'], data)
		except Exception:
			return utils.eprint('Warning: Unable to parse "%s"!' % fn)
	parseJobInfo = staticmethod(parseJobInfo)
utils.makeEnum(['WALLTIME', 'CPUTIME', 'MEMORY', 'CPUS', 'BACKEND', 'SITES', 'QUEUES', 'SOFTWARE', 'STORAGE'], WMS, useHash = True)


class InactiveWMS(WMS):
	def __init__(self, config, wmsName, oslayer):
		WMS.__init__(self, config, wmsName, oslayer)
		self._token = config.getCompositePlugin(['access token', 'proxy'], 'TrivialAccessToken',
			'MultiAccessToken', cls = AccessToken, inherit = True, tags = [self]).getInstance(oslayer)

	def canSubmit(self, neededTime, canCurrentlySubmit):
		return True

	def getAccessToken(self, wmsId):
		return self._access

	def submitJobs(self, jobNumList, task): # jobNumList = [1, 2, ...]
		utils.vprint('Inactive WMS (%s): Discarded submission of %d jobs' % (self.wmsName, len(jobNumList)), -1)

	def checkJobs(self, ids): # ids = [(WMS-61226, 1), (WMS-61227, 2), ...]
		utils.vprint('Inactive WMS (%s): Discarded check of %d jobs' % (self.wmsName, len(ids)), -1)

	def retrieveJobs(self, ids):
		utils.vprint('Inactive WMS (%s): Discarded retrieval of %d jobs' % (self.wmsName, len(ids)), -1)

	def cancelJobs(self, ids):
		utils.vprint('Inactive WMS (%s): Discarded abort of %d jobs' % (self.wmsName, len(ids)), -1)
WMS.moduleMap['inactive'] = 'InactiveWMS'


class BasicWMS(WMS):
	def __init__(self, config, wmsName, oslayer, submit, check, cancel, retrieve, brokers):
		WMS.__init__(self, config, wmsName, oslayer)

		self._aspect_submit = submit
		self._aspect_check = check
		self._aspect_cancel = cancel
		self._aspect_retrieve = retrieve
		self._aspects_brokers = brokers

		ulog = logging.getLogger('user')
		if self.wmsName != self.__class__.__name__.upper():
			ulog.log(logging.INFO, 'Using submission backend: %s (%s)' % (self.__class__.__name__, self.wmsName))
		else:
			ulog.log(logging.INFO, 'Using submission backend: %s' % self.wmsName)

		self._errorLog = config.getWorkPath('error.tar')

		# Initialise access token, broker and storage manager
		self._token = config.getCompositePlugin(['access token', 'proxy'], 'TrivialAccessToken',
			'MultiAccessToken', cls = AccessToken, inherit = True, tags = [self]).getInstance(oslayer)
		for aspect in [submit, check, cancel, retrieve]:
			aspect.setAccess(self._token)

		# UI -> SE -> WN
		self.smSEIn = config.getPlugin('se input manager', 'SEStorageManager', cls = StorageManager, tags = [self]).getInstance('se', 'se input', 'SE_INPUT')
		self.smSBIn = config.getPlugin('sb input manager', 'LocalSBStorageManager', cls = StorageManager, tags = [self]).getInstance('sandbox', 'sandbox', 'SB_INPUT')
		# UI <- SE <- WN
		self.smSEOut = config.getPlugin('se output manager', 'SEStorageManager', cls = StorageManager, tags = [self]).getInstance('se', 'se output', 'SE_OUTPUT')
		self.smSBOut = None

	def getBrokerList(self):
		return self._aspects_brokers

	def canSubmit(self, neededTime, canCurrentlySubmit):
		return self._token.canSubmit(neededTime, canCurrentlySubmit)

	def getAccessToken(self, wmsId):
		return self._token

	def _processRequirements(self, reqs):
		for aspect_brokers in self._aspects_brokers:
			reqs = aspect_brokers.processRequirements(reqs)
		return reqs

	def submitJobs(self, jobNumList, packageManager):
		packageManager.setReqProcessor(self._processRequirements)
		for result in self._aspect_submit.submitJobs(jobNumList, packageManager.getPackage):
			yield result

	def checkJobs(self, gcID_jobNum_List, stateNotFound):
		if not len(gcID_jobNum_List):
			raise StopIteration
		activity = utils.ActivityLog('checking job status')
		for result in self._aspect_check.checkJobs(gcID_jobNum_List, stateNotFound):
			yield result
		del activity

	def cancelJobs(self, gcID_jobNum_List):
		if not len(gcID_jobNum_List):
			raise StopIteration
		for result in self._aspect_cancel.cancelJobs(gcID_jobNum_List):
			yield result

	def retrieveJobs(self, gcID_jobNum_List, packageManager):
		if not len(gcID_jobNum_List):
			raise StopIteration
		activity = utils.ActivityLog('retrieving job outputs')
		for result in self._aspect_retrieve.retrieveJobs(gcID_jobNum_List, packageManager.getPackage):
			yield result
		del activity
