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

import os, time, random
from grid_control import utils
from grid_control.backends.lcg import LCG_RequirementAspect_CE, LCG_RequirementAspect_Sites, LCG_RequirementAspect_WMS
from grid_control.backends.wms_grid import GridWMS, Grid_SubmitAspect
from grid_control.gc_exceptions import RuntimeError
from python_compat import md5

def choice_exp(sample, p = 0.5):
	for x in sample:
		if random.random() < p:
			return x
	return sample[-1]

class GliteWMS_RequirementAspect_WMS():
	def __init__(self, config, oslayer):
		pass

class DiscoverWMS_Lazy: # TODO: Move to broker infrastructure
	def __init__(self, config):
		self.statePath = config.getWorkPath('glitewms.info')
		(self.wms_ok, self.wms_all, self.pingDict, self.pos) = self.loadState()
		self.wms_timeout = {}
		self._exeLCGInfoSites = utils.resolveInstallPath('lcg-infosites')
		self._exeGliteWMSJobListMatch = utils.resolveInstallPath('glite-wms-job-list-match')

	def loadState(self):
		try:
			assert(os.path.exists(self.statePath))
			tmp = utils.PersistentDict(self.statePath, ' = ')
			pingDict = {}
			for wms in tmp:
				isOK, ping, ping_time = tuple(tmp[wms].split(',', 2))
				if utils.parseBool(isOK):
					pingDict[wms] = (utils.parseStr(ping, float), utils.parseStr(ping_time, float, 0))
			return (pingDict.keys(), tmp.keys(), pingDict, 0)
		except Exception:
			return ([], [], {}, None)

	def updateState(self):
		tmp = {}
		for wms in self.wms_all:
			pingentry = self.pingDict.get(wms, (None, 0))
			tmp[wms] = '%r,%s,%s' % (wms in self.wms_ok, pingentry[0], pingentry[1])
		utils.PersistentDict(self.statePath, ' = ').write(tmp)

	def listWMS_all(self):
		result = map(str.strip, utils.LocalProcess(self._exeLCGInfoSites, 'wms').iter_stdout(None))
		random.shuffle(result)
		return result

	def matchSites(self, endpoint):
		checkArgs = ['-a']
		if endpoint:
			checkArgs.extend(['-e', endpoint])
		checkArgs.append(utils.pathShare('null.jdl'))
		proc = utils.LocalProcess(self._exeGliteWMSJobListMatch, *checkArgs)
		result = []
		try:
			for line in proc.iter_stdout(timeout = 3):
				if line.startswith(' - '):
					result.append(line[3:].strip())
		except ProcessTimeout:
			proc.kill()
			self.wms_timeout[endpoint] = self.wms_timeout.get(endpoint, 0) + 1
			if self.wms_timeout.get(endpoint, 0) > 10: # remove endpoints after 10 failures
				self.wms_all.remove(endpoint)
			return []
		return result

	def getSites(self):
		return self.matchSites(self.getWMS())

	def listWMS_good(self):
		if (self.pos == None) or (len(self.wms_all) == 0): # initial discovery
			self.pos = 0
			self.wms_all = self.listWMS_all()
		if self.pos == len(self.wms_all): # self.pos = None => perform rediscovery in next step
			self.pos = 0
		else:
			wms = self.wms_all[self.pos]
			if wms in self.wms_ok:
				self.wms_ok.remove(wms)
			if len(self.matchSites(wms)):
				self.wms_ok.append(wms)
			self.pos += 1
			if self.pos == len(self.wms_all): # mark finished 
				self.wms_ok.append(None)
		return self.wms_ok

	def getWMS(self):
		log = utils.ActivityLog('Discovering available WMS services')
		wms_best_list = []
		for wms in self.listWMS_good():
			if wms == None:
				continue
			ping, pingtime = self.pingDict.get(wms, (None, 0))
			if time.time() - pingtime > 30 * 60: # check every ~30min
				ping = utils.ping_host(wms.split('://')[1].split('/')[0].split(':')[0])
				self.pingDict[wms] = (ping, time.time() + 10 * 60 * random.random()) # 10 min variation
			if ping != None:
				wms_best_list.append((wms, ping))
		wms_best_list.sort(key = lambda (name, ping): ping)
		result = choice_exp(wms_best_list)
		if result != None:
			wms, ping = result # reduce timeout by 5min for chosen wms => re-ping every 6 submits
			self.pingDict[wms] = (ping, self.pingDict[wms][1] + 5*60)
			result = wms
		self.updateState()
		del log
		return result


DelegateMode = utils.makeEnum(['force', 'auto', 'never'])

class GliteWMS_SubmitAspect(Grid_SubmitAspect):
	def __init__(self, config, name, oslayer):
		Grid_SubmitAspect.__init__(self, config, name, oslayer, oslayer.findExecutable('glite-wms-job-submit'))
		self._delegate = {}
		self._delegateExec = None
		self._delegateMode = config.getEnum('proxy delegation', DelegateMode, DelegateMode.force, onChange = None)
		if self._delegateMode != DelegateMode.never:
			self._delegateExec = oslayer.findExecutable('glite-wms-job-delegate-proxy')
		self._useDelegate = config.getBool('try delegate', True, onChange = None)
		self._forceDelegate = config.getBool('force delegate', False, onChange = None)

	def delegateProxy(self, endpoint, timeout = 10):
		activity = utils.ActivityLog('creating delegate proxy for job submission')
		dID = dID = 'GCD' + md5(str(time.time())).hexdigest()[:10]
		args = [self._delegateExec, '-d', dID, '--noint']
		if self._configFile:
			args.extend(['--config', self._configFile])
		if endpoint:
			args.extend(['-e', endpoint])
		proc = self._oslayer.call(*args)
		proc.status(timeout, terminate = True)
		proc_out = proc.read_stdout(0)
		if ('glite-wms-job-delegate-proxy Success' in proc_out) and (dID in proc_out):
			return dID
		self._log.warning('Unable to delegate proxy!')
		self._log.log_process_result(proc)

	# Get list with submission arguments - jobScriptPath points to the jdl
	def _submitArguments(self, jobName, jobRequirements, jobFile, userOpts):
		result = [self._submitExec, '--noint', '--debug', '--logfile', '/dev/stderr']
		if self._configFile:
			result.extend(['--config', self._configFile])
		# WMS endpoint setup
		endpoint = None
		endpointList = self._getRequirementValue(jobRequirements, GridWMS.ENDPOINT)
		if endpointList:
			endpoint = endpointList[0]
		if endpoint:
			result.extend(['-e', endpoint])
		# Proxy delegation
		if (self._delegateMode != DelegateMode.never) and not self._delegate.get(endpoint):
			self._delegate[endpoint] = self.delegateProxy(endpoint)
		if self._delegate[endpoint]:
			result.extend(['-d', self._delegate[endpoint]])
		elif self._delegateMode == DelegateMode.force:
			raise RuntimeError('Unable to delegate proxy!')
		else:
			result.append('-a')
		return result + [jobFile]


class GliteWMS(GridWMS):
	alias = ['grid']
	configSections = GridWMS.configSections + ['glite-wms', 'glitewms'] # backwards compatibility

	def __init__(self, config, name, oslayer, check = None):
		oslayer.findExecutables(['lcg-infosites', 'glite-wms-job-delegate-proxy',
			'glite-wms-job-submit', 'glite-wms-job-status',
			'glite-wms-job-output', 'glite-wms-job-cancel'])
		GridWMS.__init__(self, config, name, oslayer,
			submit = GliteWMS_SubmitAspect(config, name, oslayer),
			check = check,
			submitExec = 'glite-wms-job-submit', checkExec = 'glite-wms-job-status',
			cancelExec = 'glite-wms-job-cancel', retrieveExec = 'glite-wms-job-output',
			brokers = [
				LCG_RequirementAspect_CE(config, name, oslayer),
				LCG_RequirementAspect_Sites(config, name, oslayer),
				LCG_RequirementAspect_WMS(config, name, oslayer),
			])
