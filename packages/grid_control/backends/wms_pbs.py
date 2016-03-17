#-#  Copyright 2009-2015 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.backends.aspect_broker import WMS_RequirementAspect_BrokerDiscovery
from grid_control.backends.aspect_check import WMS_CheckAspect_Serial_SSP
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.backends.wms_pbsge import PBSGE_SubmitAspect
from grid_control.job_db import Job

class PBS_SubmitAspect(PBSGE_SubmitAspect):
	def _submitArguments(self, jobName, jobRequirements, jobFile, userOpts):
		params = []
		# Job requirements
		queue = self._getRequirementValue(jobRequirements, WMS.QUEUES, [''])[0]
		if queue:
			params.extend(['-q', queue])
		nodes = self._getRequirementValue(jobRequirements, WMS.SITES, None)
		if nodes:
			params.extend(['-l', 'host=%s' % str.join('+', nodes)])

		reqMap = { WMS.MEMORY: ('pvmem', lambda m: '%dmb' % m) }
		return self._submitArgumentsCommon(jobName, jobRequirements, jobFile, userOpts + params, reqMap)

	def _submitParse(self, proc):
		# 1667161.ekpplusctl.ekpplus.cluster
		wmsID = proc.read_stdout(10).split('.')[0].strip()
		return (wmsID.isdigit(), wmsID)


class PBS_CheckAspect(WMS_CheckAspect_Serial_SSP):
	def __init__(self, config, name, oslayer, checkExec, makeFQID):
		statusMap = {
			'H': Job.SUBMITTED, 'S': Job.SUBMITTED,
			'W': Job.WAITING,   'Q': Job.QUEUED,
			'R': Job.RUNNING,   'C': Job.DONE,
			'E': Job.DONE,      'T': Job.DONE,
			'fail':	Job.FAILED, 'success': Job.SUCCESS
		}
		WMS_CheckAspect_Serial_SSP.__init__(self, config, name, oslayer, statusMap)
		self._checkExec = checkExec
		self._makeFQID = makeFQID

	def _checkArguments(self, wmsIDs):
		return [self._checkExec, '-f'] + map(self._makeFQID, wmsIDs)

	def _checkParse(self, proc):
		for section in utils.accumulate(proc.iter_stdout(10), '', lambda x, buf: x == '\n'):
			try:
				lines = section.replace('\n\t', '').split('\n')
				jobinfo = utils.DictFormat(' = ').parse(lines[1:])
				if 'exec_host' in jobinfo:
					jobinfo['dest'] = '%s/%s' % (
						jobinfo.get('exec_host').split('/')[0] + '.' + jobinfo.get('server', ''),
						jobinfo.get('queue')
					)
			except Exception:
				raise BackendError('Error reading job info:\n%s' % section)
			yield (lines[0].split(':')[1].split('.')[0].strip(), jobinfo.get('job_state'), jobinfo)


class PBS_BrokerAspect_Queues(WMS_RequirementAspect_BrokerDiscovery):
	def __init__(self, config, name, oslayer):
		WMS_RequirementAspect_BrokerDiscovery.__init__(self, config, name, oslayer, 'queues', 'UserBroker', WMS.QUEUES)
		self._configExec = oslayer.findExecutable('qconf', ['-sql'])

	def discover(self):
		(queues, active) = ({}, False)
		keys = [WMS.MEMORY, WMS.CPUTIME, WMS.WALLTIME]
		parser = dict(zip(keys, [int, utils.parseTime, utils.parseTime]))
		for line in self._oslayer.call(self._configExec, '-q').iter_stdout(10):
			if line.startswith('-'):
				active = True
			elif line.startswith(' '):
				active = False
			elif active:
				fields = map(str.strip, line.split()[:4])
				props = filter(lambda (k, v): not v.startswith('-'), zip(keys, fields[1:]))
				queues[fields[0]] = dict(map(lambda (k, v): (k, parser[k](v)), props))
		return queues


class PBS_BrokerAspect_Nodes(WMS_RequirementAspect_BrokerDiscovery):
	def __init__(self, config, name, oslayer):
		WMS_RequirementAspect_BrokerDiscovery.__init__(self, config, name, oslayer, 'queues', 'UserBroker', WMS.QUEUES)
		self._nodesExec = oslayer.findExecutable('pbsnodes')

	def discover(self):
		result = []
		for line in self._oslayer.call(self._nodesExec).iter_stdout(10):
			if not line.startswith(' ') and len(line) > 1:
				node = line.strip()
			if ('state = ' in line) and ('down' not in line) and ('offline' not in line):
				result.append(node)
		if len(result) > 0:
			return result


class PBS(LocalWMS):
	configSections = LocalWMS.configSections + ['PBS']

	def __init__(self, config, name, oslayer):
		self._server = config.get('server', '', onChange = None)
		makeFQID = lambda wmsID: utils.QM(self._server, '%s.%s' % (wmsID, self._server), wmsID)

		execDict = oslayer.findExecutables(['qsub', 'qstat', 'qdel', 'qconf', 'pbsnodes'], first = True)
		LocalWMS.__init__(self, config, name, oslayer,
			submit = PBS_SubmitAspect(config, name, oslayer, execDict['qsub']),
			check = PBS_CheckAspect(config, name, oslayer, execDict['qstat'], makeFQID),
			cancel = WMS_CancelAspect_SharedFS_LAF(config, name, oslayer,
				argPrefix = [execDict['qdel']], logBlacklist = ['Unknown Job Id']),
			retrieve = WMS_RetrieveAspect_Serial_SharedFS(config, name, oslayer),
			brokers = [
				PBS_BrokerAspect_Queues(config, name, oslayer),
				PBS_BrokerAspect_Nodes(config, name, oslayer)])
