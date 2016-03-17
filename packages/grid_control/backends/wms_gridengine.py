#-#  Copyright 2009-2016 Karlsruhe Institute of Technology
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

import xml.dom.minidom
from grid_control import utils
from grid_control.backends.aspect_broker import WMS_RequirementAspect_BrokerDiscovery
from grid_control.backends.aspect_cancel import WMS_CancelAspect_SharedFS_LAF
from grid_control.backends.aspect_check import WMS_CheckAspect_Serial
from grid_control.backends.aspect_retrieve import WMS_RetrieveAspect_Serial_SharedFS
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.backends.wms_pbsge import PBSGE_SubmitAspect
from grid_control.config import ConfigError
from grid_control.job_db import Job
from python_compat import set

class GE_SubmitAspect(PBSGE_SubmitAspect):
	def _submitArguments(self, jobName, jobRequirements, jobFile, userOpts):
		# Restart jobs = no
		params = ['-r', 'n']
		# Job requirements
		queue = self._getRequirementValue(jobRequirements, WMS.QUEUES, [''])[0]
		nodes = self._getRequirementValue(jobRequirements, WMS.SITES, None)
		if not nodes and queue:
			params.extend(['-q', queue])
		elif nodes and queue:
			params.extend(['-q', str.join(',', map(lambda node: '%s@%s' % (queue, node), nodes))])
		elif nodes:
			raise ConfigError('Please also specify queue when selecting nodes!')

		timeStr = lambda s: '%02d:%02d:%02d' % (s / 3600, (s / 60) % 60, s % 60)
		reqMap = {
			WMS.MEMORY: ('h_vmem', lambda m: '%dM' % m),
			WMS.WALLTIME: ('s_rt', timeStr),
			WMS.CPUTIME: ('h_cpu', timeStr)
		}
		return self._submitArgumentsCommon(jobName, jobRequirements, jobFile, userOpts + params, reqMap)

	def _submitParse(self, proc):
		# Your job 424992 ("test.sh") has been submitted
		wmsID = proc.read_stdout(10).split()[2].strip()
		return (wmsID.isdigit(), wmsID)


class GE_CheckAspect(WMS_CheckAspect_Serial):
	def __init__(self, config, name, oslayer):
		WMS_CheckAspect_Serial.__init__(self, config, name, oslayer)
		self._checkExec = oslayer.findExecutable('qstat')
		self._user = config.get('user', oslayer.environ.get('LOGNAME', ''), onChange = None)

	def _checkArguments(self, wmsIDs):
		result = [self._checkExec, '-xml']
		if self._user:
			result.extend(['-u', self._user])
		return result

	def _checkParse(self, proc):
		proc.status(10)
		try:
			dom = xml.dom.minidom.parseString(proc.read_stdout_log().strip())
		except Exception:
			raise BackendError("Couldn't parse qstat XML output!")
		for jobentry in dom.getElementsByTagName('job_list'):
			jobinfo = {}
			try:
				for node in jobentry.childNodes:
					if node.nodeType != xml.dom.minidom.Node.ELEMENT_NODE:
						continue
					if node.hasChildNodes():
						jobinfo[str(node.nodeName)] = str(node.childNodes[0].nodeValue)
				if 'queue_name' in jobinfo:
					queue, node = jobinfo['queue_name'].split('@')
					jobinfo['dest'] = '%s/%s' % (node, queue)
			except Exception:
				raise BackendError('Error reading job info:\n%s' % jobentry.toxml())
			yield (jobinfo['JB_job_number'], jobinfo['status'], jobinfo)

	def _checkParseState(self, state):
		if True in map(lambda x: x in state, ['h', 's', 'S', 'T', 'w']):
			return Job.QUEUED
		if True in map(lambda x: x in state, ['r', 't']):
			return Job.RUNNING
		return Job.READY


class GE_BrokerAspect_Queues(WMS_RequirementAspect_BrokerDiscovery):
	def __init__(self, config, name, oslayer):
		WMS_RequirementAspect_BrokerDiscovery.__init__(self, config, name, oslayer, 'queues', 'UserBroker', WMS.QUEUES)
		self._configExec = oslayer.findExecutable('qconf')

	def discover(self):
		result = {}
		tags = ['h_vmem', 'h_cpu', 's_rt']
		reqs = dict(zip(tags, [WMS.MEMORY, WMS.CPUTIME, WMS.WALLTIME]))
		parser = dict(zip(tags, [int, utils.parseTime, utils.parseTime]))

		for queue in map(str.strip, self._oslayer.call(self._configExec, '-sql').iter_stdout(10)):
			result[queue] = dict()
			for line in self._oslayer.call(self._configExec, '-sq %s' % queue).iter_stdout(10):
				attr, value = map(str.strip, line.split(' ', 1))
				if (attr in tags) and (value != 'INFINITY'):
					result[queue][reqs[attr]] = parser[attr](value)
		return result


class GE_BrokerAspect_Nodes(WMS_RequirementAspect_BrokerDiscovery):
	def __init__(self, config, name, oslayer):
		WMS_RequirementAspect_BrokerDiscovery.__init__(self, config, name, oslayer, 'nodes', 'UserBroker', WMS.SITES)
		self._configExec = oslayer.findExecutable('qconf')

	def discover(self):
		(result, active) = (set(), False)
		for group in map(str.strip, self._oslayer.call(self._configExec, '-shgrpl').iter_stdout(10)):
			result.add(group)
			for host in self._oslayer.call(self._configExec, '-shgrp_resolved', group).iter_stdout(10):
				result.update(host.split())
		if len(result) > 0:
			return list(result)


class GridEngine(LocalWMS):
	alias = ['SGE', 'UGE', 'OGE']
	configSections = LocalWMS.configSections + ['GridEngine'] + alias

	def __init__(self, config, name, oslayer):
		execDict = oslayer.findExecutables(['qsub', 'qstat', 'qdel', 'qconf'], first = True)
		LocalWMS.__init__(self, config, name, oslayer,
			submit = GE_SubmitAspect(config, name, oslayer),
			check = GE_CheckAspect(config, name, oslayer),
			cancel = WMS_CancelAspect_SharedFS_LAF(config, name, oslayer,
				argPrefix = [execDict['qdel']], argFun = lambda wmsIDs: [str.join(',', wmsIDs)],
				logBlacklist = ['does not exist']),
			retrieve = WMS_RetrieveAspect_Serial_SharedFS(config, name, oslayer),
			brokers = [
				GE_BrokerAspect_Queues(config, name, oslayer),
				GE_BrokerAspect_Nodes(config, name, oslayer)])
