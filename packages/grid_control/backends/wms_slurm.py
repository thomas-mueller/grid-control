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

from grid_control.backends.aspect_broker import WMS_RequirementAspect
from grid_control.backends.aspect_cancel import WMS_CancelAspect_SharedFS_LAF
from grid_control.backends.aspect_check import WMS_CheckAspect_Serial_SSP
from grid_control.backends.aspect_retrieve import WMS_RetrieveAspect_Serial_SharedFS
from grid_control.backends.aspect_submit import StreamMode, WMS_SubmitAspect_Serial_SharedFS
from grid_control.backends.wms import WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job

class JMS_SubmitAspect(WMS_SubmitAspect_Serial_SharedFS):
	def __init__(self, config, name, oslayer, submitExec):
		WMS_SubmitAspect_Serial_SharedFS.__init__(self, config, name, oslayer)
		self._submitExec = submitExec

	def _submitArguments(self, jobName, jobRequirements, jobFile, userOpts):
		# Job name
		params = [self._submitExec, '-J', jobName]
		# Job requirements
		params.extend(['-p', self._getRequirementValue(jobRequirements, WMS.CPUS, 1)])
		queue = self._getRequirementValue(jobRequirements, WMS.QUEUES, [''])[0]
		if queue:
			params.extend(['-c', queue])
		walltime = self._getRequirementValue(jobRequirements, WMS.WALLTIME)
		if walltime > 0:
			params.extend(['-T', (walltime + 59) / 60])
		cputime = self._getRequirementValue(jobRequirements, WMS.CPUTIME)
		if cputime > 0:
			params.extend(['-t', (cputime + 59) / 60])
		memory = self._getRequirementValue(jobRequirements, WMS.MEMORY)
		if memory > 0:
			params.extend(['-m', memory])
		# IO paths
		if self._streamMode == StreamMode.direct:
			stdout = self._oslayer.path.join(self._oslayer.path.dirname(jobFile), 'gc.stdout')
			stderr = self._oslayer.path.join(self._oslayer.path.dirname(jobFile), 'gc.stderr')
			params.extend(['-o', stdout, '-e', stderr])
		else:
			params.extend(['-o', '/dev/null', '-e', '/dev/null'])
		return params + [jobFile]

	def _submitParse(self, proc):
		# job_submit: Job 121195 has been submitted.
		wmsID = proc.read_stdout(10).split()[2].strip()
		return (wmsID.isdigit(), wmsID)


class JMS_CheckAspect(WMS_CheckAspect_Serial_SSP):
	def __init__(self, config, name, oslayer, checkExec):
		statusMap = {'s': Job.QUEUED, 'r': Job.RUNNING, 'CG': Job.DONE, 'w': Job.WAITING}
		WMS_CheckAspect_Serial_SSP.__init__(self, config, name, oslayer, statusMap)
		self._checkExec = checkExec

	def _checkArguments(self, wmsIDs):
		return [self._checkExec, '-l'] + wmsIDs

	def _checkParse(self, proc):
		tmpHead = ['id', 'user', 'group', 'job_name', 'queue', 'partition',
			'nodes', 'cpu_time', 'wall_time', 'memory', 'queue_time', 'status']
		for jobline in str.join('', proc.iter_stdout(10)).split('\n')[2:]:
			if jobline == '':
				continue
			tmp = map(lambda x: x.strip('\x1b(B'), jobline.replace('\x1b[m', '').split())
			jobinfo = dict(zip(tmpHead, tmp[:12]))
			if len(tmp) > 12:
				jobinfo['start_time'] = tmp[12]
			if len(tmp) > 13:
				jobinfo['kill_time'] = tmp[13]
			if len(tmp) > 14:
				jobinfo['dest_hosts'] = tmp[14]
				jobinfo['dest'] = '%s.localhost/%s' % (jobinfo['dest_hosts'], jobinfo['queue'])
			yield (jobinfo['id'], jobinfo['status'], jobinfo)

	def _checkHandleError(self, proc):
		self._logProc(proc, blacklist = ['not in queue', 'tput: No value for $TERM'])


class JMS(LocalWMS):
	alias = ['SLURM']
	configSections = LocalWMS.configSections + ['JMS']

	def __init__(self, config, name, oslayer):
		execDict = oslayer.findExecutables(['job_submit', 'job_queue', 'job_cancel'], first = True)
		LocalWMS.__init__(self, config, name, oslayer,
			submit = JMS_SubmitAspect(config, name, oslayer, execDict['job_submit']),
			check = JMS_CheckAspect(config, name, oslayer, execDict['job_queue']),
			cancel = WMS_CancelAspect_SharedFS_LAF(config, name, oslayer,
				argPrefix = [execDict['job_cancel']], logBlacklist = ['Invalid job id specified']),
			retrieve = WMS_RetrieveAspect_Serial_SharedFS(config, name, oslayer),
			brokers = [WMS_RequirementAspect(config, name, oslayer, 'queues', 'UserBroker', WMS.QUEUES)])
