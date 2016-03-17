#-#  Copyright 2008-2015 Karlsruhe Institute of Technology
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
from grid_control.backends.wms import BackendError, WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job
from python_compat import next

class LSF_SubmitAspect(WMS_SubmitAspect_Serial_SharedFS):
	def __init__(self, config, name, oslayer, submitExec):
		WMS_SubmitAspect_Serial_SharedFS.__init__(self, config, name, oslayer)
		self._submitExec = submitExec

	def _submitArguments(self, jobName, jobRequirements, jobFile, userOpts):
		# Job name
		params = [self._submitExec, '-J', jobName]
		# Job requirements
		queue = self._getRequirementValue(jobRequirements, WMS.QUEUES, [])
		if queue:
			params.extend(['-q', str.join(',', reqs[WMS.QUEUES])])
		walltime = self._getRequirementValue(jobRequirements, WMS.WALLTIME)
		if walltime > 0:
			params.extend(['-W', (walltime + 59) / 60])
		cputime = self._getRequirementValue(jobRequirements, WMS.CPUTIME)
		if cputime > 0:
			params.extend(['-c', (cputime + 59) / 60])
		# IO paths
		if self._streamMode == StreamMode.direct:
			stdout = self._oslayer.path.join(self._oslayer.path.dirname(jobFile), 'gc.stdout')
			stderr = self._oslayer.path.join(self._oslayer.path.dirname(jobFile), 'gc.stderr')
			params.extend(['-o', stdout, '-e', stderr])
		else:
			params.extend(['-o', '/dev/null', '-e', '/dev/null'])
		return params + [jobFile]

	def _submitParse(self, proc):
		# Job <34020017> is submitted to queue <1nh>.
		wmsID = proc.read_stdout(10).split()[1].strip('<>').strip()
		return (wmsID.isdigit(), wmsID)


class LSF_CheckAspect(WMS_CheckAspect_Serial_SSP):
	def __init__(self, config, name, oslayer, checkExec):
		statusMap = {
			'PEND':  Job.QUEUED,  'PSUSP': Job.WAITING,
			'USUSP': Job.WAITING, 'SSUSP': Job.WAITING,
			'RUN':   Job.RUNNING, 'DONE':  Job.DONE,
			'WAIT':  Job.WAITING, 'EXIT':  Job.FAILED,
			# Better options?
			'UNKWN': Job.FAILED,  'ZOMBI': Job.FAILED,
		}
		WMS_CheckAspect_Serial_SSP.__init__(self, config, name, oslayer, statusMap)
		self._checkExec = checkExec

	def _checkArguments(self, wmsIDs):
		return [self._checkExec, '-aw'] + wmsIDs

	def _checkParse(self, proc):
		status = proc.iter_stdout(10)
		next(status)
		tmpHead = ['id', 'user', 'status', 'queue', 'from', 'dest_host', 'job_name']
		for jobline in status:
			if jobline != '':
				try:
					tmp = jobline.split()
					jobinfo = dict(zip(tmpHead, tmp[:7]))
					jobinfo['submit_time'] = str.join(' ', tmp[7:10])
					if jobinfo['dest_host'] != '-':
						jobinfo['dest'] = '%s/%s' % (jobinfo['dest_host'], jobinfo['queue'])
					yield (jobinfo['id'], jobinfo['status'], jobinfo)
				except Exception:
					raise BackendError('Error reading job info:\n%s' % jobline)

	def _checkHandleError(self, proc):
		self._logProc(proc, blacklist = ['is not found'])


class LSF(LocalWMS):
	configSections = LocalWMS.configSections + ['LSF']

	def __init__(self, config, name, oslayer):
		execDict = oslayer.findExecutables(['bsub', 'bjobs', 'bkill'], first = True)
		LocalWMS.__init__(self, config, name, oslayer,
			submit = LSF_SubmitAspect(config, name, oslayer, execDict['bsub']),
			check = LSF_CheckAspect(config, name, oslayer, execDict['bjobs']),
			cancel = WMS_CancelAspect_SharedFS_LAF(config, name, oslayer,
				argPrefix = [execDict['bkill']], logBlacklist = ['No matching job found']),
			retrieve = WMS_RetrieveAspect_Serial_SharedFS(config, name, oslayer),
			brokers = [WMS_RequirementAspect(config, name, oslayer, 'queues', 'UserBroker', WMS.QUEUES)])
