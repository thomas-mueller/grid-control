# | Copyright 2009-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

from grid_control import utils
from grid_control.backends.wms import WMS
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job
from python_compat import izip, lmap

class JMS(LocalWMS):
	configSections = LocalWMS.configSections + ['JMS']
	_statusMap = { 's': Job.QUEUED, 'r': Job.RUNNING, 'CG': Job.DONE, 'w': Job.WAITING }

	def __init__(self, config, name):
		LocalWMS.__init__(self, config, name,
			submitExec = utils.resolveInstallPath('job_submit'),
			statusExec = utils.resolveInstallPath('job_queue'),
			cancelExec = utils.resolveInstallPath('job_cancel'))


	def unknownID(self):
		return 'not in queue !'


	def getJobArguments(self, jobNum, sandbox):
		return repr(sandbox)


	def getSubmitArguments(self, jobNum, jobName, reqs, sandbox, stdout, stderr):
		# Job name
		params = ' -J "%s"' % jobName
		# Job requirements
		if WMS.QUEUES in reqs:
			params += ' -c %s' % reqs[WMS.QUEUES][0]
		if self.checkReq(reqs, WMS.WALLTIME):
			params += ' -T %d' % ((reqs[WMS.WALLTIME] + 59) / 60)
		if self.checkReq(reqs, WMS.CPUTIME):
			params += ' -t %d' % ((reqs[WMS.CPUTIME] + 59) / 60)
		if self.checkReq(reqs, WMS.MEMORY):
			params += ' -m %d' % reqs[WMS.MEMORY]
		# processes and IO paths
		params += ' -p 1 -o "%s" -e "%s"' % (stdout, stderr)
		return params


	def parseSubmitOutput(self, data):
		# job_submit: Job 121195 has been submitted.
		return data.split()[2].strip()


	def parseStatus(self, status):
		tmpHead = ['id', 'user', 'group', 'job_name', 'queue', 'partition',
			'nodes', 'cpu_time', 'wall_time', 'memory', 'queue_time', 'status']
		for jobline in str.join('', list(status)).split('\n')[2:]:
			if jobline == '':
				continue
			tmp = lmap(lambda x: x.strip('\x1b(B'), jobline.replace('\x1b[m', '').split())
			jobinfo = dict(izip(tmpHead, tmp[:12]))
			jobinfo['dest'] = 'N/A'
			if len(tmp) > 12:
				jobinfo['start_time'] = tmp[12]
			if len(tmp) > 13:
				jobinfo['kill_time'] = tmp[13]
			if len(tmp) > 14:
				jobinfo['dest_hosts'] = tmp[14]
				jobinfo['dest'] = '%s.localhost/%s' % (jobinfo['dest_hosts'], jobinfo['queue'])
			yield jobinfo


	def getCheckArguments(self, wmsIds):
		return '-l %s' % str.join(' ', wmsIds)


	def getCancelArguments(self, wmsIds):
		return str.join(' ', wmsIds)
