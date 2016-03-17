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

from grid_control.backends.aspect_cancel import WMS_CancelAspect_SharedFS_LAF
from grid_control.backends.aspect_check import WMS_CheckAspect_Serial
from grid_control.backends.aspect_retrieve import WMS_RetrieveAspect_Serial_SharedFS
from grid_control.backends.aspect_submit import WMS_SubmitAspect_Serial_SharedFS
from grid_control.backends.wms_local import LocalWMS
from grid_control.job_db import Job
from python_compat import next

class Host_SubmitAspect(WMS_SubmitAspect_Serial_SharedFS):
	def __init__(self, config, name, oslayer):
		config.set('stream mode', 'landingzone')
		WMS_SubmitAspect_Serial_SharedFS.__init__(self, config, name, oslayer)

	def _submitArguments(self, jobName, jobRequirements, jobFile, userOpts):
		return ['/bin/sh', '-c', 'trap "" 1; nice %s &> /dev/null & echo $!' % jobFile]

	def _submitParse(self, proc):
		wmsID = proc.read_stdout(10).strip()
		return (wmsID.isdigit(), wmsID)


class Host_CheckAspect(WMS_CheckAspect_Serial):
	def _checkArguments(self, wmsIDs):
		return ['ps', 'wwup'] + wmsIDs

	def _checkParse(self, proc):
		status = proc.iter_stdout(10)
		head = map(lambda x: x.strip('%').lower(), next(status, '').split())
		for entry in map(str.strip, status):
			jobinfo = dict(zip(head, filter(lambda x: x != '', entry.split(None, len(head) - 1))))
			jobinfo['dest'] = 'localhost/localqueue'
			yield (jobinfo.get('pid'), 'R', jobinfo)

	def _checkParseState(self, state):
		return Job.RUNNING

	def _checkHandleError(self, proc):
		pass


class Host(LocalWMS):
	alias = ['Localhost']
	configSections = LocalWMS.configSections + ['Host']

	def __init__(self, config, name, oslayer):
		LocalWMS.__init__(self, config, name, oslayer,
			submit = Host_SubmitAspect(config, name, oslayer),
			check = Host_CheckAspect(config, name, oslayer),
			cancel = WMS_CancelAspect_SharedFS_LAF(config, name, oslayer,
				argPrefix = ['kill', '-9'], logBlacklist = ['No such process']),
			retrieve = WMS_RetrieveAspect_Serial_SharedFS(config, name, oslayer),
			brokers = [])
