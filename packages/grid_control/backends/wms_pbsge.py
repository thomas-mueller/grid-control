#-#  Copyright 2010-2015 Karlsruhe Institute of Technology
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

import shlex
from grid_control.backends.aspect_submit import StreamMode, WMS_SubmitAspect_Serial_SharedFS
from grid_control.backends.wms import WMS

class PBSGE_SubmitAspect(WMS_SubmitAspect_Serial_SharedFS):
	def __init__(self, config, name, oslayer):
		WMS_SubmitAspect_Serial_SharedFS.__init__(self, config, name, oslayer)
		self._submitExec = oslayer.findExecutable('qsub')
		self._account = config.get('account', '', onChange = None)
		self._shell = config.get('shell', '', onChange = None)
		# Maps with WMS.SOFTWARE tag / WMS.CPUS => submit options
		self._softwareMap = config.getDict('software requirement map', {}, parser = shlex.split, strfun = lambda x: str.join(' ', x), onChange = None)
		self._cpuMap = config.getDict('cpu requirement map', {}, parser = shlex.split, strfun = lambda x: str.join(' ', x), onChange = None)

	# Common submit arguments - called by _submitArguments
	def _submitArgumentsCommon(self, jobName, jobRequirements, jobFile, userOpts, reqMap):
		# Job name
		params = [self._submitExec] + userOpts + ['-N', jobName]
		# Job accounting
		if self._account:
			params.extend(['-P', self._account])
		# Job shell
		if self._shell:
			params.extend(['-S', self._shell])
		# Process job requirements
		softwareMatch = False
		for softwareReq in self._softwareMap[1]: # loop over ordered keys
			if str(reqs.get(WMS.SOFTWARE)).startswith(softwareReq):
				params.extend(self._softwareMap[0][softwareReq])
				softwareMatch = True
		if (None in self._softwareMap[0]) and not softwareMatch:
			params.extend(self._softwareMap[0][None])
		# Apply cpu requirement:
		cpus = self._getRequirementValue(jobRequirements, WMS.CPUS)
		if not cpus:
			params.extend(self._cpuMap[0].get(None, []))
		else:
			params.extend(self._cpuMap[0].get(str(cpus), []))
		# Apply requirement map
		for req in reqMap:
			value = self._getRequirementValue(jobRequirements, req)
			if value > 0:
				optKey, optFmt = reqMap[req]
				params.extend(['-l', '%s=%s' % (optKey, optFmt(value))])
		# Sandbox, IO paths
		if self._streamMode == StreamMode.direct:
			stdout = self._oslayer.path.join(self._oslayer.path.dirname(jobFile), 'gc.stdout')
			stderr = self._oslayer.path.join(self._oslayer.path.dirname(jobFile), 'gc.stderr')
			params.extend(['-o', stdout, '-e', stderr])
		else:
			params.extend(['-o', '/dev/null', '-e', '/dev/null'])
		return params + [jobFile]
