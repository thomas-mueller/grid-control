#-#  Copyright 2015 Karlsruhe Institute of Technology
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
from grid_control import utils
from grid_control.backends.aspect_base import Sandbox_SharedFS, WMS_Aspect
from hpfwk import AbstractError

# StreamMode specifies how stdout and stderr are handled by the backend
#  * direct - no redirection by the script, streams handled by the batch system
#  * landingzone - write stdout/stderr into the landing zone
#  * tmp - write stdout/stderr into the node tmp dir and put into the landing zone at the end
StreamMode = utils.makeEnum(['direct', 'landingzone', 'tmp'])

class WMS_SubmitAspect(WMS_Aspect):
	def __init__(self, config, name, oslayer):
		WMS_Aspect.__init__(self, config, name, oslayer,
			utils.getNamedLogger('WMS', name, self, 'Submit'))

	# Return Result object with jobsPerChunk and chunks_per_step
	def submitChunkInfo(self):
		raise AbstractError

	# Submit jobs from packageMaker(jobNumList) and yield (jobNum, WMS ID, other data)
	def submitJobs(self, jobNumList, packageMaker):
		raise AbstractError

	def _getRequirementValue(self, requirementList, query, default = None):
		for (req, value) in requirementList:
			if query == req:
				return value
		return default


class WMS_SubmitAspect_Chunked(WMS_SubmitAspect):
	pass # TODO


class WMS_SubmitAspect_Serial(WMS_SubmitAspect):
	def __init__(self, config, name, oslayer):
		WMS_SubmitAspect.__init__(self, config, name, oslayer)
		self._chunksPerStep = config.getInt('chunks per run', 100)
		self._streamMode = config.getEnum('stream mode', StreamMode, StreamMode.direct)
		self._submitOpts = shlex.split(config.get('submit options', '', onChange = None))

	def submitChunkInfo(self):
		return utils.Result(jobsPerChunk = 1, chunksPerStep = self._chunksPerStep)

	def submitJobs(self, jobNumList, packageMaker):
		for jobNum in jobNumList:
			if utils.abort():
				raise StopIteration
			package = packageMaker(jobNum, streams = self._streamMode)
			jobToken = ('%02x'*8) % tuple(map(ord, self._oslayer.urandom(8)))
			yield self._submitJob(jobNum, jobToken, package)

	# Call submit command and return (jobNum, gcID, data)
	def _submitCall(self, jobNum, jobToken, workflowID, jobName, jobRequirements, jobFile, timeout = 5):
		args = self._submitArguments(jobName, jobRequirements, jobFile, self._submitOpts)
		proc = self._oslayer.call(*args)

		if proc.status(timeout) == None:
			self._log.warning('Submission command %r did not return after %d seconds' % (proc.get_call(), timeout))
		if proc.status(0, terminate = True) != 0:
			self._log.log_process_result(proc)
			return (jobNum, None, {})

		(isValid, wmsID) = self._submitParse(proc)
		if not isValid:
			self._log.warning('Submission command %r did not yield valid process id: %r' % (proc.get_call(), wmsID))
			self._log.log_process_result(proc)
			gcID = None
		else:
			gcID = self._createID(workflowID, jobToken, wmsID)
		return (jobNum, gcID, {'job_file': jobFile})

	# Submit single job and yield (jobNum, WMS ID, other data) - should use _submitCall()
	def _submitJob(self, jobNum, jobToken, package):
		raise AbstractError

	# Get list with submission arguments
	def _submitArguments(self, jobName, jobRequirements, jobFile, userOpts):
		raise AbstractError

	# Parse and validate result from submit process and return (isValid, gcID)
	def _submitParse(self, proc):
		raise AbstractError


# Serial + Shared FS between submit host and worker node
class WMS_SubmitAspect_Serial_SharedFS(WMS_SubmitAspect_Serial):
	def __init__(self, config, name, oslayer):
		WMS_SubmitAspect_Serial.__init__(self, config, name, oslayer)
		self._sb = Sandbox_SharedFS(config, oslayer)

	# Submit job and yield (jobNum, WMS ID, other data)
	def _submitJob(self, jobNum, jobToken, package):
		sandboxDir = self._sb.getSandbox(jobNum, jobToken, package.taskID)
		self._oslayer.ensureDirectoryExists(sandboxDir)
		sandboxScript = self._oslayer.path.join(sandboxDir, '%s.%d.sh' % (package.taskID, jobNum))
		self._oslayer.writeExecutable(sandboxScript, package.script(sandboxDir))

		return self._submitCall(jobNum, jobToken, package.taskID,
			package.jobName, package.requirements, sandboxScript)
