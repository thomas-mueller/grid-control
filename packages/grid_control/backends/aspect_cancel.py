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

import time
from grid_control import utils
from grid_control.backends.aspect_base import Sandbox_SharedFS, WMS_Aspect
from grid_control.gc_exceptions import RuntimeError
from hpfwk import AbstractError

class WMS_CancelAspect(WMS_Aspect):
	def __init__(self, config, name, oslayer):
		WMS_Aspect.__init__(self, config, name, oslayer,
			utils.getNamedLogger('WMS', name, self, 'Cancel'))

	# Cancel status of jobs and yield (jobNum, gcID)
	def cancelJobs(self, gcID_jobNum_List):
		raise AbstractError


# Chunked cancel operation
class WMS_CancelAspect_Chunked(WMS_CancelAspect):
	def __init__(self, config, name, oslayer):
		WMS_CancelAspect.__init__(self, config, name, oslayer)
		self._chunkInterval = 5

	# Split list of gcID_jobNum tuples into chunks
	def _chunkIter(self, gcID_jobNum_List):
		raise AbstractError

	def cancelJobs(self, gcID_jobNum_List):
		waitFlag = False
		for gcID_jobNum_Chunk in self._chunkIter(gcID_jobNum_List):
			# Delete jobs in groups - with 5 seconds between groups
			if waitFlag and (utils.wait(self._chunkInterval) == False):
				break
			waitFlag = True
			for result in self._cancelChunk(gcID_jobNum_Chunk):
				yield result

	# Cancel a chunk of jobs
	def _cancelChunk(self, gcID_jobNum_Chunk):
		pass


# Uniform chunks
class WMS_CancelAspect_Chunked_Uniform(WMS_CancelAspect_Chunked):
	def __init__(self, config, name, oslayer):
		WMS_CancelAspect_Chunked.__init__(self, config, name, oslayer)
		self._chunksize = 5

	def _chunkIter(self, gcID_jobNum_List):
		chunkOffsets = range(0, len(gcID_jobNum_List), self._chunksize)
		return map(lambda x: gcID_jobNum_List[x:x + self._chunksize], chunkOffsets)


# Shared FileSystem
class WMS_CancelAspect_SharedFS(WMS_CancelAspect):
	def __init__(self, config, name, oslayer):
		WMS_CancelAspect.__init__(self, config, name, oslayer)
		self._sb = Sandbox_SharedFS(config, oslayer)

	def cancelJobs(self, gcID_jobNum_List):
		activity = utils.ActivityLog('cancelling jobs')
		wmsID_gcID_map = self._mapIDs(gcID_jobNum_List)
		proc = self._oslayer.call(*self._cancelArguments(wmsID_gcID_map.keys()))
		if proc.status(10, terminate = True) != 0:
			self._cancelHandleError(proc)
		del activity

		activity = utils.ActivityLog('waiting for jobs to finish')
		time.sleep(5)
		for gcID, jobNum in gcID_jobNum_List:
			(workflowID, backendName, jobToken, wmsID) = self.parseID(gcID)
			path = self._sb.getSandbox(jobNum, jobToken, workflowID)
			if path == None:
				self._log.warning('Sandbox for job %d with gcID "%s" could not be found' % (jobNum, gcID))
				continue
			try:
				self._oslayer.removeDirectory(path)
			except Exception:
				raise RuntimeError('Sandbox for job %d with gcID "%s" could not be deleted' % (jobNum, gcID))
			yield (jobNum, gcID)
		del activity

	def _cancelHandleError(self, proc):
		self._log.log_process_result(proc)

	def _cancelArguments(self, wmsIDs):
		raise AbstractError


# Shared FileSystem, Lambda Argument Function
class WMS_CancelAspect_SharedFS_LAF(WMS_CancelAspect_SharedFS):
	def __init__(self, config, name, oslayer, argPrefix, argFun = lambda x: x, logBlacklist = []):
		WMS_CancelAspect_SharedFS.__init__(self, config, name, oslayer)
		self._argPrefix = argPrefix
		self._argFun = argFun
		self._logBlacklist = logBlacklist

	def _cancelArguments(self, wmsIDs):
		return self._argPrefix + self._argFun(wmsIDs)

	def _cancelHandleError(self, proc):
		self._logProc(proc, blacklist = self._logBlacklist)
