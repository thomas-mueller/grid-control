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

from grid_control import utils
from grid_control.backends.aspect_base import WMS_Aspect
from hpfwk import AbstractError

class WMS_CheckAspect(WMS_Aspect):
	def __init__(self, config, name, oslayer):
		WMS_Aspect.__init__(self, config, name, oslayer,
			utils.getNamedLogger('WMS', name, self, 'Check'))

	# Check status of jobs and yield (jobNum, gcID, jobStatus, jobInfos)
	def checkJobs(self, gcID_jobNum_List, stateNotFound):
		raise AbstractError


class WMS_CheckAspect_Serial(WMS_CheckAspect):
	def checkJobs(self, gcID_jobNum_List, stateNotFound):
		wmsID_gcID_Map = self._mapIDs(gcID_jobNum_List)
		proc = self._oslayer.call(*self._checkArguments(wmsID_gcID_Map.keys()))

		gcID_jobNum_Map = dict(gcID_jobNum_List)
		for (wmsID, raw_status, data) in self._checkParse(proc):
			gcID = wmsID_gcID_Map.get(wmsID)
			if gcID:
				yield (gcID_jobNum_Map.pop(gcID), gcID, self._checkParseState(raw_status), data)

		for gcID in gcID_jobNum_Map: # If status check didn't give results, assume the job has finished
			yield (gcID_jobNum_Map[gcID], gcID, stateNotFound, {})

		if proc.status(0, terminate = True) != 0:
			self._checkHandleError(proc)

	def _checkHandleError(self, proc):
		self._log.log_process_result(proc)

	def _checkArguments(self, wmsIDs):
		raise AbstractError

	def _checkParse(self, proc):
		raise AbstractError

	def _checkParseState(self, state):
		raise AbstractError


# Simple Status Parser
class WMS_CheckAspect_Serial_SSP(WMS_CheckAspect_Serial):
	def __init__(self, config, name, oslayer, statusMap):
		WMS_CheckAspect_Serial.__init__(self, config, name, oslayer)
		self._statusMap = statusMap

	def _checkParseState(self, state):
		return self._statusMap[state]
