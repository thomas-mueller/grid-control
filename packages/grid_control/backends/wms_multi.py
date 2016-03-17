#-#  Copyright 2012-2016 Karlsruhe Institute of Technology
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

from grid_control.backends.aspect_base import WMS_Aspect
from grid_control.backends.broker import Broker
from grid_control.backends.wms import WMS
from grid_control.utils import Result

# Distribute to WMS according to job id prefix

class MultiWMS(WMS):
	def __init__(self, config, wmsName, wmsList, oslayer):
		WMS.__init__(self, config, wmsName, oslayer)
		# Load submodules and determine timing
		self._defaultWMS = wmsList[0].getInstance(oslayer)
		defaultT = self._defaultWMS.getTimings()
		self._timing = Result(waitOnIdle = defaultT.waitOnIdle, waitBetweenSteps = defaultT.waitBetweenSteps)
		self._wmsMap = {self._defaultWMS.getObjectName().lower(): self._defaultWMS}
		for wmsEntry in wmsList[1:]:
			wmsObj = wmsEntry.getInstance(oslayer)
			self._wmsMap[wmsObj.getObjectName().lower()] = wmsObj
			wmsT = wmsObj.getTimings()
			self._timing.waitOnIdle = max(self._timing.waitOnIdle, wmsT.waitOnIdle)
			self._timing.waitBetweenSteps = max(self._timing.waitBetweenSteps, wmsT.waitBetweenSteps)

		self._brokerWMS = config.getPlugin('wms broker', 'RandomBroker',
			cls = Broker, tags = [self]).getInstance('wms', 'wms', self._wmsMap.keys)


	def getTimings(self):
		return self._timing


	def canSubmit(self, neededTime, canCurrentlySubmit):
		canCurrentlySubmit = self._defaultWMS.canSubmit(neededTime, canCurrentlySubmit)
		for wmsPrefix, wmsObj in self._wmsMap.items():
			canCurrentlySubmit = wmsObj.canSubmit(neededTime, canCurrentlySubmit)
		return canCurrentlySubmit


	def getAccessToken(self, gcID):
		return self._wmsMap.get(WMS_Aspect.parseID(gcID)[0], self._defaultWMS).getAccessToken(gcID)


	def submitJobs(self, jobNumList, packageManager):
		def brokerJobs(jobNum):
			jobReq = self._brokerWMS.brokerAdd(task.getRequirements(jobNum), WMS.BACKEND)
			return dict(jobReq).get(WMS.BACKEND)[0]
		return self._forwardCall(jobNumList, brokerJobs,
			lambda wmsObj, args: wmsObj.submitJobs(args, packageManager))


	def checkJobs(self, gcID_jobNum_List, stateNotFound):
		return self._forwardCall(gcID_jobNum_List,
			lambda (gcID, jobNum): WMS_Aspect.parseID(gcID)[0],
			lambda wmsObj, args: wmsObj.checkJobs(args, stateNotFound))


	def cancelJobs(self, gcID_jobNum_List):
		return self._forwardCall(gcID_jobNum_List,
			lambda (gcID, jobNum): WMS_Aspect.parseID(gcID)[0],
			lambda wmsObj, args: wmsObj.cancelJobs(args))


	def retrieveJobs(self, gcID_jobNum_List, packageManager):
		return self._forwardCall(gcID_jobNum_List,
			lambda (gcID, jobNum): WMS_Aspect.parseID(gcID)[0],
			lambda wmsObj, args: wmsObj.retrieveJobs(args, packageManager))

	# Classify all elements of list "args" using the "assignFun"
	def _getMapBackend2Args(self, args, assignFun):
		result = {}
		for arg in args:
			backendName = assignFun(arg)
			if not backendName:
				backendName = self._defaultWMS.wmsName # assignFun(arg) not valid => default backend
			elif backendName.lower() not in self._wmsMap:
				backendName = self._defaultWMS.wmsName # assignFun(arg) not setup => default backend
			result.setdefault(backendName.lower(), []).append(arg)
		# Return dictionary with mapping: backend names => list of backend arguments
		return result

	# call "callFun" with args
	def _forwardCall(self, args, assignFun, callFun):
		argMap = self._getMapBackend2Args(args, assignFun)
		for wmsPrefix in filter(lambda wmsPrefix: wmsPrefix in argMap, self._wmsMap):
			wms = self._wmsMap[wmsPrefix]
			for result in callFun(wms, argMap[wmsPrefix]):
				yield result
