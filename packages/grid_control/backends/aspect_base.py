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

class WMS_Aspect(object):
	def __init__(self, config, name, oslayer, log):
		self._name = name
		self._oslayer = oslayer
		self._log = log
		self._access = None

	def setAccess(self, token):
		self._access = token

	# Create ID - WMSID-<workflow ID>.<backend ID>.<job token>.<wms ID>
	def _createID(self, workflowID, jobToken, wmsID):
		return 'WMSID-%s.%s.%s.%s' % (workflowID, self._name, jobToken, wmsID)

	# Split ID - returns tuple with (workflowID, backendName, jobToken, wmsID)
	def parseID(gcID):
		if gcID.startswith('WMSID-'):
			return tuple(gcID[6:].split('.', 3))
		# legacy wms ID support
		elif gcID.startswith('WMSID.'):
			(prefix, backendName, wmsID) = gcID.split('.', 2)
			return (None, backendName, wmsID, wmsID)
		elif gcID.startswith('http'):
			return (None, 'grid', None, gcID)
	parseID = staticmethod(parseID)

	# Return wmsID_gcID_map
	def _mapIDs(gcID_jobNum_List):
		return dict(map(lambda (gcID, jobNum): (WMS_Aspect.parseID(gcID)[3], gcID), gcID_jobNum_List))
	_mapIDs = staticmethod(_mapIDs)

	# log process helpter function for backends:
	#   discardlist => don't log if *any* stderr line is on the discardlist
	#     blacklist => don't log if *all* stderr lines are on the blacklist
	def _logProc(self, proc, blacklist = [], discardlist = []):
		if not blacklist and not discardlist:
			return self._log.log_process_result(proc)
		blacklist = map(str.lower, blacklist)
		discardlist = map(str.lower, discardlist)
		for line in proc.read_stderr_log().splitlines():
			line = line.lower()
			for entry in discardlist:
				if entry in line:
					return
			isBlacklisted = False
			for entry in blacklist:
				if entry in line:
					isBlacklisted = True
					break
			if not isBlacklisted:
				return self._log.log_process_result(proc)


class Sandbox_SharedFS(object):
	def __init__(self, config, oslayer):
		self._oslayer = oslayer
		self._sandboxRoot = oslayer.getPath(config.get('sandbox path', '${PWD}/sandbox'))

	def getSandbox(self, jobNum, jobToken, workflowID):
		return self._oslayer.path.join(self._sandboxRoot, workflowID, 'job_%d_%s' % (jobNum, jobToken))


class Sandbox_SharedFS_Legacy(Sandbox_SharedFS):
	pass # TODO: Implement getSandbox with jobToken == wmsID
