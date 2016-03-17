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

import os, sys

glite_path = os.environ.get('GLITE_WMS_LOCATION', os.environ.get('GLITE_LOCATION', ''))
for p in ['lib', 'lib64', os.path.join('lib', 'python'), os.path.join('lib64', 'python')]:
	sys.path.append(os.path.join(glite_path, p))

from grid_control import utils
from grid_control.backends.aspect_check import WMS_CheckAspect
from grid_control.backends.wms import BackendError
from grid_control.backends.wms_glitewms import GliteWMS

try: # gLite 3.2
	import wmsui_api
	glStates = wmsui_api.states_names
	def getStatusDirect(wmsId):
		try: # new parameter json
			jobStatus = wmsui_api.getStatus(wmsui_api.getJobIdfromList(None, [wmsId])[0], 0)
		except:
			jobStatus = wmsui_api.getStatus(wmsui_api.getJobIdfromList([wmsId])[0], 0)
		return map(lambda name: (name.lower(), jobStatus.getAttribute(glStates.index(name))), glStates)
except Exception: # gLite 3.1
	try:
		from glite_wmsui_LbWrapper import Status
		import Job
		wrStatus = Status()
		jobStatus = Job.JobStatus(wrStatus)
		def getStatusDirect(wmsId):
			wrStatus.getStatus(wmsId, 0)
			err, apiMsg = wrStatus.get_error()
			if err:
				raise BackendError(apiMsg)
			info = wrStatus.loadStatus()
			return zip(map(str.lower, jobStatus.states_names), info[0:jobStatus.ATTR_MAX])
	except Exception:
		getStatusDirect = None

class GliteWMSDirect_CheckAspect(WMS_CheckAspect):
	# Check status of jobs and yield (jobNum, gcID, jobStatus, jobInfos)
	def checkJobs(self, gcID_jobNum_List, stateNotFound):
		wmsID_gcID_Map = self._mapIDs(gcID_jobNum_List)
		gcID_jobNum_Map = dict(gcID_jobNum_List)

		for wmsID in wmsID_gcID_Map:
			data = utils.filterDict(dict(getStatusDirect(wmsID)), vF = lambda v: (v != '') and (v != '0'))
			data['dest'] = data.get('destination', 'N/A')
			gcID = wmsID_gcID_Map.get(data['jobid'])
			if gcID:
				yield (gcID_jobNum_Map.pop(gcID), gcID, Grid_CheckAspect.statusMap.get(data['status'].lower(), stateNotFound), data)
			if utils.abort():
				break

		for gcID in gcID_jobNum_Map: # If status check didn't give results, assume the job has finished
			yield (gcID_jobNum_Map[gcID], gcID, stateNotFound, {})

class GliteWMSDirect(GliteWMS):
	def __init__(self, config, name, oslayer):
		check = None
		if getStatusDirect:
			check = GliteWMSDirect_CheckAspect(config, name, oslayer)
		GliteWMS.__init__(self, config, name, oslayer, check)
