# | Copyright 2016 Karlsruhe Institute of Technology
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

import time
from grid_control.job_db import Job
from grid_control.report import Report
from grid_control.utils.parsing import parseStr
from python_compat import lmap

class BackendReport(Report):
	def __init__(self, jobDB, task, jobs = None, configString = ''):
		Report.__init__(self, jobDB, task, jobs, configString)
		levelMap = {'wms': 2, 'endpoint': 3, 'site': 4, 'queue': 5}
		self._idxList = lmap(lambda x: levelMap[x.lower()], configString.split())

	def _getReportInfos(self):
		result = []
		defaultJob = Job()
		t_now = time.time()
		for jobNum in self._jobs:
			jobObj = self._jobDB.get(jobNum, defaultJob)
			runtime = parseStr(jobObj.get('runtime'), int, 0)
			for attempt in jobObj.history:
				if (attempt == jobObj.attempt) and (jobObj.state == Job.SUCCESS):
					time_info = runtime
				elif attempt == jobObj.attempt:
					time_info = t_now - float(jobObj.submitted)
				if (attempt == jobObj.attempt - 1) and (jobObj.state != Job.SUCCESS):
					time_info = runtime
				else:
					time_info = 0
				dest = jobObj.history[attempt]
				if dest == 'N/A':
					dest_info = [dest]
				else:
					dest_info = dest.split('/')
				wmsName = jobObj.wmsId.split('.')[1]
				endpoint = 'N/A'
				if 'http:' in jobObj.wmsId:
					endpoint = jobObj.wmsId.split(':')[1].split('/')[0]
				result.append([jobObj.state, time_info, wmsName, endpoint] + dest_info)
		return result

	def display(self):
		overview = self._getReportInfos()
		def fillDict(result, items, idx_list = None, indent = 0):
			if not idx_list:
				for entry in items:
					result.setdefault(entry[0], []).append(entry[1])
				return result
			def getClassKey(entry):
				idx = idx_list[0]
				if idx < len(entry):
					return entry[idx]
				return 'N/A'
			classMap = {}
			for entry in items:
				classMap.setdefault(getClassKey(entry), []).append(entry)
			tmp = {}
			for classKey in classMap:
				childInfo = fillDict(result.setdefault(classKey, {}), classMap[classKey], idx_list[1:], indent + 1)
				for key in childInfo:
					tmp.setdefault(key, []).extend(childInfo[key])
			result[None] = tmp
			return tmp
		displayDict = {}
		fillDict(displayDict, overview, self._idxList)
		return 0
