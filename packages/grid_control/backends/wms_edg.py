#-#  Copyright 2007-2016 Karlsruhe Institute of Technology
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

from grid_control.backends.lcg import LCG_RequirementAspect_CE, LCG_RequirementAspect_Sites
from grid_control.backends.wms_grid import GridWMS, Grid_SubmitAspect

class EDG_SubmitAspect(Grid_SubmitAspect):
	def _formatRequirements_Storage(self, seList):
		fmt = lambda se: '(target.GlueSEUniqueID == %s)' % self._jdlWriter.format(se)
		if seList:
			return 'anyMatch(other.storage.CloseSEs, ' + str.join(' || ', map(fmt, seList)) + ')'

	# Get list with submission arguments - jobScriptPath points to the jdl
	def _submitArguments(self, jobName, jobRequirements, jobFile, userOpts):
		result = [self._submitExec, '--noint', '--debug', '--logfile', '/dev/stderr']
		if self._configFile:
			result.extend(['--config-vo', self._configFile])
		ceList = self._getRequirementValue(jobRequirements, GridWMS.CE, [])
		if ceList:
			result.extend(['-r', ceList[0]])
		return result + [jobFile]


class EuropeanDataGrid(GridWMS):
	alias = ['EDG', 'LCG']

	def __init__(self, config, name, oslayer):
		GridWMS.__init__(self, config, name, oslayer,
			submit = EDG_SubmitAspect(config, name, oslayer, oslayer.findExecutable('edg-job-submit')),
			submitExec = 'edg-job-submit', checkExec = 'edg-job-status',
			cancelExec = 'edg-job-cancel', retrieveExec = 'edg-job-get-output',
			brokers = [
				LCG_RequirementAspect_CE(config, name, oslayer),
				LCG_RequirementAspect_Sites(config, name, oslayer),
			])
