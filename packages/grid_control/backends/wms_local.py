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

from grid_control import utils
from grid_control.backends.aspect_broker import WMS_RequirementAspect
from grid_control.backends.wms import BasicWMS, WMS
from grid_control.gc_exceptions import InstallationError
from grid_control.utils.file_objects import VirtualFile
from python_compat import itemgetter

class Local_RequirementAspect(WMS_RequirementAspect):
	def __init__(self, config, name, oslayer):
		WMS_RequirementAspect.__init__(self, config, name, oslayer)
		self._memory = config.getInt('memory', -1, onChange = None)

	# Process requirements
	def processRequirements(self, reqs):
		if (self._memory > 0) and (dict(reqs).get(WMS.MEMORY, 0) < self._memory):
			# local jobs need higher (more realistic) memory requirements
			reqs = filter(lambda (k, v): k != WMS.MEMORY, reqs)
			reqs.append((WMS.MEMORY, self._memory))
		return reqs


class LocalWMS(BasicWMS):
	configSections = BasicWMS.configSections + ['local']

	def __init__(self, config, name, oslayer, submit, check, cancel, retrieve, brokers):
		config.setInt('wait idle', 20)
		config.setInt('wait work', 5)
		BasicWMS.__init__(self, config, name, oslayer, submit, check, cancel, retrieve,
			brokers + [Local_RequirementAspect(config, name, oslayer)])
		#self.scratchPath = config.getList('scratch path', ['TMPDIR', '/tmp'], onChange = True)

	def _getSandboxFiles(self, module, monitor, smList):
		files = BasicWMS._getSandboxFiles(self, module, monitor, smList)
		for idx, authFile in enumerate(self._access.getAuthFiles()):
			files.append(VirtualFile(('_proxy.dat.%d' % idx).replace('.0', ''), open(authFile, 'r').read()))
		return files
utils.makeEnum(['QUEUES', 'NODES'], LocalWMS)


class Local(WMS):
	def __new__(cls, config, name, oslayer):
		markers = [('qping', 'GridEngine'), ('pbs-config', 'PBS'), ('pbsnodes', 'PBS'),
			('qsub', 'GridEngine'), ('bsub', 'LSF'), ('job_slurm', 'SLURM')]
		located = oslayer.findExecutables(map(itemgetter(0), markers), raiseNotFound = False)
		for cmd, wms in markers:
			if located[cmd]:
				return WMS.getInstance(wms, config, name, oslayer)
		raise InstallationError('Unable to find local backend - please specify wanted backend manually!')
