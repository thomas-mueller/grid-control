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

from python_compat import set, sorted

class JobBundle:
	def __init__(self, config, task, monitor):
		self._bundleRoot = config.getWorkPath('submit')
		self._transferSE = config.getState('init', detail = 'storage')
		task.validateVariables()

		self.outputFiles = map(lambda (d, s, t): t, self._getSandboxFilesOut(task)) # HACK

		# Bundle sandbox tar file
		sandbox = self._getSandboxName(task)
		utils.ensureDirExists(os.path.dirname(sandbox), 'sandbox directory')
		if not os.path.exists(sandbox) or self.config.getState(detail = 'sandbox'):
			pass



	def getSubmitBundle(self, jobNum, wms):
		# Transfer common SE files
		wms.smSEIn.addFiles(map(lambda (d, s, t): t, task.getSEInFiles())) # add task SE files to SM
		if self._transferSE:
			wms.smSEIn.doTransfer(task.getSEInFiles())
			self._transferSE = False

		bundlePath = os.path.join(self._bundleRoot, '%.%s.sh' % (self._task.taskID, str(jobNum)))
		utils.genTarball(BundlePath, submitFiles)

		return utils.Result(
			executable = "",
			requirements = self._task.getRequirements(jobNum),
			inputFiles = [],
			outputFiles = [])

	def _getSandboxFilesIn(self, task):
		return [
			('GC Runtime', utils.pathShare('gc-run.sh'), 'gc-run.sh'),
			('GC Runtime library', utils.pathShare('gc-run.lib'), 'gc-run.lib'),
			('GC Sandbox', self.config.getWorkPath('files', task.taskID, self.wmsName, 'gc-sandbox.tar.gz'), 'gc-sandbox.tar.gz'),
		]


	def _getSandboxFilesOut(self, task):
		return [
			('GC Wrapper - stdout', 'gc.stdout', 'gc.stdout'),
			('GC Wrapper - stderr', 'gc.stderr', 'gc.stderr'),
			('GC Job summary', 'job.info', 'job.info'),
		] + map(lambda fn: ('Task output', fn, fn), )


	def _getSandboxFiles(self, task, monitor, smList):

		# Prepare all input files
		depList = set(itertools.chain(*map(lambda x: x.getDependencies(), [task] + smList)))
		depPaths = map(lambda pkg: utils.pathShare('', pkg = pkg), os.listdir(utils.pathGC('packages')))
		depFiles = map(lambda dep: utils.resolvePath('env.%s.sh' % dep, depPaths), depList)

		envDictList = list(itertools.chain(map(lambda x: x.getTaskConfig(), [monitor, task] + smList)))
		envDict = utils.mergeDicts(taskEnv)
		envDict.update({'GC_DEPFILES': str.join(' ', depList),
			'GC_USERNAME': wms.getAccessToken().getUsername(), 'GC_WMS_NAME': wms.getObjectName()})

		jobEnv = utils.mergeDicts([task.getJobConfig(jobNum), extras])
		jobEnv['GC_ARGS'] = task.getJobArguments(jobNum).strip()
		content = utils.DictFormat(escapeString = True).format(jobEnv, format = 'export %s%s%s\n')

		taskConfig = sorted(utils.DictFormat(escapeString = True).format())
		varMappingDict = dict(zip(monitor.getTaskConfig().keys(), monitor.getTaskConfig().keys()))
		varMappingDict.update(task.getVarMapping())
		varMapping = sorted(utils.DictFormat(delimeter = ' ').format(varMappingDict, format = '%s%s%s\n'))
		# Resolve wildcards in task input files
		return list(itertools.chain(monitor.getFiles(), depFiles,
			[VirtualFile('_config.sh', taskConfig), VirtualFile('_varmap.dat', varMapping)]))


	def _writeJobConfig(self, cfgPath, jobNum, task, extras = {}):
		try:
			utils.safeWrite(open(cfgPath, 'w'), content)
		except:
			raise RethrowError('Could not write job config data to %s.' % cfgPath)
