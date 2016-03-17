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

import os, time
from grid_control import utils
from grid_control.backends.aspect_base import Sandbox_SharedFS, WMS_Aspect
from hpfwk import AbstractError

class WMS_RetrieveAspect(WMS_Aspect):
	def __init__(self, config, name, oslayer):
		WMS_Aspect.__init__(self, config, name, oslayer,
			utils.getNamedLogger('WMS', name, self, 'Retrieve'))
		self._outputPath = config.getWorkPath('output')
		self._failedPath = config.getWorkPath('failed')

		for dn in [self._outputPath, self._failedPath]:
			utils.ensureDirExists(dn, 'output directory')
		self._preserveFailures = config.getBool('preserve failures', False)

	# Retrieve Jobs from WMS given list of (gcID, jobNum) tuples
	# yields (jobNum, jobExitCode, jobData, localHostOutputDir)
	def retrieveJobs(self, gcID_jobNum_List, packageMaker):
		retrievedJobs = []
		for outputJobNum, outputDir in self._retrieveOutput(gcID_jobNum_List, packageMaker):
			# outputJobNum != None, outputDir == None => Job could not be retrieved
			if outputDir == None:
				if outputJobNum not in retrievedJobs:
					yield (outputJobNum, -1, {}, outputDir)
				continue

			# outputJobNum == None, outputDir != None => Found leftovers of job retrieval
			if outputJobNum == None:
				continue

			# outputJobNum != None, outputDir != None => Job retrieval from WMS was ok
			from grid_control.backends import WMS
			jobInfo = WMS.parseJobInfo(os.path.join(outputDir, 'job.info'))
			if jobInfo:
				(jobNum, jobExitCode, jobData) = jobInfo
				if jobNum != outputJobNum:
					raise RuntimeError('Invalid job id in job file %s' % info)
				outputTarget = os.path.join(self._outputPath, 'job_%d' % jobNum)

				if self._preserveFailures and os.path.exists(outputTarget):
					utils.forceMove(outputTarget,
						os.path.join(self._failedPath, 'job_%d_%d' % (jobNum, time.time())))
				if utils.forceMove(outputDir, outputTarget):
					retrievedJobs.append(outputJobNum)
					# Handle wildcard files:
					if 'GC_WC.tar.gz' in os.listdir(outputTarget):
						wildcardTar = os.path.join(outputTarget, 'GC_WC.tar.gz')
						try:
							tarfile.TarFile.open(wildcardTar, 'r:gz').extractall(outputTarget)
							os.unlink(wildcardTar)
						except:
							self._log.error("Can't unpack output files contained in %s" % wildcardTar)
					yield (jobNum, jobExitCode, jobData, outputTarget)
				else:
					yield (jobNum, -1, {}, outputDir)
				continue

			# Clean empty outputDirs
			for subDir in map(lambda x: x[0], os.walk(outputDir, topdown=False)):
				try:
					os.rmdir(subDir)
				except Exception:
					pass

			if os.path.exists(outputDir):
				# Preserve failed job
				utils.ensureDirExists(self._failedPath, 'failed output directory')
				utils.forceMove(outputDir, os.path.join(self._failedPath, os.path.basename(outputDir)))

			yield (outputJobNum, -1, {}, outputDir)

	# Retrieve Output sandboxes from WMS given a list of (gcID, jobNum) tuples
	# yields (jobNum, localHostOutputDir)
	def _retrieveOutput(self, gcID_jobNum_List, packageMaker):
		raise AbstractError


class WMS_RetrieveAspect_Serial(WMS_RetrieveAspect):
	def __init__(self, config, name, oslayer):
		WMS_RetrieveAspect.__init__(self, config, name, oslayer)
		self._stagePath = config.getWorkPath('stage.%s' % name)
		utils.ensureDirExists(self._stagePath, 'stage directory')

	def _retrieveOutput(self, gcID_jobNum_List, packageMaker):
		for gcID, jobNum in gcID_jobNum_List:
			outputFiles = []
			for (area, fn) in packageMaker(jobNum).output_transfers:
				if area == 'sandbox':
					outputFiles.append(fn)

			stageToken = ('%02x'*8) % tuple(map(ord, self._oslayer.urandom(8)))
			(workflowID, backendName, jobToken, wmsID) = self.parseID(gcID)
			stagePath = os.path.join(self._stagePath, 'job_%d_%s_%s' % (jobNum, jobToken, stageToken))
			(jobNum, submitHostPath) = self._retrieveOutputSingle(jobNum, gcID, outputFiles)
			if submitHostPath:
				try:
					self._oslayer.copyToLocal(submitHostPath, stagePath, outputFiles)
				except Exception:
					pass
				try:
					self._oslayer.removeDirectory(submitHostPath)
				except Exception:
					pass
			if submitHostPath:
				yield (jobNum, stagePath)
			else:
				yield (jobNum, None)

	# Retrieve Output sandboxes from WMS for a single gcID/jobNum
	# yields (jobNum, outputDir)
	def _retrieveOutputSingle(self, jobNum, gcID, outputFiles):
		raise AbstractError


# Serial + Shared file system
class WMS_RetrieveAspect_Serial_SharedFS(WMS_RetrieveAspect_Serial):
	def __init__(self, config, name, oslayer):
		WMS_RetrieveAspect_Serial.__init__(self, config, name, oslayer)
		self._sb = Sandbox_SharedFS(config, oslayer)

	def _retrieveOutputSingle(self, jobNum, gcID):
		(workflowID, backendName, jobToken, wmsID) = self.parseID(gcID)
		return (jobNum, self._sb.getSandbox(jobNum, jobToken, workflowID))
