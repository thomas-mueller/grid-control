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

import sys, time
from grid_control import utils
from grid_control.backends.aspect_base import Sandbox_SharedFS
from grid_control.backends.aspect_cancel import WMS_CancelAspect_Chunked_Uniform
from grid_control.backends.aspect_check import WMS_CheckAspect_Serial_SSP
from grid_control.backends.aspect_retrieve import WMS_RetrieveAspect_Serial
from grid_control.backends.aspect_submit import WMS_SubmitAspect_Serial
from grid_control.backends.jdlwriter import JDLWriter
from grid_control.backends.wms import BasicWMS, WMS
from grid_control.job_db import Job
from grid_control.utils.file_objects import VirtualFile
from hpfwk import APIError
from python_compat import parsedate

class Grid_SubmitAspect(WMS_SubmitAspect_Serial):
	def __init__(self, config, name, oslayer, submitExec):
		WMS_SubmitAspect_Serial.__init__(self, config, name, oslayer)
		self._submitExec = submitExec
		self._jdlWriter = JDLWriter()
		self._vo = config.get('vo', '', onChange = None)
		self._configFile = config.getPath('config', '', onChange = None)
		self._sandbox = Sandbox_SharedFS(config, oslayer)
		self._sandboxRoot = oslayer.getPath(config.get('sandbox path', '${PWD}/sandbox'))
#		self._warnSBSize = config.getInt('warn sb size', 5 * 1024 * 1024)

	def _formatRequirements_Storage(self, seList):
		fmt = lambda se: 'Member(%s, other.GlueCESEBindGroupSEUniqueID)' % self._jdlWriter.format(se)
		if seList:
			return '( %s )' % str.join(' || ', map(fmt, seList))

	def _formatRequirements_Sites(self, sites):
		fmt = lambda x: 'RegExp(%s, other.GlueCEUniqueID)' % self._jdlWriter.format(x)
		(blacklist, whitelist) = utils.splitBlackWhiteList(sites)
		sitereqs = map(lambda x: '!' + fmt(x), blacklist)
		if len(whitelist):
			sitereqs.append('(%s)' % str.join(' || ', map(fmt, whitelist)))
		if sitereqs:
			return '( %s )' % str.join(' && ', sitereqs)

	def _formatRequirements(self, reqList):
		result = ['other.GlueHostNetworkAdapterOutboundIP']
		for reqType, reqValue in reqList:
			if reqType == WMS.SOFTWARE:
				result.append('Member(%s, other.GlueHostApplicationSoftwareRunTimeEnvironment)' % self._jdlWriter.format(reqValue))
			elif reqType == WMS.WALLTIME:
				if reqValue > 0:
					result.append('(other.GlueCEPolicyMaxWallClockTime >= %d)' % int((reqValue + 59) / 60))
			elif reqType == WMS.CPUTIME:
				if reqValue > 0:
					result.append('(other.GlueCEPolicyMaxCPUTime >= %d)' % int((reqValue + 59) / 60))
			elif reqType == WMS.MEMORY:
				if reqValue > 0:
					result.append('(other.GlueHostMainMemoryRAMSize >= %d)' % reqValue)
			elif reqType == WMS.STORAGE:
				result.append(self._formatRequirements_Storage(reqValue))
			elif reqType == WMS.SITES:
				result.append(self._formatRequirements_Sites(reqValue))
		return str.join(' && ', filter(lambda x: x != None, result))

	# Submit single job and yield (jobNum, WMS ID, other data) - should use _submitCall()
	def _submitJob(self, jobNum, jobToken, package):
		dn = self._sandbox.getSandbox(jobNum, jobToken, package.taskID)
		self._oslayer.ensureDirectoryExists(dn)
		for (area, fn) in package.input_transfers:
			if area == 'sandbox':
				self._oslayer.copyToRemote(self._oslayer.path.dirname(fn), dn, fn)

		def getSandboxFiles(files):
			sb_files = map(lambda (area, fn): fn, filter(lambda (area, fn): area == 'sandbox', files))
			# ... TODO wildcard logic
			return sb_files

		# Warn about too large sandboxes
#		sbSizes = map(os.path.getsize, sbIn)
#		if sbSizes and (self._warnSBSize > 0) and (sum(sbSizes) > self._warnSBSize):
#			if not utils.getUserBool('Sandbox is very large (%d bytes) and can cause issues with the WMS! Do you want to continue?' % sum(sbSizes), False):
#				sys.exit(os.EX_OK)
#			self._warnSBSize = 0
		# Handle wildcards in output sandbox
#		wcList = filter(lambda x: '*' in x, sbOut)
#		if len(wcList):
#			self._writeJobConfig(cfgPath, jobNum, module, {'GC_WC': str.join(' ', wcList)})
#			sandboxOutJDL = filter(lambda x: x not in wcList, sbOut) + ['GC_WC.tar.gz']
#		else:
#			self._writeJobConfig(cfgPath, jobNum, module)
#			sandboxOutJDL = sbOut

		vo = self._vo
		if not vo:
			vo = self._access.getGroup()

		execPath = self._oslayer.path.join(dn, 'job_%d.sh' % jobNum)
		jdlData = {
			'Executable': 'job_%d.sh' % jobNum,
			'Arguments': '',
			'StdOutput': 'gc.stdout',
			'StdError': 'gc.stderr',
			'InputSandbox': [execPath] + getSandboxFiles(package.input_transfers),
			'OutputSandbox': getSandboxFiles(package.output_transfers),
			'Requirements': self._formatRequirements(package.requirements),
			'VirtualOrganisation': vo,
			'Rank': '-other.GlueCEStateEstimatedResponseTime',
			'RetryCount': 2
		}
		cpus = max([1] + map(lambda (rt, rv): rv, filter(lambda (rt, rv): rt == WMS.CPUS, package.requirements)))
		jdlBuffer = self._jdlWriter.writeJDL(jdlData, ['Rank', 'Requirements'])
		jdlPath = self._oslayer.path.join(dn, 'job_%d.jdl' % jobNum)
		self._oslayer.writeFile(jdlPath, VirtualFile(jdlPath, jdlBuffer))
		self._oslayer.writeExecutable(execPath, package.script(dn = '.'))
		jobNum, gcID, data = self._submitCall(jobNum, jobToken, package.taskID, package.jobName, package.requirements, jdlPath, timeout = 20)
		data['jdl'] = jdlBuffer
		return (jobNum, gcID, data)

	# Parse and validate result from submit process and return (isValid, gcID)
	def _submitParse(self, proc):
		result = proc.read_stdout(timeout = 30)
		if 'glite-wms-job-submit Success' not in result:
			return (False, result)
		for line in map(str.strip, result.splitlines()):
			if line.startswith('http'):
				return (True, line)
		return (False, result)


class Grid_CancelAspect(WMS_CancelAspect_Chunked_Uniform):
	def __init__(self, config, name, oslayer, cancelExec):
		WMS_CancelAspect_Chunked_Uniform.__init__(self, config, name, oslayer)
		self._cancelExec = cancelExec

	def _cancelChunk(self, gcID_jobNum_Chunk):
		wmsID_gcID_map = self._mapIDs(gcID_jobNum_Chunk)
		gcID_jobNum_map = dict(gcID_jobNum_Chunk)

		activity = utils.ActivityLog('cancelling jobs')
		args = [self._cancelExec, '--noint', '--logfile', '/dev/stderr'] + wmsID_gcID_map.keys()
		proc = self._oslayer.call(*args)
		# select cancelled jobs
		for line in filter(lambda x: x.startswith('- '), proc.iter_stdout(10)):
			gcID = wmsID_gcID_map.get(line.strip('- \n'))
			yield (gcID_jobNum_map.get(gcID), gcID)
		del activity

		if proc.status(10) != 0:
			self._logProc(proc, discardlist = ['Keyboard interrupt raised by user'])


class Grid_CheckAspect(WMS_CheckAspect_Serial_SSP):
	statusMap = {
		'ready':     Job.READY,
		'submitted': Job.SUBMITTED,
		'waiting':   Job.WAITING,
		'queued':    Job.QUEUED,
		'scheduled': Job.QUEUED,
		'running':   Job.RUNNING,
		'aborted':   Job.ABORTED,
		'cancelled': Job.CANCELLED,
		'failed':    Job.ABORTED,
		'done':      Job.DONE,
		'cleared':   Job.ABORTED
	}
	def __init__(self, config, name, oslayer, statusExec):
		WMS_CheckAspect_Serial_SSP.__init__(self, config, name, oslayer, Grid_CheckAspect.statusMap)
		self._statusExec = statusExec

	def _checkArguments(self, wmsIDs):
		return [self._statusExec, '--verbosity', 1, '--noint', '--logfile', '/dev/stderr'] + wmsIDs

	def _checkParse(self, proc):
		jobinfo = {}
		for line in proc.iter_stdout(5):
			if jobinfo and ('======' in line):
				yield (jobinfo.get('id'), jobinfo.get('status'), jobinfo)
				jobinfo = {}
			try:
				key, value = filter(lambda x: x != '', map(str.strip, line.split(':', 1)))
			except:
				continue
			key = key.lower()
			if key.startswith('status info'):
				jobinfo['id'] = value
			elif key.startswith('current status'):
				if 'failed' in value:
					jobinfo['status'] = 'failed'
				else:
					jobinfo['status'] = value.split('(')[0].split()[0].lower()
			elif key.startswith('status reason'):
				jobinfo['reason'] = value
			elif key.startswith('destination'):
				jobinfo['dest'] = value
			elif key.startswith('reached') or key.startswith('submitted'):
				jobinfo['timestamp'] = int(time.mktime(parsedate(value)))

	def _checkHandleError(self, proc):
		self._logProc(proc, discardlist = ['Keyboard interrupt raised by user'])


class Grid_RetrieveAspect(WMS_RetrieveAspect_Serial):
	def __init__(self, config, name, oslayer, outputExec):
		WMS_RetrieveAspect_Serial.__init__(self, config, name, oslayer)
		self._outputExec = outputExec
		self._sandboxRoot = oslayer.getPath(config.get('sandbox path', '${PWD}/sandbox'))

	def _retrieveOutputSingle(self, jobNum, gcID, outputFiles):
		dn = self._oslayer.path.join(self._sandboxRoot, 'tmp')
		self._oslayer.ensureDirectoryExists(dn, 'temporary retrieval directory')
		(workflowID, backendName, jobToken, wmsID) = self.parseID(gcID)
		args = [self._outputExec, '--noint', '--logfile', '/dev/stderr', '--dir', dn, wmsID]
		proc = self._oslayer.call(*args)
		try:
			for line in proc.iter_stdout(10):
				if line.startswith(dn):
					return (jobNum, line.strip())
		except Exception:
			pass
		self._logProc(proc, discardList = ['Output files already retrieved'])
		return (jobNum, None)


class GridWMS(BasicWMS):
	configSections = BasicWMS.configSections + ['grid']

	def __init__(self, config, name, oslayer, brokers = [],
			submit = None, submitExec = None,
			check = None, checkExec = None,
			cancel = None, cancelExec = None,
			retrieve = None, retrieveExec = None):

		config.set('access token', 'VomsProxy')
		oslayer.findExecutables(filter(lambda x: x != None, [submitExec, checkExec, cancelExec, retrieveExec]))
		if submitExec and not submit:
			submit = Grid_SubmitAspect(config, name, oslayer, oslayer.findExecutable(submitExec))
		if checkExec and not check:
			check = Grid_CheckAspect(config, name, oslayer, oslayer.findExecutable(checkExec))
		if cancelExec and not cancel:
			cancel = Grid_CancelAspect(config, name, oslayer, oslayer.findExecutable(cancelExec))
		if retrieveExec and not retrieve:
			retrieve = Grid_RetrieveAspect(config, name, oslayer, oslayer.findExecutable(retrieveExec))
		if None in [submit, check, cancel, retrieve]:
			raise APIError('GridWMS aspects not set!')
		BasicWMS.__init__(self, config, name, oslayer, submit, check, cancel, retrieve, brokers)
utils.makeEnum(['CE', 'ENDPOINT', 'SITES'], GridWMS, useHash = True)


class Grid(WMS):
	def __new__(cls, config, name, oslayer):
		return WMS.getInstance('GliteWMS', config, name, oslayer)
