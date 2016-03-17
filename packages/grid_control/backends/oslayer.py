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

import os, logging
from grid_control import utils
from grid_control.backends.access import AccessToken
from grid_control.gc_exceptions import GCError, InstallationError, RuntimeError
from grid_control.utils.process_base import LocalProcess, ProcessTimeout
from grid_control.utils.rpc_python import RemotePython
from hpfwk import AbstractError, NamedPlugin
from python_compat import itemgetter, md5

class OSLayerError(GCError):
	pass

class OSLayer(NamedPlugin):
	configSections = NamedPlugin.configSections + ['gateway']
	_standalone = False

	def create(cls, config, clsList = []):
		result = None
		if not clsList:
			return OSLayer.getInstance('LocalOS', config, 'local', None)
		clsList.reverse()
		while clsList:
			clsWrapper = clsList.pop()
			cls = clsWrapper.getClass()
			if (result == None) and not cls._standalone:
				result = OSLayer.getInstance('LocalOS', config, 'local', None)
			result = clsWrapper.getInstance(baseOS = result)
		return result
	create = classmethod(create)

	def __init__(self, config, name, baseOS):
		NamedPlugin.__init__(self, config, name)
		self._log = logging.getLogger('oslayer.%s' % name)
		self._baseOS = baseOS
		self._addPaths = config.getList('search paths', [])
		self._pathCache = {}
		self._execCache = {}
		self.path = type('OSLayerPath', (), {})()
		self.path.join = os.path.join
		self.path.basename = os.path.basename
		self.path.dirname = os.path.dirname
		self.urandom = os.urandom

	def __getattr__(self, name):
		self._log.log(logging.DEBUG3, 'Using API %r' % name)
		if self._initProperties:
			self._initProperties()
			self._initProperties = None
			return getattr(self, name)
		raise AttributeError('%r does not exist!' % name)

	def _initProperties(self):
		raise AbstractError

	def _findExecutables(self, cmdList):
		raise AbstractError

	def findExecutables(self, cmdList, raiseNotFound = True, first = True):
		cmdList_todo = filter(lambda cmd: cmd not in self._execCache, cmdList)
		if cmdList_todo:
			execPaths = dict(zip(cmdList_todo, self._findExecutables(cmdList_todo)))
		result = []
		for cmd in cmdList:
			if cmd in cmdList_todo:
				self._execCache[cmd] = execPaths[cmd]
			result.append(self._execCache[cmd])
		if raiseNotFound and ([] in result):
			missing = map(itemgetter(0), filter(lambda (cmd, r): r == [], zip(cmdList, result)))
			raise InstallationError('Could not find executables: %r' % str.join(',', missing))
		if first:
			def getFirst(value):
				if value:
					return value[0]
			result = map(getFirst, result)
		return dict(zip(cmdList, result))

	def findExecutable(self, cmd, checkArgs = None, checkCode = None, checkOutput = None, timeout = 20,
			raiseNotFound = True, resolveCmd = True):
		if resolveCmd:
			cmdPathDict = self.findExecutables([cmd], raiseNotFound = False, first = True)
			if not cmdPathDict.get(cmd, None):
				if raiseNotFound:
					raise InstallationError('Could not find executable %r' % cmd)
				return None
			cmdPath = cmdPathDict[cmd]
		else:
			cmdPath = cmd
		if checkArgs == None:
			return cmdPath
		try:
			(retCode, stdout, stderr) = self.call(cmdPath, *checkArgs).finish(timeout)
		except ProcessTimeout:
			raise InstallationError('Executable %r did not respond in time (%ds)' % (cmdPath, timeout))
		if not checkCode:
			checkCode = lambda value: value == 0
		if not checkCode(retCode):
			raise InstallationError('Executable %r did not give the expected result %r' % (cmdPath, retCode))
		if checkOutput:
			try:
				checkOutput(stdout.strip())
			except Exception:
				raise InstallationError('Executable %r did not give the expected output' % cmdPath)
		return cmdPath

	def call(self, cmd, *args):
		raise AbstractError

	def readFile(self, fn):
		raise AbstractError

	def writeFile(self, fn, srcObj):
		raise AbstractError

	def writeExecutable(self, fn, srcObj):
		raise AbstractError

	def _getPath(self, fn):
		raise AbstractError

	def getPath(self, fn):
		if fn not in self._pathCache:
			self._pathCache[fn] = self._getPath(fn)
		return self._pathCache[fn]

	def ensureDirectoryExists(dn, desc = 'directory'):
		raise AbstractError

	def removeDirectory(self, dn):
		raise AbstractError

	def removeFiles(self, fnList):
		raise AbstractError

	def _copyToLocal(self, fnSource, fnTarget):
		raise AbstractError

	def copyToLocal(self, remoteDN, localDN, selector):
		raise AbstractError

	def _copyToRemote(self, fnSource, fnTarget):
		raise AbstractError

	def copyToRemote(self, localDN, remoteDN, selector):
		raise AbstractError


class PythonOS(OSLayer):
	def _initProperties(self):
		self.environ = self._pythonCall('dict(os.environ)')

	def _pythonCall(self, expr):
		raise AbstractError

	def _findExecutables(self, cmdList):
		searchPathExpr = """(os.environ['PATH'].split(os.pathsep) + %r)""" % self._addPaths
		joinPaths = """lambda c: filter(lambda fn: os.access(fn, os.X_OK), map(lambda p: os.path.join(p, c), %s))""" % searchPathExpr
		return self._pythonCall("""map(%s, %r)""" % (joinPaths, cmdList))

	def writeExecutable(self, fn, srcObj):
		self.writeFile(fn, srcObj)
		return self._pythonCall("""os.chmod(%r, __import__('stat').S_IRWXU)""" % fn)

	def _getPath(self, fn):
		return self._pythonCall("""os.path.abspath(os.path.expandvars(os.path.normpath(os.path.expanduser(%r))))""" % fn)

	def ensureDirectoryExists(self, dn, desc = 'directory'):
		try:
			return self._pythonCall("""map(os.makedirs, filter(lambda dn: not os.path.exists(dn), [%r]))""" % dn)
		except Exception:
			raise OSLayerError('Problem creating %s "%s"' % (desc, dn), RuntimeError)

	def removeDirectory(self, dn):
		return self._pythonCall("""__import__('shutil').rmtree(%r)""" % dn)

	def removeFiles(self, fnList):
		rmExpr = """lambda fn: (fn, {True: __import__('shutil').rmtree, False: os.unlink}[os.path.isdir(fn)](fn))[0]"""
		return self._pythonCall("""map(%s, filter(os.path.exists, %r))""" % (rmExpr, fnList))

	def _getExpr_selectFiles(self, dn, selector):
		selector = map(lambda x: os.path.join(dn, x), selector)
		allFilesExpr = """reduce(list.__add__, map(lambda (root, dl, fl): map(lambda fn: os.path.join(root, fn), fl), os.walk(%r)), [])""" % dn
		matchFunExpr = """lambda fn: True in map(lambda pat: __import__('fnmatch').fnmatch(fn, pat), %r)""" % selector
		return """filter(%s, %s)""" % (matchFunExpr, allFilesExpr)

	def copyToLocal(self, remoteDN, localDN, selector):
		for fnSource in self._pythonCall(self._getExpr_selectFiles(remoteDN, selector)):
			self._copyToLocal(fnSource, fnSource.replace(remoteDN, localDN))

	def copyToRemote(self, localDN, remoteDN, selector):
		for fnSource in eval(self._getExpr_selectFiles(localDN, selector)):
			self._copyToRemote(fnSource, fnSource.replace(localDN, remoteDN))


class LocalOS(PythonOS):
	_standalone = True

	def __init__(self, config, name, baseOS):
		PythonOS.__init__(self, config, name, baseOS)
		self._tmpPath = config.getWorkPath()

	def _pythonCall(self, expr):
		self._log.log(logging.DEBUG1, 'Evaluating python code %r' % expr)
		return eval(expr)

	def _copyToLocal(self, fnSource, fnTarget):
		dn = os.path.dirname(fnTarget)
		if not os.path.exists(dn):
			os.makedirs(dn)
		os.rename(fnSource, fnTarget)

	def call(self, cmd, *args):
		return LocalProcess(cmd, *args)

	def writeFile(self, fn, srcObj):
		fp = open(fn, 'w')
		while True:
			data = srcObj.read(1024*16)
			if not data:
				break
			fp.write(data)
		fp.close()

	def readFile(self, fn):
		fp = open(fn)
		data = fp.read()
		fp.close()
		return data


class Local(LocalOS):
	pass


class RemoteOS(PythonOS):
	def __init__(self, config, name, baseOS):
		PythonOS.__init__(self, config, name, baseOS)
		self._remotePython = RemotePython(self.call)
		if not self._remotePython.eval('import os, sys\nTrue', 10):
			raise RuntimeError('Unable to import base libraries!')

	def _pythonCall(self, expr):
		self._log.log(logging.WARNING, 'Evaluating python code %r' % expr)
		return self._remotePython.eval(expr, 10)

	def _copyToLocal(self, fnSource, fnTarget):
		dn = os.path.dirname(fnTarget)
		if not os.path.exists(dn):
			os.makedirs(dn)
		# TODO: use scp instead
		fp = open(fnTarget, 'w')
		tmp = self.readFile(fnSource)
		fp.write(tmp)
		fp.close()

	def writeFile(self, fn, srcObj):
		data = srcObj.read()
		self._remotePython.send(fn, data, len(data) / 1024. * 100. + 10)

	def readFile(self, fn):
		(remoteHash, data) = self._remotePython.recv(fn, 120)
		localHash = md5(data).hexdigest()
		assert(remoteHash == localHash)
		return data


class SSH(RemoteOS):
	def __init__(self, config, name, baseOS):
		config.set('remote host', name)
		self._connection_args = ['-x']#, '-o', 'BatchMode=yes']
		port = config.getInt('remote port', -1)
		if port > 0:
			self._connection_args.extend(['-p', port])
		user = config.get('remote user', '')
		host = config.get('remote host')
		# variable name contains "password" - exception handler won't show content
		self._password = config.get('remote password', '')
		if user:
			self._connection_args.append('%s@%s' % (user, host))
		else:
			self._connection_args.append(host)
		RemoteOS.__init__(self, config, name, baseOS)
		env = config.getDict('remote env', {})[0]
		if env: # ssh -o SendEnv is usually very strict - use env from coreutils to set environment
			self._connection_args.append('env')
			self._connection_args.extend(map(lambda key: '%s=%s' % (key, env[key]), env))
		script = config.get('remote wrapper', '')
		if script: # as alternative to remote env, this allows to specifiy a wrapper to set vars
			self._connection_args.append(script)

	def call(self, *args):
		proc = self._baseOS.call('ssh', *(self._connection_args + list(args)))
		if self._password:
			proc.wait_stdout(10, lambda b: 'password' in b.lower())
			proc.write_stdin(self._password + '\n', log = False)
			proc.clear_logs()
		return proc


class GSISSH(SSH):
	def __init__(self, config, name, baseOS):
		SSH.__init__(self, config, name, baseOS)
		self._access = config.getClass('access token', 'VomsProxy', cls = AccessToken)

	def call(self, *args):
		return self._baseOS.call('gsissh', *(self._connection_args + list(args)))
