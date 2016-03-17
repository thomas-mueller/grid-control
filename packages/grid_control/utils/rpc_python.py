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

def remoteRPCHost(): # this is the first code that is executed on the remote python process
	import sys
	sys.ps1 = ''
	sys.ps2 = ''
	try:
		rpcResult = __import__('json').dumps
	except ImportError:
		try:
			rpcResult = __import__('simplejson').dumps
		except ImportError:
			def rpcResult(data):
				if isinstance(data, str):
					return '"' + repr("'\0" + data)[6:]
				elif isinstance(data, list) or isinstance(data, tuple):
					return '[%s]' % str.join(', ', map(rpcResult, data))
				elif isinstance(data, dict):
					return '{%s}' % str.join(', ', map(lambda k: '%s: %s' % (rpcResult(k), rpcResult(data[k])), data))
				return {None: 'null', True: 'true', False: 'false'}.get(data, str(data))

	def rpcOutputResult(value):
		result = rpcResult(value)
		sys.stdout.write('R:%d:' % len(result))
		sys.stdout.write(result)
		sys.stdout.write('\n')

	def rpcOutputExcept(type, value, traceback):
		exargs = rpcResult(value.args)
		result = '%s:%d:%s' % (type.__name__, len(exargs), exargs)
		sys.stdout.write('E:%d:' % len(result))
		sys.stdout.write(result)
		sys.stdout.write('\n')

	try:
		import hashlib
		md5 = hashlib.md5
	except:
		import md5
		md5 = md5.md5

	def rpcRecvFile(fn, dlen, data):
		fp = open(fn, 'w')
		fp.write(data)
		fp.close()
		return md5(data).hexdigest()

	def rpcSendFile(fn):
		fp = open(fn)
		data = fp.read()
		fp.close()
		return (md5(data).hexdigest(), data)

	sys.displayhook = rpcOutputResult
	sys.excepthook = rpcOutputExcept


import time, inspect
from grid_control.utils.parsing import parseJSON
from grid_control.utils.process_base import ProcessTimeout
from grid_control.utils.thread_tools import GCLock
from python_compat import md5

class RPCError(Exception):
	def __init__(self, msg, detail = None):
		if detail != None:
			msg += '\n%s' % repr(detail)
		Exception.__init__(self, msg)


class RemotePython:
	def __init__(self, callFun):
		self._proc = callFun('python', '-iuE')
		self._proc.setup_terminal()
		try:
			if not self.registerProcedure(remoteRPCHost):
				raise RPCError('Python-RPC handshake failed!', self._proc)
		except ProcessTimeout:
			raise RPCError('Python-RPC handshake timeout!', self._proc)
		self._lock = GCLock()

	def registerProcedure(self, proc):
		codeRPC = map(lambda l: l[1:], inspect.getsource(proc).splitlines()[1:]) # remove leading \t
		self._proc.write_stdin(str.join('\n', codeRPC) + '\n')
		try:
			header = str(int(time.time()))
			self._proc.write_stdin(header + '\n')
			confirmCond = lambda x: x.endswith('R:%d:%s\n' % (len(header), header))
			confirmReply = self._proc.wait_stdout(timeout = 10, cond = confirmCond)
			if not confirmCond(confirmReply):
				return False # RPC procedure registration failed
		except ProcessTimeout:
			self._proc.kill()
			raise
		self._proc.read_stderr(timeout = 0)
		self._proc.clear_logs()
		return True

	def _process(self, timeout, fun, *args, **kwargs):
		waited = 0
		while not self._lock.acquire(False):
			time.sleep(0.1)
			waited += 0.1
			if (timeout != None) and (waited > timeout):
				raise RPCError('Python-RPC process is blocked by another thread!')
		try:
			result = fun(*args, **kwargs)
		except:
			self._proc.clear_logs()
			self._lock.release()
			raise
		self._proc.clear_logs()
		self._lock.release()
		return result

	def eval(self, expr, timeout):
		return self._process(timeout, self._eval, expr, timeout)

	def _eval(self, expr, timeout):
		self._proc.write_stdin(expr + '\n')
		remote = self._proc.wait_stdout(timeout, cond = lambda v: v.endswith('\n'))
		if not remote.endswith('\n'):
			raise RPCError('')
		(rtype, rlen, rdata) = remote.split(':', 2)
		rdata = rdata.rstrip()
		assert(int(rlen) == len(rdata))
		if rtype == 'R':
			return parseJSON(rdata)
		elif rtype == 'E':
			extype, exargslen, exargs = rdata.split(':', 2)
			assert(int(exargslen) == len(exargs))
			exargs = parseJSON(exargs)
			exclass = eval(extype)
			assert(issubclass(exclass, Exception))
			raise exclass(*exargs)
		else:
			raise RPCError('Invalid result type', rdata)

	def send(self, fn, data, timeout):
		return self.eval('rpcRecvFile(%r, %d, %r)\n' % (fn, len(data), data), timeout)

	def recv(self, fn, timeout):
		return self.eval('rpcSendFile(%r)\n' % fn, timeout)
