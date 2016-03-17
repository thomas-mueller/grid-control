#!/usr/bin/env python
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

import os, sys, getpass, logging, optparse
from gcSupport import Job, OSLayer, getConfig, parseOptions, utils
from grid_control.backends import WMS
from grid_control.backends.aspect_submit import StreamMode
from grid_control.utils import ProcessArchiveHandler
from grid_control.utils.file_objects import VirtualFile
from python_compat import md5

def exitWithHelp(message):
	sys.stderr.write(message + '\n')
	sys.stderr.write('Use --help to get a list of options!\n')
	sys.exit(os.EX_USAGE)

def main():
	parser = optparse.OptionParser()
	parser.add_option('-w', '--wms',      dest='wms',        default='local', help='Select WMS')
	parser.add_option('-o', '--option',   dest='options',    default=[],      action='append',
		help='Set configuration options')
	parser.add_option('-a', '--access',   dest='access',     default='local', help='Select access method')
	parser.add_option('-j', '--jobs',     dest='jobs',       default=None,    help='File with job IDs')
	parser.add_option('',   '--debug',    dest='debug',      default=False,   action='store_true')
	parser.add_option('-e', '--env',      dest='env',        default=[],      action='append',
		help='Environment variables for remote submission hosts')
	parser.add_option('-s', '--search',   dest='searchpath', default=[],      action='append',
		help='Add search paths for finding executables')
	parser.add_option('-p', '--password', dest='password',    default=False,  action='store_true',
		help='Ask for passwords')

	ogSubmit = optparse.OptionGroup(parser, 'Job submission',
		'Usage: %s submit [options] <executable> [<arguments>...]' % sys.argv[0])
	ogSubmit.add_option('', '--njobs', dest='submit_jobs', default=1,
		help='Number of jobs')
	ogSubmit.add_option('', '--var', dest='submit_var', default=[], action='append',
		help='Variable definitions - eg. VAR=VALUE')
	ogSubmit.add_option('', '--cores', dest='submit_cores', default=0,
		help='#CPUs requirement of jobs')
	ogSubmit.add_option('', '--memory', dest='submit_memory', default=0,
		help='Memory requirement of jobs')
	ogSubmit.add_option('', '--walltime', dest='submit_walltime', default=0,
		help='Walltime requirement of jobs')
	ogSubmit.add_option('', '--cputime',  dest='submit_cputime', default=0,
		help='Cputime requirement of jobs')
	ogSubmit.add_option('', '--queue', dest='submit_queue', default='',
		help='Submission queue')
	ogSubmit.add_option('', '--site', dest='submit_site', default='',
		help='Submission site')
	parser.add_option_group(ogSubmit)

	ogCheck = optparse.OptionGroup(parser, 'Job status check',
		'Usage: %s check [options] <job id> ...' % sys.argv[0])
	parser.add_option_group(ogCheck)

	ogCancel = optparse.OptionGroup(parser, 'Job cancellation',
		'Usage: %s cancel [options] <job id> ...' % sys.argv[0])
	parser.add_option_group(ogCancel)

	ogOutput = optparse.OptionGroup(parser, 'Job output retrieval',
		'Usage: %s output [options] <job id> ...' % sys.argv[0])
	parser.add_option_group(ogOutput)

	ogInfo = optparse.OptionGroup(parser, 'Query WMS information',
		'Usage: %s info [options] <job id> ...' % sys.argv[0])
	ogInfo.add_option('', '--brokers', dest='show_brokers', default=False, action='store_true',
		help='Show brokers')
	ogInfo.add_option('', '--token', dest='show_token', default=False, action='store_true',
		help='Show WMS access token')
	parser.add_option_group(ogInfo)

	(opts, args) = parseOptions(parser)
	if opts.debug:
		logging.getLogger('exception').addHandler(logging.StreamHandler(sys.stdout))

	if len(args) == 0:
		exitWithHelp('WMS command missing!')
	command = args[0]
	args = args[1:]

	envDict = {}
	for entry in opts.env:
		remote, envEntry = entry.split(':', 1)
		envDict.setdefault(remote, []).append(envEntry.replace('=', '=>'))
	configDict = {'wms': opts.wms, 'wms access': opts.access}
	for option in opts.options:
		key, value = option.split('=', 1)
		configDict[key] = value.replace('\\n', '\n')
		print configDict
	config = getConfig(configDict = configDict, section = 'backend')
	config.changeView(setSections = ['gateway']).set('search paths', str.join('\n', opts.searchpath))
	for remote in envDict:
		config_tmp = config.changeView(setSections = ['gateway %s' % remote])
		config_tmp.set('remote env', str.join('\n', envDict[remote]))

	utils.ensureDirExists(config.getWorkPath())
	os.environ['PWD'] = config.getWorkPath()
	os.chdir(config.getWorkPath())
	oslayerList = config.getClassList('wms access', cls = OSLayer)
	if opts.password:
		for oscls in oslayerList:
			password = getpass.getpass('Password for %s: ' % oscls.getObjectName())
			if password:
				config_tmp = config.changeView(setSections = ['gateway %s' % oscls.getObjectName()])
				config_tmp.set('remote password', password)

	oslayer = OSLayer.create(config, oslayerList)
	wms = config.getClass('wms', cls = WMS).getInstance(oslayer)
	logging.getLogger().addHandler(ProcessArchiveHandler('error.tar'))

	def get_gcID_jobNum(args):
		if opts.jobs:
			args += map(str.strip, open(opts.jobs).readlines())
		if len(args) == 0:
			exitWithHelp('No jobs specified')
		gcID_jobNum = map(lambda x: x.rsplit(':', 1), args)
		return map(lambda (gcID, jobNum): (gcID, int(jobNum)), gcID_jobNum)

	class DummyBundler:
		def __init__(self):
			self._taskID = 'GC' + md5(os.urandom(20)).hexdigest()[:12]
			self.setReqProcessor(lambda x: x)
		def setReqProcessor(self, proc):
			self._reqProc = proc
		# script options for stdout/err: stream (direct), delay (tmp), file (shared)
		def getPackage(self, jobNum, streams = StreamMode.landingzone):
			if streams == StreamMode.direct:
				scriptContent = 'echo "JOBID=%d\nEXITCODE=0\n" > job.info; echo $$; (echo "Hello World"; echo $$; sleep 10)' % jobNum
			else:#if streams == StreamMode.shared:
				scriptContent = 'echo "JOBID=%d\nEXITCODE=0\n" > job.info; (echo "Hello World"; echo $$; sleep 10) > stdout' % jobNum
			def genScript(dn):
				return VirtualFile(None, '#!/bin/sh\ncd %s\n' % dn + scriptContent)
			reqs = []
			if opts.submit_memory:
				reqs.append((WMS.MEMORY, int(opts.submit_memory)))
			if opts.submit_cores:
				reqs.append((WMS.CPUS, int(opts.submit_cores)))
			if opts.submit_walltime:
				reqs.append((WMS.WALLTIME, int(opts.submit_walltime)))
			if opts.submit_cputime:
				reqs.append((WMS.CPUTIME, int(opts.submit_cputime)))
			if opts.submit_queue:
				reqs.append((WMS.QUEUES, [opts.submit_queue]))
			if opts.submit_site:
				reqs.append((WMS.SITES, opts.submit_site.split()))
			return utils.Result(
				taskID = self._taskID,
				jobName = '%s_%d' % (self._taskID, jobNum),
				script = genScript,
				requirements = self._reqProc(reqs),
				input_transfers = [('sandbox', '/etc/fstab')],
				output_transfers = [('sandbox', 'stdout'), ('sandbox', 'job.info'), ('sandbox', 'gc.stdout'), ('sandbox', 'gc.stderr')]
			)

	if command == 'submit':
		if len(args) != 1:
			exitWithHelp('Executable missing!')
		submitResults = list(wms.submitJobs(range(int(opts.submit_jobs)), DummyBundler()))
		for jobNum, gcID, data in submitResults:
			print jobNum, gcID, data
		if opts.jobs:
			fp = open(opts.jobs, 'w')
			for jobNum, gcID, data in submitResults:
				fp.write('%s:%d\n' % (gcID, jobNum))
			fp.close()

	elif command == 'check':
		for (jobNum, gcID, jobState, jobInfos) in wms.checkJobs(get_gcID_jobNum(args), "not listed"):
			print jobNum, gcID, Job.enum2str(jobState, jobState), jobInfos

	elif command == 'output':
		for (jobNum, jobExitCode, data, outputDir) in wms.retrieveJobs(get_gcID_jobNum(args), DummyBundler()):
			print jobNum, jobExitCode, data, outputDir

	elif command == 'cancel':
		if opts.jobs:
			args += map(str.strip, open(opts.jobs).readlines())
		if len(args) == 0:
			exitWithHelp('No jobs specified')
		gcID_jobNum = map(lambda x: x.rsplit(':', 1), args)
		gcID_jobNum = map(lambda (gcID, jobNum): (gcID, int(jobNum)), gcID_jobNum)
		for x in wms.cancelJobs(gcID_jobNum):
			print x

	elif command == 'info':
		if opts.show_brokers:
			for broker in wms._aspects_brokers:
				try:
					print broker
					for entry in broker.discover():
						print entry,
					print
				except:
					pass

		if opts.show_token:
			tokenList = []
			if not args:
				tokenList.append(wms.getAccessToken(None))
			else:
				for gcID in args:
					tokenList.append(wms.getAccessToken(gcID))
			print tokenList
			def getEntries():
				for token in tokenList:
					result = {0: token.getUsername(), 1: token.getFQUsername(), 2: token.getGroup(),
						4: token.canSubmit(neededTime = 0, canCurrentlySubmit = False),
#						5: utils.strTime(token._getTimeleft(True))
					}
					if not token.getAuthFiles():
						yield result
					for fn in token.getAuthFiles():
						result[3] = fn
						yield result
						result = {}
			utils.printTabular([(0, 'User'), (1, 'fqUsername'), (2, 'Group'), (3, 'AuthFile'), (4, 'active'), (5, 'Time left')], getEntries())

	else:
		print 'Unknown command: %r' % command

sys.exit(main())
