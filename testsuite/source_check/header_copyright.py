# -*- coding: utf-8 -*-
import sys, subprocess, os
x = """
# | Copyright %s Karlsruhe Institute of Technology
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

""".lstrip()

def updateHeader(fn):
	licence_update_revs = [
		'421bfb25b9ba81c68225f77bc5142a9c9b68e183',
		'a5cbd19838ca2fe1f30857c19882e008214c11eb',
	]
	gitlog = subprocess.check_output('git log --follow --format="format:%ai %an <%ae> %H" ' + fn +
		str.join('', map(lambda x: ' | grep -v ' + x, licence_update_revs)), shell = True)
	try:
		year_a = gitlog.splitlines()[-1].split('-')[0]
		year_b = gitlog.splitlines()[0].split('-')[0]
		if year_a == year_b:
			year = year_a
		else:
			year = year_a + '-' + year_b
		header = x % year
	except:
		print 'no version', fn
		raise
	doInsert = True
	data = open(fn).readlines()
	fp = open(fn, 'w')
	for line in data:
		if ('print "%s"' % fn) in line:
			continue
		if line.startswith('#-#') or line.startswith('# |'):
			continue
		elif line.startswith('#!'):
			fp.write(line)
		elif doInsert and (line.strip() != ''):
			doInsert = False
			fp.write(header)
#			fp.write('print "%s"\n' % fn)
			fp.write(line)
		elif not doInsert:
			fp.write(line)
	fp.close()
	if data != open(fn).readlines():
		print 'changed', fn

import getFiles
for (fn, _) in getFiles.getFiles(showExternal = False, showAux = True, showTestsuite = False):
	if 'fragmentForCMSSW.py' in fn: # don't add header to config fragment
		continue
	try:
		sys.stdout.write(fn + ' ')
		sys.stdout.flush()
		updateHeader(fn)
		sys.stdout.write('OK\n')
	except Exception, e:
		sys.stdout.write('FAIL\n')
		print e
		pass
