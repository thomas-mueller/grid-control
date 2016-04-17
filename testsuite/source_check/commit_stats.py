# -*- coding: utf-8 -*-
import sys, subprocess, os
x = """
#-#  Copyright %s Karlsruhe Institute of Technology
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

""".lstrip()

import getFiles

if __name__ == '__main__':
	os.system('git log --pretty="%H|%aN|%aE" --no-merges -w --numstat > addHEADER.log')
	email_dict = {}
	commits_dict = {}
	changes_a_dict = {}
	changes_fn_dict = {}
	for line in map(str.strip, open('addHEADER.log')):
		if ('|' in line) and ('@' in line):
			author, email = line.split('|')[1:]
			email_dict[author] = email
			commits_dict[author] = commits_dict.get(author, 0) + 1
		elif line:
			add, rm, fn = line.split()
			if add == '-':
				continue
			if getFiles.matchFile(fn, showExternal = False):
				prev_a = changes_a_dict.get(author, (0, 0))
				changes_a_dict[author] = (prev_a[0] + int(add), prev_a[1] + int(rm))
				prev_fn = changes_fn_dict.get(fn, (0, 0))
				changes_fn_dict[fn] = (prev_fn[0] + int(add), prev_fn[1] + int(rm))
#	print email_dict
	email_dict['Manuel Zeise'] = 'manuel.zeise@sap.com'

#	for fn in sorted(changes_fn_dict, key = lambda x: changes_fn_dict[x]):
#	for fn in sorted(changes_fn_dict):
#		print '(%5d:%5d)' % changes_fn_dict[fn], fn
#	print len(changes_fn_dict)
#	for a in sorted(changes_a_dict, key = lambda x: (-(changes_a_dict[x][0] + changes_a_dict[x][1]), x.split()[1])):
#		print '(%5d:%5d)' % changes_a_dict[a], a
#	print changes_a_dict[']
	for a in sorted(changes_a_dict, key = lambda x: (-(changes_a_dict[x][0] - changes_a_dict[x][1]), x.split()[1])):
#		print (changes_a_dict[a][0] - changes_a_dict[a][1]),
		print ' '*14, '%-23s' % a.decode('utf-8'), '<%s>' % email_dict[a].replace('stober@cern.ch', 'mail@fredstober.de').lower()
#		print changes_a_dict[a]
#	print len(changes_a_dict)
