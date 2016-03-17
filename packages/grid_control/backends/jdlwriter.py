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

from python_compat import sorted

class JDLWriter(object):
	def format(self, value):
		if isinstance(value, str):
			for token, subst in [('\\', r'\\'), ('\"', r'\"'), ('\n', r'\n')]:
				value = value.replace(token, subst)
			return '"%s"' % value
		elif isinstance(value, list):
			return '{ %s }' % str.join(', ', map(self.format, value))
		elif isinstance(value, int):
			return str(value)
		raise APIError('Invalid input to JDL writer')

	def writeJDL(self, data = {}, formatted = []):
		result = ''
		for key in sorted(data):
			value = data[key]
			if key not in formatted:
				value = self.format(value)
			result += '%s = %s;\n' % (key, value)
		return result
