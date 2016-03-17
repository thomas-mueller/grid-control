#-#  Copyright 2007-2015 Karlsruhe Institute of Technology
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

from grid_control.backends.aspect_broker import WMS_RequirementAspect_BrokerDiscovery
from grid_control.backends.wms_grid import GridWMS

class LCG_RequirementAspect(WMS_RequirementAspect_BrokerDiscovery):
	def __init__(self, config, name, oslayer, option, default, reqKey, lcgKey, parser = str.strip):
		WMS_RequirementAspect_BrokerDiscovery.__init__(self, config, name, oslayer, option, default, reqKey)
		self._exec = oslayer.findExecutable('lcg-infosites')
		self._lcgKey = lcgKey
		self._parser = parser

	def discover(self):
		for line in self._oslayer.call(self._exec, self._lcgKey).iter_stdout(timeout = 10):
			result = self._parser(line)
			if result:
				yield result


class LCG_RequirementAspect_CE(LCG_RequirementAspect):
	def __init__(self, config, name, oslayer):
		def parseCE(line):
			if '/' in line:
				return line.split()[-1]
		LCG_RequirementAspect.__init__(self, config, name, oslayer, 'sites', 'UserBroker',
			reqKey = GridWMS.CE, lcgKey = 'ce', parser = parseCE)


class LCG_RequirementAspect_Sites(LCG_RequirementAspect):
	def __init__(self, config, name, oslayer):
		def parseSite(line):
			if '/' in line:
				return line.split()[-1].split(':')[0]
		LCG_RequirementAspect.__init__(self, config, name, oslayer, 'sites', 'UserBroker',
			reqKey = GridWMS.SITES, lcgKey = 'ce', parser = parseSite)


class LCG_RequirementAspect_WMS(LCG_RequirementAspect):
	def __init__(self, config, name, oslayer):
		LCG_RequirementAspect.__init__(self, config, name, oslayer, 'endpoint', 'RandomBroker',
			reqKey = GridWMS.ENDPOINT, lcgKey = 'wms')

	def discover(self):
		return ['https://graspol.nikhef.nl:7443/glite_wms_wmproxy_server']
