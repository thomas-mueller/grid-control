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

from grid_control.backends.broker import Broker
from hpfwk import NamedPlugin, AbstractError

# Represent a broker used by the WMS
class WMS_RequirementAspect(NamedPlugin):
	tagName = 'reqs'
	def __init__(self, config, name, oslayer):
		NamedPlugin.__init__(self, config, name)
		self._oslayer = oslayer
	# Process requirements
	def processRequirements(self, reqs):
		raise AbstractError


# Represent a broker used by the WMS
class WMS_RequirementAspect_Broker(WMS_RequirementAspect):
	def __init__(self, config, name, oslayer, prefix, default, req, discoverFun = None):
		WMS_RequirementAspect.__init__(self, config, name, oslayer)
		brokerClass = config.getClass(prefix + ' broker', default, cls = Broker, inherit = True, tags = [self])
		self._broker = brokerClass.getInstance(prefix, prefix, discoverFun)
		self._req = req

	# Check status of jobs and yield (jobNum, gcID, jobStatus, jobInfos)
	def processRequirements(self, reqs):
		return self._broker.brokerAdd(reqs, self._req)


# Represents a Broker with discovery mechanism available
class WMS_RequirementAspect_BrokerDiscovery(WMS_RequirementAspect_Broker):
	def __init__(self, config, name, oslayer, prefix, default, req):
		WMS_RequirementAspect_Broker.__init__(self, config, name, oslayer, prefix, default, req, self.discover)

	def discover(self):
		raise AbstractError
