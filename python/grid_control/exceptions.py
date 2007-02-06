import sys

# our exception base class
class GridError(Exception):
	def __init__(self, message):
		self.message = message

	def showMessage(self):
		print >> sys.stderr, "%s: %s" % (sys.argv[0], self.message)


# some error with the Grid installation
class InstallationError(GridError):
	pass	# just inherit everything from GridError