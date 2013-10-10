from thrift.transport import TSocket, THttpClient, TTransport, TZlibTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol
try:
	#only available from thrift >= 0.9
	from thrift.protocol import TJSONProtocol

	HAS_JSON_PROTOCOL = True
except ImportError:
	HAS_JSON_PROTOCOL = False

from urlparse import urlparse


class ThriftClient(object):
	"""
	This methods allow
	"""
	BINARY = "BINARY"
	COMPACT = "COMPACT"
	if HAS_JSON_PROTOCOL:
		JSON = "JSON"

	def __init__(self, interface, app=None, config=None):
		self.interface = interface
		self.protocol = None
		self.transport = None
		self.client = None
		self.config = config
		if app is not None:
			self.init_app(app)

	def init_app(self, app, config=None):
		if not config:
			config = self.config
		if not config:
			config = app.config

		config.setdefault("THRIFTCLIENT_TRANSPORT", "tcp://127.0.0.1:9090")
		config.setdefault("THRIFTCLIENT_PROTOCOL", ThriftClient.BINARY)

		config.setdefault("THRIFTCLIENT_BUFFERED", False)
		config.setdefault("THRIFTCLIENT_ZLIB", False)

		self._set_client(app, config)

		@app.before_request
		def before_request():
			assert(self.client is not None)
			assert(self.transport is not None)
			try:
				self.transport.open()
			except TTransport.TTransportException:
				raise RuntimeError("Unable to connect to thrift server")

		@app.teardown_request
		def after_request(response):
			self.transport.close()

	def _set_client(self, app, config):
		#configure thrift thransport
		uri = urlparse(config["THRIFTCLIENT_TRANSPORT"])
		if uri.scheme == "tcp":
			port = uri.port or 9090
			self.transport = TSocket.TSocket(uri.netloc, port)
		elif uri.scheme == "http" or uri.scheme == "https":
			self.transport = THttpClient.THttpClient(config["THRIFTCLIENT_SERVER"])
		elif uri.scheme == "unix":
			self.transport = TSocket.TSocket(None, None, uri.path)
		else:
			raise RuntimeError(
				"invalid configuration for THRIFTCLIENT_TRANSPORT: {transport}"
				.format(transport = config["THRIFTCLIENT_TRANSPORT"])
				)

		#configure additionnal protocol layers
		if config["THRIFTCLIENT_BUFFERED"] == True:
			self.transport = TTransport.TBufferedTransport(self.transport)
		if config["THRIFTCLIENT_ZLIB"] == True:
			self.transport = TZlibTransport.TZlibTransport(self.transport)

		#configure thrift protocol
		if config["THRIFTCLIENT_PROTOCOL"] == ThriftClient.BINARY:
			self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
		elif config["THRIFTCLIENT_PROTOCOL"] == ThriftClient.COMPACT:
			self.protocol = TCompactProtocol.TCompactProtocol(self.transport)
		elif HAS_JSON_PROTOCOL and config["THRIFTCLIENT_PROTOCOL"] == ThriftClient.JSON:
			self.protocol = TJSONProtocol.TJSONProtocol(self.transport)
		else:
			raise RuntimeError(
				"invalid configuration for THRIFTCLIENT_PROTOCOL: {protocol}"
				.format(protocol = config["THRIFTCLIENT_PROTOCOL"])
				)

		#create the client from the interface
		self.client = self.interface(self.protocol)
