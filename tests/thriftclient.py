# -*- coding:utf-8 -*-

from thrift.transport import *
from thrift.transport import TSSLSocket
from thrift.protocol import *
from thrift.protocol import TCompactProtocol

import unittest

from flask import Flask
from flask_thriftclient import ThriftClient


class StubClient:

    def __init__(self, protocol):
        pass


class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.app = Flask(__name__)

    def test_default_values(self):
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSocket.TSocket))
        self.assertTrue(isinstance(client.protocol,
                                   TBinaryProtocol.TBinaryProtocol))
        self.assertEquals(client.transport.port, 9090)
        self.assertEquals(client.transport.host, "localhost")

    def test_transport_bad_scheme(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "bad://whatever"
        with self.assertRaises(RuntimeError):
            client = ThriftClient(StubClient, self.app)

    def test_transport_none_transport(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = None
        with self.assertRaises(RuntimeError):
            client = ThriftClient(StubClient, self.app)

    def test_transport_empty_url(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = ""
        with self.assertRaises(RuntimeError):
            client = ThriftClient(StubClient, self.app)

    def test_transport_no_scheme(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "/tmp/somefile"
        with self.assertRaises(RuntimeError):
            client = ThriftClient(StubClient, self.app)

    def test_transport_tcp_noport(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "tcp://192.168.0.42"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSocket.TSocket))
        self.assertEquals(client.transport.port, 9090)
        self.assertEquals(client.transport.host, "192.168.0.42")

    def test_transport_tcp_longurl(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "tcp://mydomain.foo.com:5921/whatever?its=21;not=zrzer#used"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSocket.TSocket))
        self.assertEquals(client.transport.port, 5921)
        self.assertEquals(client.transport.host, "mydomain.foo.com")

    def test_transport_unix_1(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "unix:///tmp/testunixsocket"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSocket.TSocket))
        self.assertEquals(client.transport._unix_socket, "/tmp/testunixsocket")

    def test_transport_unix_2(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "unix:/tmp/testunixsocket"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSocket.TSocket))
        self.assertEquals(client.transport._unix_socket, "/tmp/testunixsocket")

    def test_transport_unix_bad(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "unix://tmp/testunixsocket"
        with self.assertRaises(RuntimeError):
            client = ThriftClient(StubClient, self.app)

    def test_transport_http(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "http://foo.bar.com:8080/end/point"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, THttpClient.THttpClient))
        self.assertEquals(client.transport.scheme, "http")
        self.assertEquals(client.transport.host, "foo.bar.com")
        self.assertEquals(client.transport.port, 8080)
        self.assertEquals(client.transport.path, "/end/point")

    def test_transport_https(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "https://foo.bar.com:8080/end/point"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, THttpClient.THttpClient))
        self.assertEquals(client.transport.scheme, "https")
        self.assertEquals(client.transport.host, "foo.bar.com")
        self.assertEquals(client.transport.port, 8080)
        self.assertEquals(client.transport.path, "/end/point")

    def test_transport_http_defaultport(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "http://foo.bar.com/end/point"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, THttpClient.THttpClient))
        self.assertEquals(client.transport.scheme, "http")
        self.assertEquals(client.transport.port, 80)

    def test_transport_defaultport(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "https://foo.bar.com/end/point"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, THttpClient.THttpClient))
        self.assertEquals(client.transport.scheme, "https")
        self.assertEquals(client.transport.port, 443)

    def test_transport_tcps_no_cert(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "tcps://192.168.0.42"
        self.app.config["THRIFTCLIENT_SSL_VALIDATE"] = False
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSSLSocket.TSSLSocket))
        self.assertEquals(client.transport.port, 9090)
        self.assertEquals(client.transport.host, "192.168.0.42")

    def test_transport_tcps_with_cert(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "tcps://192.168.0.42"
        self.app.config["THRIFTCLIENT_SSL_CA_CERTS"] = "tests/cacert.pem"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSSLSocket.TSSLSocket))
        self.assertEquals(client.transport.port, 9090)
        self.assertEquals(client.transport.host, "192.168.0.42")

    def test_transport_tcps_forgot_cert(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "tcps://192.168.0.42"
        self.app.config["THRIFTCLIENT_SSL_VALIDATE"] = True
        self.app.config["THRIFTCLIENT_SSL_CA_CERTS"] = None

        with self.assertRaises(IOError):
            client = ThriftClient(StubClient, self.app)

    def test_transport_tcps_unreadable_cert(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "tcps://192.168.0.42"
        self.app.config["THRIFTCLIENT_SSL_VALIDATE"] = True
        self.app.config["THRIFTCLIENT_SSL_CA_CERTS"] = "missingcert"

        with self.assertRaises(IOError):
            client = ThriftClient(StubClient, self.app)

    def test_transport_unixs_no_cert(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "unixs:/tmp/thriftsocketfile"
        self.app.config["THRIFTCLIENT_SSL_VALIDATE"] = False
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSSLSocket.TSSLSocket))
        self.assertEquals(client.transport._unix_socket,
                          "/tmp/thriftsocketfile")

    def test_transport_unixs_no_cert_2(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "unixs:///tmp/thriftsocketfile"
        self.app.config["THRIFTCLIENT_SSL_VALIDATE"] = False
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSSLSocket.TSSLSocket))
        self.assertEquals(client.transport._unix_socket,
                          "/tmp/thriftsocketfile")

    def test_transport_unixs_bad_hostname(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "unixs://tmp/thriftsocketfile"
        self.app.config["THRIFTCLIENT_SSL_VALIDATE"] = False
        with self.assertRaises(RuntimeError):
            client = ThriftClient(StubClient, self.app)

    def test_transport_unixs_with_cert(self):
        self.app.config[
            "THRIFTCLIENT_TRANSPORT"] = "unixs:/tmp/thriftsocketfile"
        self.app.config["THRIFTCLIENT_SSL_CA_CERTS"] = "tests/cacert.pem"
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.transport, TSSLSocket.TSSLSocket))
        self.assertEquals(client.transport._unix_socket,
                          "/tmp/thriftsocketfile")

    def test_protocol_bad(self):
        self.app.config["THRIFTCLIENT_PROTOCOL"] = "BAD"
        with self.assertRaises(RuntimeError):
            client = ThriftClient(StubClient, self.app)

    def test_protocol_binary(self):
        self.app.config["THRIFTCLIENT_PROTOCOL"] = ThriftClient.BINARY
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.protocol,
                                   TBinaryProtocol.TBinaryProtocol))

    def test_protocol_compact(self):
        self.app.config["THRIFTCLIENT_PROTOCOL"] = ThriftClient.COMPACT
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.protocol,
                                   TCompactProtocol.TCompactProtocol))

    def test_protocol_json(self):
        self.app.config["THRIFTCLIENT_PROTOCOL"] = ThriftClient.JSON
        client = ThriftClient(StubClient, self.app)
        self.assertTrue(isinstance(client.protocol,
                                   TJSONProtocol.TJSONProtocol))

    def test_connection(self):
        """
        http connections aren't really opened, so we can tests
        them without a server
        """
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "http://localhost:8735"
        client = ThriftClient(StubClient, self.app)

        @self.app.route("/testme")
        def testme():
            return "OK" if client.transport.isOpen() else "KO"

        testclient = self.app.test_client()
        ret = testclient.get("/testme")
        self.assertEquals(ret.data, "OK")
        self.assertFalse(client.transport.isOpen())

    def test_connection_no_server(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "tcp://localhost:8735"
        client = ThriftClient(StubClient, self.app)

        @self.app.route("/testme")
        def testme():
            return "KO"

        testclient = self.app.test_client()
        ret = testclient.get("/testme")
        self.assertEquals(ret.status_code, 500)
        self.assertFalse(client.transport.isOpen())

    def test_no_alwaysconnect(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "tcp://localhost:8735"
        self.app.config["THRIFTCLIENT_ALWAYS_CONNECT"] = False
        client = ThriftClient(StubClient, self.app)

        @self.app.route("/testme")
        def testme():
            return "KO" if client.transport.isOpen() else "OK"

        testclient = self.app.test_client()
        ret = testclient.get("/testme")
        self.assertEquals(ret.data, "OK")
        self.assertFalse(client.transport.isOpen())

    def test_connect_ctx(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "http://localhost:8735"
        self.app.config["THRIFTCLIENT_ALWAYS_CONNECT"] = False
        client = ThriftClient(StubClient, self.app)

        with client.connect():
            self.assertTrue(client.transport.isOpen())
        self.assertFalse(client.transport.isOpen())

    def test_autoconnect(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "http://localhost:8735"
        self.app.config["THRIFTCLIENT_ALWAYS_CONNECT"] = False
        client = ThriftClient(StubClient, self.app)

        @self.app.route("/testme")
        @client.autoconnect
        def testme():
            return "OK" if client.transport.isOpen() else "KO"

        testclient = self.app.test_client()
        ret = testclient.get("/testme")
        self.assertEquals(ret.data, "OK")
        self.assertFalse(client.transport.isOpen())

    def test_autoconnect_with_alwaysconnect(self):
        self.app.config["THRIFTCLIENT_TRANSPORT"] = "http://localhost:8735"
        self.app.config["THRIFTCLIENT_ALWAYS_CONNECT"] = False
        client = ThriftClient(StubClient, self.app)

        @self.app.route("/testme")
        @client.autoconnect
        def testme():
            return "OK" if client.transport.isOpen() else "KO"

        testclient = self.app.test_client()
        ret = testclient.get("/testme")
        self.assertEquals(ret.data, "OK")
        self.assertFalse(client.transport.isOpen())


if __name__ == "__main__":
    unittest.main()
