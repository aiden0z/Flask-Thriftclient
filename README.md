# Flask ThriftClient

This extension provide a simple intergration with remote thrift
services.

~~~python
from flask import Flask
from flask_thriftclient import ThriftClient

from MyGeneratedThriftCode import MyService

app = Flask(__name__)
app.config["THRIFTCLIENT_TRANSPORT"] = "tcp://127.0.0.1:9090"

thriftclient = ThriftClient(MyService.Client, app)

@app.route("/")
def home():
    data = thriftclient.client.mymethod()
    return data
~~~

## Transport

Thrift endpoints are defined in the configuration variable
THRIFTCLIENT_TRANSPORT as an URL. The default transport is
tcp://localhost:9090 

Available url schemes are:

tcp: use TCP socket as transport, you have to define the server
address and port. If the port isn't defined, 9090 will be used  

Example:

  * tcp://127.0.0.1

  * tcp://localhost:1234/


http: use HTTP protocol as transport. Examples:

  * http://myservice.local/

unix: use unix sockets as transport, as this scheme follow URI format,
it *MUST* have either no or three "/" before the socket path 

  * unix:///tmp/mysocket #absolute path

  * unix:/tmp/mysocket #absolute path

  * unix:./mysocket #relative path

## SSL

You may set SSL version of transport communications by using *'s'*
version of url scheme:

tcp <=> tcps
http <=> https
unix <=> unixs

examples:

  * https://myserver/

  * unixs:/tmp/mysocket

  * tcps://localhost:5533/

Two options are related to SSL transport:

THRIFTCLIENT_SSL_VALIDATE: True if the certificate has to be validated
(default True)

THRIFTCLIENT_SSL_CA_CERTS: path to the SSL certificate (default None)

Note that you *MUST* set one of theses options:

~~~python
app.config["THRIFTCLIENT_SSL_VALIDATE"] = False
app.config["THRIFTCLIENT_TRANSPORT"] = "https://127.0.0.1/"

#or

app.config["THRIFTCLIENT_SSL_CA_CERTS"] = "./cacert.pem"
app.config["THRIFTCLIENT_TRANSPORT"] = "https://127.0.0.1/"

~~~


## Protocol

You may define which procotol must be use by setting the parametter
*THRIFTCLIENT_PROTOCOL*. The default protocol is Binary. 

Available parametters are: 

ThriftClient.BINARY or "BINARY" : use the binary protocol

ThriftClient.COMPACT or "COMPACR" : use the compact protocol

ThriftClient.JSON or "JSON" : use the JSON protocol. note that this
protocol is only available for thrift >= 0.9.1


## Options

Other options are:

THRIFTCLIENT_BUFFERED: use buffered transport (default False)

THRIFTCLIENT_ZLIB: use zlib compressed transport (default False)