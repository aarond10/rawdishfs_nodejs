(function() {
  var async = require('./libs/async');
  var dnode = require('dnode');
  var crypto = require('crypto');
  var fslayer = require('./fslayer');
  var https = require('https');
  var url = require('url');
  var util = require('util');

  module.exports = {createServer:createServer, Peer:Peer};

  //////////////////////////////////////

  // Creates a read / write message stream compressed with zlib containing arbitrary messages encoded with JSON.
  function MessageStream(stream) {
    Stream.call(this);
    this._buffer = new Buffer();
    var self = this;
    var istream = this.pipe(zlib.createGunzip());
    var ostream = zlib.createGzip();
    ostream.pipe(this);
    istream.on('data', function onData(data) {
      var ix = data.indexOf('\n');
      if (ix == -1) {
        self._buffer += data;
      } else {
        self.emit('message', JSON.decode(self._buffer + data.slice(0, ix)));
        self._buffer = data.slice(ix+1, data.length);
      }
    });
  }
  util.inherits(MessageStream, Stream);

  MessageStream.prototype.send = function(msg) {
    ostream.write(JSON.stringify(msg)+'\n');
  }

  //////////////////////////////////////

  // Represents a peer connect to this node.
  function Peer(cert, stream, blockstores, peers) {
    this.cert = cert;
    this.stream = new MessageStream(stream);
    this.blockstores = blockstores;
    this.peers = peers;

    this.stream.on('message', function onMsg(msg) {
      if (msg.type == 'ping') {
        msgstream.send({type:'pong'});
      } else if (msg.type == 'peer') {
        var key = JSON.stringify([msg.host, msg.port])
        if (!self.peers[key])
          self.peers[key] = self.connect(msg.host, msg.port);
      } else if (msg.type == 'blockstore') {
        if (!self.blockstores[msg.blockstore_id])
          self.blockstores[msg.blockstore_id] = new PeerBlockStore(msg.host, msg.port);
      } else if (msg.type == 'get') {
        if (self.blockstores[msg.blockstore_id])
          self.blockstores[msg.blockstore_id].get(msg.key, function onGetData(data) {
            self.stream.send({
              type: 'getResponse',
              blockstore_id: msg.blockstore_id,
              key: msg.key,
              data: data
            });
          });
      } else if (msg.type == 'store') {
      } else if (msg.type == 'capacity') {
      } else if (msg.type == 'size') {
      }
    }
      msgstream.onClose = function() {
        console.log('Closed connection to', stream.address());
      }
      msgstream.onError = function(e) {
        console.log('Error on connection to', stream.address(), ':', e);
      }
  }

  function PeerServer(port, key, cert, ca, peers, blockstores) {
    this.options = {
      key: key,
      cert: cert,
      ca: [ ca ],
      requestCert: true,
      rejectUnauthorized: true,
    }
    this.port = port;
    this.peers = peers;
    this.blockstores = blockstores;
    var self = this;

    this.server = tls.createServer(options, function tlsData(stream) {
      console.log('New peer:', stream.getPeerCertificate());

      var msgstream = new MessageStream(stream);
    });
    this.server.listen(port, function() {
      console.log('Listening for RPCs at localhost:' + port + '/');
    }
  }

  PeerServer.prototype.ProcessMessage = function(msg) {
  }

  PeerServer.prototype.connect = function(host, port) {
    
  }

  // Creates a server for RPC connections
  function createServer(port, key, cert, ca, peers, blockstores) {
    return new PeerServer(port, key, cert, ca, peers, blockstores);
  }

  // Connects to a peer and shares our peers and blockstores with it
  function connect(host, port, key, cert, ca, peers, blockstores) {
    var options = {
      host: host,
      port: port,
      path: '/rpc/connect',
      method: 'GET'
    https.request({
  }
  

})();
