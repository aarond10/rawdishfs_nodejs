(function() {
  var dnode = require('dnode');

  module.exports = {MeshNode:MeshNode};

  function MeshNode(params, blockstore) {
    var self = this;
    this.$params = params;
    this.$blockstore = blockstore;
    this.$peers = {};
    this.$server = dnode(function(client, conn) {
      // conn.stream is not created until *after* this function is called so
      // we defer exposing anything to the user until authenticate() is called.
      this.authenticate = function authenticate(callback) {
        var owner = conn.stream.getPeerCertificate()
                        .subject.emailAddress.replace('@','_');
        console.log("New connection from " + owner + " at address " + 
                    conn.stream.remoteAddress + ':' + conn.stream.remotePort);

        // If we can expose a user-restricted blockstore, do it. This will 
        // prevent the user from being able to tromp over other's files and
        // allow us to track per-user usage towards quota.
        var user_blockstore = null;
        if (blockstore.SpecializeToOwner !== undefined)
          user_blockstore = blockstore.SpecializeToOwner(owner);
        else
          user_blockstore = blockstore;

        // Give the user what they want.
        callback({
          blockstore: user_blockstore,
          add_peer: self.connect.bind(self),
        });
      };
    }).listen(params);
  };

  // Returns a list of all peers and their states.
  MeshNode.prototype.peers = function() {
    var ret = [];
    for (var i in this.$peers)
      ret.push({
        owner: i,
        host: this.$peers[i].host, 
        port: this.$peers[i].port, 
        state: this.$peers[i].state, 
      })
    return ret;
  }

  // Returns all connected blockstores as one collective object.
  // If duplicate IDs exist, one will overwrite the other.
  MeshNode.prototype.blockstores = function() {
    var blockstores = {};
    for (var i in this.$blockstore)
      blockstores[i] = this.$blockstore[i];
    for (var i in this.$peers)
      if (this.$peers[i].state == 'connected')
        for (var j in this.$peers[i].blockstore) {
          if (blockstores[j] !== undefined)
            console.error("BlockStore collision for ID", j, "and peer", i);
          blockstores[j] = this.$peers[i].blockstore[j];
        }
    return blockstores;
  };

  MeshNode.prototype.connect = function(host, port) {
    var self = this;
    // (Note this doesn't completely get around the issue of multiple addresses. 
    // e.g. DNS and IP, multi-interfaces, etc..)
    if (host != this.$params.host || port != this.$params.port) {
      console.info('MeshNode.connect(' + host + ', ' + port + ')');

      // Build a new params structure based on our server one - this lets us 
      // use our SSL certs (if specified).
      var params = JSON.parse(JSON.stringify(self.$params));
      params.host = host;
      params.port = port;
      params.reconnect = 1000;

      dnode.connect(params, function StartConnection(client, conn) {
        client.authenticate(function CompleteConnection(data) {
          var owner = conn.stream.getPeerCertificate()
                          .subject.emailAddress.replace('@','_');
          // Disconnect if we've already connected to this peer.
          if (self.$peer[owner] !== undefined) {
            conn.end();
            return;
          }

          // Build full mesh.
          data.add_peer(self.$params.host, self.$params.port);
          for (var i in self.$peers)
            data.add_peer(self.$peers[i].host, self.$peers[i].port);

          // Keep track of thie peer and its connection state
          var peer = { 
            host: host,
            port: port,
            state: 'connecting',
            blockstore: data.blockstore,
          };
          self.$peer[owner] = peer;

	  conn.on('ready', function() { 
	    peer.state = 'connected';
	    peer.client = client;
	    peer.conn = conn;
	  });
	  conn.on('drop', function() { 
	    peer.state = 'disconnected';
	  });
	  conn.on('end', function() { 
	    peer.state = 'ended'; 
	    //delete(self.$peers[owner]); ??
          });
	});
      });
    }
  };
})();
