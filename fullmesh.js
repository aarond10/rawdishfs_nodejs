(function() {
  var crypto = require('crypto');
  var dnode = require('dnode');
  var events = require('events');
  var util = require('util');

  module.exports = {MeshNode:MeshNode};

  function MeshNode(params, local_blockstore) {
    events.EventEmitter.call(this);
    var self = this;
    this._params = params;
    this._blockstore = local_blockstore;
    this._peers = {};
    this._server = dnode(function(client, conn) {
      this.blockstore = local_blockstore;
      this.AddPeer = function AddPeer(host, port) { self.connect(host, port); };
    }).listen(params);
  };
  util.inherits(MeshNode, events.EventEmitter);

  // Returns all blockstores merged into one giant map. 
  // If duplicate IDs exist, one will overwrite the other.
  MeshNode.prototype.blockstores = function() {
    var blockstores = {};
    for (var i in this._blockstore)
      blockstores[i] = this._blockstore[i];
    for (var i in this._peers)
      if (this._peers[i].state == 'connected')
        for (var j in this._peers[i].client.blockstore)
          blockstores[j] = this._peers[i].client.blockstore[j];
    return blockstores;
  };

  MeshNode.prototype.connect = function(host, port) {
    var self = this;
    var key = JSON.stringify([host, port]);
    // Only connect if we aren't already.
    // (Note this doesn't completely get around the issue. DNS and IP can be many-to-one.)
    if (this._peers[key] === undefined && (host != this._params.host || port != this._params.port)) {
      console.info('MeshNode.connect(' + host + ', ' + port + ')');
      // Mark yet-to-be-connected peer in our table so we know not to connect to it again.
      this._peers[key] = {host:host, port:port, state:'connecting'};

      // Build a new params structure based on our server one - this lets us use SSL 
      // certs (if specified).
      var params = {host:host, port:port, reconnect:1000};
      for (var i in this._params)
        if(params[i] === undefined)
          params[i] = this._params[i];

      dnode.connect(params, function(client, conn) {
        // Build full mesh.
        client.AddPeer(self._params.host, self._params.port);
        for (var i in self._peers)
          client.AddPeer(self._peers[i].host, self._peers[i].port);

        // Incrementally update used disk in remote BlockStores.
        var blockstore_ids = [];
        var blockstore_id_ix = 0;
        setInterval(function() {
          if (blockstore_ids.length != 0 && self._peers[key].state == 'connected') {
            blockstore_id_ix = (blockstore_id_ix+1) % blockstore_ids.length;
            var id = blockstore_ids[blockstore_id_ix];
            client.blockstore[id].GetSize(function(err, size) {
              if (!err)
                client.blockstore[id]._size = size;
            });
          }
        }, 10000);

        conn.on('ready', function() { 
          self._peers[key].state = 'connected';
          self._peers[key].client = client;
          self._peers[key].conn = conn;
          for (var i in client.blockstore)
            blockstore_ids.push(client.blockstore[i].id);
          self.emit('ready', self._peers[key]);
        });
        conn.on('drop', function() { 
          self._peers[key].state = 'disconnected';
          self.emit('drop', self._peers[key]);
        });
        conn.on('reconnect', function() { 
          //self._peers[key].state = 'connected';
          self.emit('reconnect', self._peers[key]);
        });
        conn.on('end', function() { self._peers[key].state = 'ended'; /*delete(self._peers[key]);*/ self.emit('end', params); });
      });
    }
  };

  // Returns a local or remote BlockStore for storing an arbitrary block.
  MeshNode.prototype.SelectBlockStore = function() {
    var blockstores = this.blockstores();
    var free_space = 0;
    for (var i in blockstores)
      free_space += blockstores[i]._size;
    var ix = parseInt(Math.random()*free_space);
    
    for (var i in blockstores) {
      if (ix <= 0)
        return blockstores[i];
      ix -= blockstores[i]._size;
    }

    console.error("Whoops. This shouldn't be possible.");
    return null;
  };

  // Looks after coding and distribution of data blocks, maintenance of those
  // data blocks, chaining of blocks, HMAC verification, encryption, etc. 
  // Basically provides a unified view of storage backed by a set of BlockStores.
  function DataStore(mesh, rootdir_key) {
    this.get = function(key, callback) {
      var result = null;
      var result_blockstore = null;
      async.forEachLimit(mesh.blockstores(), 32, function(blockstore, callback) {
        blockstore.get(key, function(err, data) {
          if (data) {
            result = data;
            result_blockstore = blockstore;
            callback(true); // force forEachLimit to stop.
          } else {
            callback();
          }
        });
      }, function(err) {
        // err is not important.
        if (result)
          callback(null, result, result_blockstore);
        else
          callback("File not found.");
      });
    };

    this.store = function(key, data, callback) {
      this.get(key, function(err, data, blockstore) {
        // TODO: Deal with race condition here between get and store.
        if (err) {
          mesh.SelectBlockStore().store(key, data, callback);
        } else {
          blockstore.store(key, data, callback);
        }
      });
    };

    // Returns a jsDAV compatible directory object.
    this.getRootDir = function(callback) {
      callback(null, DataStoreDAV.CreateRootDir(this, rootdir_key));
    }
  }
})();

