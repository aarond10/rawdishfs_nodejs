(function() {
  var async = require('./libs/async');
  var crypto = require('crypto');
  var events = require('events');
  var util = require('util');

  module.exports = {getReadStream:getReadStream, getWriteStream:getWriteStream};

  // Creates or opens a FileSystem
  function FileSystem(mesh, name, password, callback) {
    var root_keys = [];
    var root_dir = { seq_id:-1, files:[], subdirs:[] };
    var root_dirs = [];
    // Each file system has 4 root directory locations written to in round-robin order with identity
    // determined based on hash of name, password and index.
    for (var i = 0; i < 4; i++)
      root_keys.push(crypto.createHash('sha1').update(name + '_' + password + '_' + i).digest('hex'));

    // We don't know what blockstore's hold our root directory data so we have to search all of them.
    var blockstore = mesh.GetBlockStores();
    async.forEach(root_keys, function searchForRootMatchingKey(key, callback) {
      var key_bs = null;
      async.forEachLimit(blockstore, 32, function(bs, callback) {
        bs.get(key, function gotRootDir(err, data) {
          if (err) // skip errors (file not found, etc)
            return callback();
          try {
            var directory = JSON.parse(data.toString());
            if (directory.seq_id > root_dir.seq_id) {
              root_blockstores = [];
              for (var i = 0; i < 4; i++)
                root_dirs.push([]);
              root_dir = directory;
            }
            if (directory.seq_id == root_seq_id)
              root_dirs[i].push(directory);
            callback();
          } catch(e) {
            callback(e);
          }
        });
      }, function searchedAllBlockStores(err) {
        if (key_bs !== null) {
          root_blockstores.push(key_bs);
        } else {
          root_blockstores.push(mesh.SelectBlockStore());
        }
        callback(err);
      });
    }, callback);
  }

  // Returns an EventEmitter that acts similar to an fs.ReadStream but fetches file contents from
  // local and remote blockstores. 
  function getReadStream(filename, blockstores) {
    var emitter = new events.EventEmitter();
    // Note: Trivial test algorithm.
    function nextBlock(i) {
      var hash = crypto.createHash('sha1');
      hash.update(filename + i);
      var key = hash.digest('hex');
      console.log('Fetching block', key, 'from blockstore', i);
      blockstores[i%blockstores.length].get(key, function gotBlock(err, data) {
        if (err) {
          emitter.emit('end');
        } else {
          emitter.emit('data', data);
          nextBlock(i+1);
        }
      });
    }
    nextBlock(0);
    return emitter;
  }

  function getWriteStream(filename, blockstores) {
    var writer = {};
    // Note: Trivial test algorithm
    var i = 0;
    writer.write = function(data, callback) {
      var pos = 0;
      var ops = [];
      while (pos < data.length) {
        var hash = crypto.createHash('sha1');
        hash.update(filename + i);
        var key = hash.digest('hex');
        var len = Math.min(16384, data.length - pos);
        ops.push({ix:i%blockstores.length, key:key, start:pos, end:pos + len});
        i++;
        pos += len;
      }
      async.forEach(ops, function doWrite(op, callback) {
        blockstores[op.ix].store(op.key, data.slice(op.start, op.end), callback);
      }, callback);
    };
    return writer;
  }

})();
