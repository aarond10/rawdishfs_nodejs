(function() {
  var async = require('./libs/async');
  var crypto = require('crypto');
  var fs = require('fs');
  var mkdirp = require('./libs/mkdirp');

  module.exports = {BlockStore:BlockStore};

  var MAX_BLOCK_SIZE = 16384; // 16KB max size per block.
  var BLOCK_STORE_SIZE = 128 << 20; // 128MB
  var OVERWRITE_MODE = true;  // true if its OK for a block to be overwritten once committed to disk.

  // Simple disk-backed key/value store with fixed maximum size.
  function BlockStore(path, callback) {
    this._path = path;
    this._size = 0;
    this._capacity = BLOCK_STORE_SIZE;
    var self = this;
    async.waterfall([
      function CreateDirectory(callback) {
        mkdirp.mkdirp(path, callback);
      },
      function GetOrCreateBlockID(made, callback) {
        var id_file = path + '/.id';
        fs.readFile(id_file, function(err, data) {
          if (err) {
            self._id = crypto.randomBytes(16).toString('hex');
            fs.writeFile(id_file, self._id, callback);
          } else {
            self._id = data.toString();
            callback(err);
          }
        });
      },
      function ReadExistingData(callback) {
        fs.readdir(path, callback);
      },
      function SumExistingData(files, callback) {
        async.forEach(files, function addSizes(file, callback) {
          if (file.substr(0,1) == '.')
            return callback();
          fs.stat(path + '/' + file, function statResult(err, stats) {
            console.log(self._size, stats.size);
            if (stats && stats.isFile())
              self._size += stats.size;
            callback(err);
          });
        }, callback);
      }
    ], callback);

    // Return the total size of the BlockStore.
    this.capacity = function() {
      return this._capacity;
    }

    // Returns the current size of BlockStore in bytes (used capacity)
    this.GetSize = function(callback) {
      callback(undefined, self._size);
    }
      
    // Returns the ID of this block store.
    this.id = function() {
      return Buffer(self._id).toString();
    }
      
    // Retrieve a block. Key should be a hexadecimal value but this is not checked.
    this.get = function(key, callback) {
      fs.readFile(self._path + '/' + key, callback);
    }

    // Store a block. Key should be a hexadecimal value but this is not checked.
    this.store = function(key, block, callback) {
      // Hack to force data into string form.
      if (block.length === undefined) 
        block = '' + block;
      if (block.length > MAX_BLOCK_SIZE) {
	return callback("Attempted to write oversized block (" + block.length + " > " + MAX_BLOCK_SIZE + ")");
      }
      if (self._size + block.length > self._capacity) {
	return callback("Store failed. BlockStore is full.");
      }
      fs.stat(self._path + '/' + key, function statResult(err, stats) {
        if (stats) {
          if (OVERWRITE_MODE)
            self._size -= stats.size;
          else
            return callback("Refusing to overwrite existing block: " + key);
        }
        self._size += block.length;
        fs.writeFile(self._path + '/' + key, block, function writeDone(err) {
	  if (err)
	    self._size -= block.length;
	  callback(err);
        });
      });
    }
  }


})();
