(function() {
  var async = require('./libs/async');
  var crypto = require('crypto');
  var fs = require('fs');
  var mkdirp = require('./libs/mkdirp');
  var util = require('util');

  module.exports = {BlockStore:BlockStore};

  var MAX_BLOCK_SIZE = 16384; // 16KB max size per block.
  var BLOCK_STORE_SIZE = 128 << 20; // 128MB
  var OVERWRITE_MODE = true;  // true if its OK for a block to be overwritten once committed to disk.

  // Simple disk-backed key/value store with fixed maximum size.
  function BlockStore(path, callback) {
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
            self.id = crypto.randomBytes(16).toString('hex');
            fs.writeFile(id_file, self.id, callback);
          } else {
            self.id = data.toString();
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

    // Returns the current size of BlockStore in bytes (used capacity)
    this.GetSize = function(callback) {
      callback(undefined, self._size);
    }
      
    // Retrieve a block. Key should be a hexadecimal value but this is not checked.
    this.get = function(key, callback) {
      fs.readFile(path + '/' + key, callback);
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
      fs.stat(path + '/' + key, function statResult(err, stats) {
        if (stats) {
          if (OVERWRITE_MODE)
            self._size -= stats.size;
          else
            return callback("Refusing to overwrite existing block: " + key);
        }
        self._size += block.length;
        fs.writeFile(path + '/' + key, block, function writeDone(err) {
	  if (err)
	    self._size -= block.length;
	  callback(err);
        });
      });
    }
  }

  // Extends BlockStore by creating a shim for remote users to use,
  // adding owner metadata and exporting per-user usage.
  function MetaBlockStore(path, callback) {
    BlockStore.call(this, path, callback);

    var self = this;
    var storeFunc = this.store;
    var getFunc = this.get;

    // Returns an object that looks like a plain old BlockStore but is 
    // actually backed by a MetaBlockStore and transparently sets
    // owner data in the background for stores and prevents gets of other
    // users data.
    this.GetUserBlockStore = function(owner) {
      this._size = self._size;
      this.id = self.id;
      this._capacity = self._capacity;

      this.GetSize = function(callback) {
        this._size = self._size;
        callback(null, this._size);
      };

      this.store = function(key, block, callback) {
	if (key.find(".") != -1)
	  return callback("Invalid key name: " + key);
	async.waterfall([
	  function(callback) {
	    self.get(key + ".owner", function(err, data) {
              if (data && data.toString() != owner)
                return callback("Permission denied."); // file exists and owned by someone else.
              callback(null); // potential small race condition here?
            });
	  },
	  function(callback) {
	    self.store(key + ".owner", owner, callback);
	  },
	  function(callback) {
	    self.store(key, block, callback);
	  },
	], callback);
      };

      this.get = function(key, callback) {
	if (key.find(".") != -1)
	  return callback("Invalid key name: " + key);
	async.waterfall([
	  function(callback) {
	    self.get(key + ".owner", callback);
	  },
	  function(data, callback) {
	    fileowner = data.toString();
            if (fileowner != owner)
              callback("Permission denied.");
            else
	      self.get(key, callback);
	  },
	], callback);
      };
    }
  }
  util.inherits(MetaBlockStore, BlockStore);
})();
