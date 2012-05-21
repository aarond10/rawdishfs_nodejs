(function() {
  var async = require('async');
  var crypto = require('crypto');
  var fs = require('fs');
  var mkdirp = require('mkdirp');

  module.exports = {BlockStore:BlockStore};

  var MAX_BLOCK_SIZE = 16384; // 16KB max size per block.
  var BLOCK_STORE_SIZE = 128 << 20; // 128MB
  var OVERWRITE_MODE = true;  // true if its OK for a block to be overwritten once committed to disk.
  var SCANNER_INTERVAL = 100;  // time between incremental file scans in milliseconds

  var key_regexp = /[a-fA-F0-9]+/;  // We assume blocks keys are hex hashes
  var uid_regexp = /[a-zA-Z0-9][a-zA-Z0-9_\.\-]*/;
  var file_regexp = /([a-zA-Z0-9][a-zA-Z0-9_\.\-]*)\$([a-fA-F0-9]+)/;  // matches uid and key.

  // Simple disk-backed key/value store with fixed maximum size.
  function BlockStore(path, callback) {
    var $quota = {};  // per-user quotas.
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
          var match = file_regexp.exec(file);
          if (!match || match.length != 3)
            return callback();
          fs.stat(path + '/' + file, function statResult(err, stats) {
            if (stats && stats.isFile())
              $quota[match[1]] = ($quota[match[1]] || 0) + stats.size
            callback(err);
          });
        }, callback);
      },
    ], callback);

    // Returns the per-user quota levels for this BlockStore.
    this.quota = function(callback) { callback(null, $quota); }

    // Returns the size of the blockstore (i.e. its capacity).
    this.size = function(callback) { callback(null, BLOCK_STORE_SIZE); }

    // Returns an object that can be used to read/write to the store but only
    // under a single user ID.
    this.accessor = function(uid) {
      if (!uid_regexp.test(uid))
        return console.warning("User attempted to setuid with invalid ID (", uid, ")");

      // Copy the BlockStore ID from our parent for identification purposes
      this.id = self.id;
      // Returns the current uid of this blockstore.
      this.getuid = function(callback) { callback(null, uid); }
      // Returns the quota for this BlockStore and uid.
      this.quota = function(callback) { self.quota(function(err, quota) { callback(err, quota[uid]); }); }
      // Returns the size of the blockstore (i.e. its capacity) 
      // TODO: Should we subtract other users data so we can be essentially ignorant of it?
      this.size = self.size.bind(self);
      // Retrieve a block.
      this.get = function(key, callback) {
        if (!key_regexp.test(key))
          return callback("Invalid key: " + key);
        fs.readFile(path + '/' + uid + '$' + key, callback);
      }
      // Store a block.
      this.store = function(key, block, callback) {
	if (!key_regexp.test(key))
	  return callback("Invalid key: " + key);
	if (typeof block.length === undefined)  // Hack to force data into string form.
	  block = '' + block;
	if (block.length > MAX_BLOCK_SIZE) {
	  return callback("Attempted to write oversized block (" + block.length + " > " + MAX_BLOCK_SIZE + ")");
	}
	var size = 0;
	for (var i in $quota) size += $quota[i];
	if (size + block.length > BLOCK_STORE_SIZE) {
	  return callback("Store failed. BlockStore is full.");
	}
	$quota[uid] = ($quota[uid] || 0) + block.length;  // Add quota first, remove if we get errors. Avoids race issues.
	fs.stat(path + '/' + uid + '$' + key, function statResult(err, stats) {
	  if (stats) {
	    if (OVERWRITE_MODE)
	      $quota[uid] -= stats.size;
	    else
	      return callback("Refusing to overwrite existing block: " + key);
	  }
	  fs.writeFile(path + '/' + uid + '$' + key, block, function writeDone(err) {
	    if (err)
	      $quota[uid] -= block.length;
	    callback(err);
	  });
	});
      }
      // Remove a block.
      this.remove = function(key, callback) {
	if (!key_regexp.test(key))
	  return callback("Invalid key: " + key);
	fs.stat(path + '/' + uid + '$' + key, function statResult(err, stats) {
          if (err)
            return callback(err);
	  fs.unlink(path + '/' + uid + '$' + key, function deleteDone(err) {
	    if (!err)
	      $quota[uid] -= stats.size;
	    callback(err);
	  });
	});
      }
    }

    // Kicks off an incremental scan of the blockstore, calling the provided
    // callback function for every block of data in the store.
    this.scanner = function(callback) {
      var handle = null;
      var curFiles = [];

      function RunScan() {
	var filename = null;
	while (filename !== undefined && !file_regexp.test(filename)) 
	  filename = curFiles.shift();
	if (filename) {
	  fs.readFile(path + '/' + filename, function ReadFile(err, data) {
	    var match = file_regexp.exec(filename);
	    handle = setTimeout(RunScan, SCANNER_INTERVAL);
	    callback(err, match[2], data, match[1]);
	  });
	} else {
	  fs.readdir(path, function GotDir(err, files) {
	    if (!err)
	      curFiles = files;
	    handle = setTimeout(RunScan, SCANNER_INTERVAL);
	  });
	}
      }
      handle = setTimeout(RunScan, SCANNER_INTERVAL);

      // Stops this incremental scanner.
      this.stop = function() {
        clearTimeout(handle);
      }
    }
  }
})();
