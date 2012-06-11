(function() {
  var async = require('async');
  var crypto = require('crypto');
  var fs = require('fs');
  var mkdirp = require('mkdirp');

  module.exports = {BlockStore:BlockStore};

  var MAX_BLOCK_SIZE = 16384; // 16KB max size per block.
  var BLOCK_STORE_SIZE = 128 << 20; // 128MB
  var OVERWRITE_MODE = true;  // true if its OK for a block to be overwritten once committed to disk.
  var SCANNER_INTERVAL = 50;  // time between incremental file scans in milliseconds

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
      function StatDirAndGetDevID(made, callback) {
        fs.stat(path, callback);
      },
      function GetOrCreateBlockID(stats, callback) {
        // The device ID is exported as a way to track the level of 
        // redundency the BlockStore provides to remote nodes.
        self.dev = stats.dev;

        var id_file = path + '/.id';
        fs.readFile(id_file, function(err, data) {
          if (err) {
            // Same length as SHA1 hash.
            self.id = crypto.randomBytes(20).toString('hex');
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
      var self_accessor = this;
      if (!uid_regexp.test(uid))
        return console.warning("User attempted to setuid with invalid ID (", uid, ")");

      // Copy the BlockStore ID from our parent for identification purposes
      this.id = self.id;
      // Returns the current uid of this blockstore.
      this.getuid = function(callback) { callback(null, uid); }
      // Returns the quota for this BlockStore and uid.
      this.quota = function(callback) { self.quota(function(err, quota) { callback(err, quota[uid]||0); }); }
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
        var filesize_change = block.length;
	$quota[uid] = ($quota[uid] || 0) + block.length;  // Add quota first, remove if we get errors. Avoids race issues.
	fs.stat(path + '/' + uid + '$' + key, function statResult(err, stats) {
	  if (stats) {
	    if (OVERWRITE_MODE) {
	      $quota[uid] -= stats.size;
              filesize_change -= stats.size;
            } else
	      return callback("Refusing to overwrite existing block: " + key);
	  }
	  fs.writeFile(path + '/' + uid + '$' + key, block, 'binary', function writeDone(err) {
	    if (err)
	      $quota[uid] -= block.length;
	    callback(err, filesize_change);
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
      // Starts an incremental scanner that triggers a callback for every
      // user owned block in the BlockStore, with the key name of the block.
      // Only runs through the block once.
      this.scan_once = function(file_callback, done_callback) {
	var handle = null;
        async.waterfall([
          function(callback) { fs.readdir(path, callback); },
          function(files, callback) {
            var curFiles = files;
	    function RunScan() {
	      var filename = null;
              var match = null;
	      while (filename !== undefined) {
                match = file_regexp.exec(filename);
                if (match && match[1] == uid)
                  break;
	        filename = curFiles.shift();
              }
	      if (filename) {
	        handle = setTimeout(RunScan, SCANNER_INTERVAL);
	        file_callback(null, match[2], function StopScan() { clearTimeout(handle); });
	      } else {
	        callback();
              }
	    }
	    handle = setTimeout(RunScan, SCANNER_INTERVAL);
          }
        ], function(err) {
          if (err)
            console.error("Got error in scan_once", err);
          done_callback(err);
        });
      }
      // Same as scan_once but runs continuously.
      this.scanner = function(callback) {
        self_accessor.scan_once(callback, function(err) {
          if (err)
            console.log("Got error in scanner", err);
          self_accessor.scanner(callback);
        });
      }
    }

    // Kicks off an incremental scan of the blockstore, calling the provided
    // callback function for every block of data in the store for every user.
    this.scanner = function(callback) {
      var handle = null;
      var curFiles = [];

      function RunScan() {
	var filename = null;
	while (filename !== undefined && !file_regexp.test(filename)) 
	  filename = curFiles.shift();
	if (filename) {
	  fs.readFile(path + '/' + filename, 'binary', function ReadFile(err, data) {
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
