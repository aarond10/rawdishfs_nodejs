(function() {
  var async = require('async');
  var crypto = require('crypto');

  module.exports = {UnifiedBlockStore:UnifiedBlockStore};

  var MAX_BLOCK_SIZE = 16384; // 16KB max size per block.
  var SCANNER_INTERVAL = 100;  // time between incremental file scans in milliseconds

  var key_regexp = /[a-fA-F0-9]+/;  // We assume blocks keys are hex hashes

  // Combines several BlockStore.accessor's and provides a unified storage set
  // for blocks stored across all of them using consistent hashing. There is
  // assumed to be only one UnifiedBlockStore per userid at any given time.
  // Upon first connection, blocks that are misplaced are moved (using a get()
  // and store() followed by a remove()). From there on out, blocks are placed in
  // locations based on their consistent hash value.
  function UnifiedBlockStore(uid) {
    var $quota = 0;
    var $capacity = 0;
    var self = this;
    var $bsids = [];
    var $accessor = [];

    // Given a key, returns the BlockStore ID of the store it should be 
    // assigned to.
    function AccessorIdFromKey(key) {
      var hash = crypto.createHash("sha1");
      hash.update(key);
      hash = hash.digest('hex');
      // Note that $bsids is stored sorted.
      for (var i = 0; i < $bsids.length; ++i) {
        //console.log('hash', hash, '$bsids[i]', $bsids[i]);
        if (hash < $bsids[i]) {
          //console.log('i', i, 'hash', hash, ' < $bsids[i]', $bsids[i]);
          return $bsids[((i + $bsids.length - 1) % $bsids.length)];
        }
      }
      return $bsids[$bsids.length - 1];
    }

    // Given a BlockStore, check all its blocks to make sure there isn't 
    // a more appropriate place for them.
    function CheckBlockLocations(accessor, callback) {
      accessor.scan_once(function ScanFunc(err, key, stop) {
        var aid = AccessorIdFromKey(key);
        if (aid != accessor.id) {
          console.log('Moving block', key, 'from', accessor.id, 'to better location', aid);
	  async.waterfall([
	    function GetBlock(callback) { accessor.get(key, callback) },
	    function StoreBlock(data, callback) { $accessor[aid].store(key, data, callback); },
	    function RemoveOriginal(filesize_delta, callback) { accessor.remove(key, callback) },
	  ], function(err) {
	    if (err)
	      console.error("Error moving block", key, "from", accessor.id, "to", aid);
	  });
	}
      }, callback);
    }
          
    // Returns the per-user quota levels for this BlockStore.
    this.quota = function(callback) { callback(null, $quota); }

    // Returns the size of the blockstore (i.e. its capacity).
    this.size = function(callback) { callback(null, $capacity); }

    // Add a remote blockstore to the set and migrates blocks to/from it.
    this.add = function(accessor, callback) {
      async.waterfall([
        function CheckUID(callback) {
          accessor.getuid(callback);
        },
        function CheckUID(auid, callback) {
          if (auid != uid)
            return callback("UID of accessor doesn't match local UID. (" + auid + " != " + uid + ")");
          accessor.quota(callback);
        },
        function GetQuota(quota, callback) {
          console.log("adding quota", quota);
          $quota += quota;
          accessor.size(callback);
        },
        function GetCapacity(capacity, callback) {
          $capacity += capacity;
          $accessor[accessor.id] = accessor;
          $bsids.push(accessor.id);
          $bsids.sort();
          callback();
        },
        function CheckBSAfter(callback) {
          // Checks that blocks in the BS after don't belong here.
          if ($bsids.length > 1) {
            var ix = $bsids.indexOf(accessor.id);
            var next_ix = (ix + $bsids.length - 1) % $bsids.length;
            var next_accessor = $accessor[$bsids[next_ix]];
            CheckBlockLocations(next_accessor, callback);
          } else {
            callback();
          }
        },
        function CheckBSBefore(callback) {
          // Checks that blocks in the BS before don't belong here.
          if ($bsids.length > 1) {
            CheckBlockLocations(accessor, callback);
          } else {
            callback();
          }
        },
      ], callback);
    };

    // Retrieve a block.
    this.get = function(key, callback) {
      if (!key_regexp.test(key))
        return callback("Invalid key: " + key);
      var bsid = AccessorIdFromKey(key);
      $accessor[bsid].get(key, callback);
    }

    // Store a block.
    this.store = function(key, block, callback) {
      // We redo some of the 
      if (typeof block.length === undefined)  // Hack to force data into string form.
        block = '' + block;
      if (block.length > MAX_BLOCK_SIZE) {
        return callback("Attempted to write oversized block (" + block.length + " > " + MAX_BLOCK_SIZE + ")");
      }
      if (($quota + block.length) > $capacity) {
        return callback("Store failed. BlockStore is full.");
      }

      // TODO: If we overwrite, take into account in quota. Perhaps return whether we overwrote or not?
      $quota += block.length; 

      $accessor[AccessorIdFromKey(key)].store(key, block, function(err, filesize_delta) {
          $quota -= block.length;
        if (!err)
          $quota += filesize_delta;
        callback(err);
      });
    }

    // Remove a block.
    this.remove = function(key, callback) {
      var accessor = $accessor[AccessorIdFromKey(key)];
      async.waterfall([
        // TODO: We really want a remote stat op but we don't support that yet.
        function GetBlock(callback) { accessor.get(key, callback) },
        function UpdateQuotaAndRemove(data, callback) {
          $quota -= data.length;
          accessor.remove(key, callback);
        },
      ], callback);
    }
  }
})();

