(function() {
  var async = require('async');
  var crypto = require('crypto');
  var stream = require('stream');
  var util = require('util');

  module.exports = {CodedBlockStore:CodedBlockStore};//, FileSystem:FileSystem};

  var BLOCK_SIZE = 16384; // 16KB per block.
  var PASSWORD = "1234"; // TODO: Replace with key passed into CodedBlocKStore.

  // Provides a streamable, authenticated, but *unencrypted* storage API for
  // arbitrary length blobs of data built upon a unifiedblockstore interface. 
  // Seeking is not currently supported.
  //
  // Blocks are signed with a sha256 HMAC based on the blocks name, key and 
  // contents. Block keys are derived from the block name. Block length is
  // stored in the first 4 bytes (31-bit max blob length) of the first block.
  // 
  // Each blob is written N times sequentially according to num_replicas with
  // the hope (but not assurance) that blocks land on different physical
  // devices to add redundancy. 
  //
  // Subsequent blocks are keyed using recursive hashing.
  function CodedBlockStore(blockstore, num_replicas) {
    // Represents a file on a coded block store. Note that there is no actual 
    // directory structure. File names are just seeds for a hashing function.
    this.File = function(filename) {

      // Class for fetching an ordered series of block keys for blocks 
      // making up a file.
      function KeyFactory() {
        var hash = crypto.createHash('sha256');
        hash.update(filename);
        this.next = function() {
          var key = hash.digest('hex');
          hash = crypto.createHash('sha256');
          hash.update(filename + key);
          return key;
        }
      }

      // Calculates the HMAC for a given block of a file.
      function GetHMAC(key, block) {
        var hmac = crypto.createHmac('sha256', PASSWORD);
        hmac.update(filename);
        hmac.update(key);
        hmac.update(block.slice(0, block.length - 32));
        return hmac;
      }

      // Fetches at least one valid block from the next num_replicas blocks
      function FetchNextLogicalBlock(keyfactory, callback) {
        var data = null;
        var i = 0;

	// Given a key, fetches and authenticates a block before returning it.
	function FetchPhysicalBlock(key, callback) {
	  blockstore.get(key, function HMACCheckerCallback(err, data) {
	    if (err)
	      return callback(err);
	    var calculated_hmac_value = GetHMAC(key, data).digest('hex');
	    var block_hmac_value = data.slice(data.length - 32, data.length);
	    if (calculated_hmac_value != block_hmac_value.toString('hex')) {
	      return callback("Block HMAC mismatch. (" +
			      calculated_hmac_value + " != " +
			      block_hmac_value.toString('hex') + ")");
	    }
	    callback(null, data.slice(0, data.length - 32));
	  });
	}

        function ReplicaAttempt(err, data) {
          i++;
          if (err) {
            if (i < num_replicas)
              FetchPhysicalBlock(keyfactory.next(), ReplicaAttempt);
            else
              callback("Error reading block: " + err);
          } else {
            // Skip remaining replicas.
            while (i < num_replicas) {
              keyfactory.next();
              i++;
            }
            callback(null, data);
          }
        }
        FetchPhysicalBlock(keyfactory.next(), ReplicaAttempt);
      }

      
      this.GetReadStream = function() {
	var self = this;
	var keyfactory = new KeyFactory();
	this.$length = -1;
	this.$pos = 0;

	function ProcessFirstBlock(err, data) {
	  if (err)
	    return self.emit('error', err);
	  self.$length = data.readInt32LE(0);
	  self.$pos += data.length - 4;
	  console.log('Read first block. Length is', self.$length, 'and pos', self.$pos);
	  self.emit('data', data.slice(4));
	  if (self.$pos < self.$length)
	    FetchNextLogicalBlock(keyfactory, ProcessSubsequentBlock);
	  else
	    self.emit('end');
	}

	function ProcessSubsequentBlock(err, data) {
	  if (err)
	    return self.emit('error', err);
	  self.$pos += data.length;
	  self.emit('data', data);
	  if (self.$pos < self.$length)
	    FetchNextLogicalBlock(keyfactory, ProcessSubsequentBlock);
	  else
	    self.emit('end');
	}
	setTimeout(function() { FetchNextLogicalBlock(keyfactory, ProcessFirstBlock); }, 0);
      }
      require('util').inherits(this.GetReadStream, stream.Stream);

      // Because stream length is not known until the end, we defer writing of 
      // the first block of data until end() is called. We also buffer internally
      // to fill blocks and emit('end') when we get confirmation back that the
      // file has been completely written to disk.
      this.GetWriteStream = function() {
	var self = this;
	var keyfactory = new KeyFactory();
	this.$buffer = new Buffer(0);
	this.$first_block = null;
	this.$length = 0;

	function AppendToBuffer(data) {
	  var oldbuf = self.$buffer;
	  self.$buffer = new Buffer(self.$buffer.length + data.length);
	  oldbuf.copy(self.$buffer, 0, 0, oldbuf.length);
	  data.copy(self.$buffer, oldbuf.length, 0, data.length);
	  self.$length += data.length;
	}

	this.write = function(data) {
	  AppendToBuffer(new Buffer(data));

	  // Special case for first block that is written last with total length prefixed.
	  if (self.$first_block == null &&
	      self.$buffer.length >= (BLOCK_SIZE - 4 - 32)) {
	    var datalen = Math.min(BLOCK_SIZE - 32 - 4, self.$buffer.length);
	    self.$first_keys = [];
	    for (var i = 0; i < num_replicas; ++i)
	      self.$first_keys.push(keyfactory.next());
	    console.log("Write first block of", datalen, "bytes.", num_replicas, "copies");
	    self.$first_block = new Buffer(datalen + 4 + 32);
	    self.$buffer.copy(self.$first_block, 4, 0, datalen);
	    self.$buffer = self.$buffer.slice(datalen);  // Note: doesn't free the start of the buffer - just moves pointer.
	  }

	  var ret = true;

	  while (self.$buffer.length >= (BLOCK_SIZE - 32)) {
	    var datalen = Math.min(BLOCK_SIZE - 32, this.$buffer.length);
	    var key = keyfactory.next();
	    var block = new Buffer(datalen + 32);
	    self.$buffer.copy(block, 0, 0, datalen);
	    self.$buffer = self.$buffer.slice(datalen);  // Note: doesn't free the start of the buffer - just moves pointer.
	    block.write(GetHMAC(key, block).digest(), block.length - 32, block.length, 'binary');
	    blockstore.store(key, block, function StoreFinished(err) {
	      if (err)
		self.emit('error', err);
	      else
		self.emit('drain');
	    });
	    ret = false;
	  }
	  return ret;
	}

	this.end = function(data) {
	  var self = this;

	  // end() takes an optional last block of data.
	  if (data !== undefined)
	    self.write(data);

	  // We only write the first block out after writing out any remaining 
	  // data first.
	  function WriteFirstBlock() {
	    // Rewrite the first block with the block length prefixed.
	    self.$first_block.writeInt32LE(self.$length, 0);
	    async.forEach(self.$first_keys, function(key, callback) {
	      self.$first_block.write(GetHMAC(key, self.$first_block).digest(), 
				      self.$first_block.length - 32, 
				      self.$first_block.length, 'binary');
	      blockstore.store(key, self.$first_block, callback);
	    }, function(err) {
	      if (err)
		return self.emit('error', err);
	      else
		return self.emit('end');
	    });
	  }

	  // Flush last block if we have data.
	  if (self.$buffer.length > 0) {
	    var datalen = this.$buffer.length;
	    var key = keyfactory.next();
	    var block = new Buffer(datalen + 32);
	    self.$buffer.copy(block, 0, 0, datalen);
	    block.write(GetHMAC(key, block).digest(), block.length - 32, block.length, 'binary');
	    blockstore.store(key, block, function StoreFinished(err) {
	      if (err)
		self.emit('error', err);
	      else
		WriteFirstBlock();
	    });
	  } else {
	    WriteFirstBlock();
	  }
	  self.writable = false;
	}
	
      }
      require('util').inherits(this.GetWriteStream, stream.Stream);

      // Deletes all blocks associated with a file on disk.
      this.Delete = function(callback) {
	var self = this;
	var keyfactory = new KeyFactory();
	this.$length = -1;
	this.$pos = 0;

	function ProcessFirstBlock(err, data) {
	  if (err)
	    return callback(err);
	  var length = data.readInt32LE(0);
          var keys = [];
	  var keyfactory = new KeyFactory();
          for (var i = 0; i < (num_replicas * Math.ceil((length+4)/(BLOCK_SIZE-32))); i++)
            keys.push(keyfactory.next());
          async.forEachLimit(keys, 16, function(key, callback) {
            blockstore.remove(key, callback);
          }, callback);
	}

	setTimeout(function() { FetchNextLogicalBlock(keyfactory, ProcessFirstBlock); }, 0);
      }
    }
  }
})();


