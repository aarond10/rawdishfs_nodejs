(function() {
  var async = require('./libs/async');
  var crypto = require('crypto');
  var events = require('events');
  var util = require('util');

  module.exports = {getReadStream:getReadStream, getWriteStream:getWriteStream};

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
