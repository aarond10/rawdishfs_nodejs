var async = require('async');
var crypto = require('crypto');
var blockstore = require('./blockstore');
var filesystem = require('./filesystem');
var unifiedblockstore = require('./unifiedblockstore');

var TESTDATA_PATH = './testdata/unifiedblockstore/';
var TESTFILE_SIZE = 10 << 20;  // 10MB

// Helper function to set up a new instance of UnifiedBlockStore
function GetCodedBlockStore(uid, callback) {
  var ubs = new unifiedblockstore.UnifiedBlockStore(uid);
  async.forEach(['a', 'b', 'c'], function(val, callback) {
    var bs = null;
    async.waterfall([
      function(callback) {
        bs = new blockstore.BlockStore(TESTDATA_PATH + val, callback);
      },
      function(callback) {
        var accessor = new bs.accessor(uid);
        ubs.add(new bs.accessor(uid), callback);
      },
    ], callback);
  }, callback);
  return new filesystem.CodedBlockStore(ubs, 1);
}

// Helper function to write a file and trigger callback when done.
function WriteFile(cbs, filename, callback) {
  var file = new cbs.File(filename);
  var writer = new file.GetWriteStream();
  var i = 0;
  function ContinueWriteUntilSaturated() {
    while (i < (TESTFILE_SIZE / 16)) {
      i++;
      if (writer.write("0123456789abcdef") == false)
        break;
    }
    if (i >= (TESTFILE_SIZE / 16))
      writer.end();
  }
  writer.on('error', function(err) { callback(err); });
  writer.on('drain', ContinueWriteUntilSaturated);
  writer.on('end', function() { callback(null); });
  ContinueWriteUntilSaturated();
}

// Reads a file that is expected to contain the same data as written by
// WriteFile and calls callback when done.
function ReadFile(cbs, filename, callback) {
  var file = new cbs.File(filename);
  var reader = new file.GetReadStream();
  var expected = new Buffer("0123456789abcdef");
  var length = 0;
  reader.on('data', function(data) {
    for (var i = 0; i < data.length; i++) {
      if (expected[length % expected.length] != data[i])
        console.error("Unexpected file data.");
      length++;
    }
  });
  reader.on('end', function() {
    if (TESTFILE_SIZE != length)
      console.error("Size mismatch.");
    callback(null);
  });
  reader.on('error', function(err) {
    console.error('Reader error:', err);
    callback(err);
  });
}

exports.testStoreAndGet = function(test) {
  var filename = 'some_arbitrary_name.txt';
  var cbs = null;
  async.waterfall([
    function(callback) { cbs = GetCodedBlockStore('usera', callback); },
    function(callback) { WriteFile(cbs, filename, callback); },
    function(callback) { ReadFile(cbs, filename, callback); },
    function(callback) { var file = new cbs.File(filename); file.Delete(callback); },
  ], function(err) {
    test.ok(!err);
    test.done();
  });
};

/*
exports.testStoreDelete = function(test) {
  var cbs = GetCodedBlockStore('usera',
  function Loaded(err) {
    var newdata = '';
    var writer = new cbs.GetWriteStream('some_arbitrary_name.txt');
    writer.on('error', function(err) {
      console.error('Writer error:', err);
      test.ok(false);
    });
    for (var i = 0; i < 16384; i++)
      writer.write('This is some repeated data. ');

    writer.on('end', function() {
      var reader = new cbs.GetReadStream('some_arbitrary_name.txt');
      reader.on('data', function(data) {
        console.log('Data:', data);
      });
      reader.on('end', function() {
        test.done();
      });
      reader.on('error', function(err) {
        console.error('Reader error:', err);
        test.ok(false);
      });
    });
    writer.end();
  });
};
*/
