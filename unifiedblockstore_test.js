var async = require('async');
var crypto = require('crypto');
var blockstore = require('./blockstore');
var unifiedblockstore = require('./unifiedblockstore');

var TESTDATA_PATH = './testdata/unifiedblockstore/';

// Helper function to set up a new instance of UnifiedBlockStore
function GetUnifiedBlockStore(uid, callback) {
  var ret = new unifiedblockstore.UnifiedBlockStore(uid);
  async.forEach(['a', 'b', 'c'], function(val, callback) {
    var bs = null;
    async.waterfall([
      function(callback) {
        bs = new blockstore.BlockStore(TESTDATA_PATH + val, callback);
      },
      function(callback) {
        var accessor = new bs.accessor(uid);
        ret.add(new bs.accessor(uid), callback);
      },
    ], callback);
  }, callback);
  return ret;
}

exports.testQuota = function(test) {
  var ubs = GetUnifiedBlockStore('usera',
  function Loaded(err) {
    ubs.quota(function GotQuota(err, quota) {
      test.equal(null, err);
      test.equal(63, quota);
      test.done();
    });
  });
};

exports.testSize = function(test) {
  var ubs = GetUnifiedBlockStore('usera',
  function Loaded(err) {
    ubs.size(function GotSize(err, size) {
      test.equal(null, err);
      test.equal((128 << 20) * 3, size);  // hard coded for now.
      test.done();
    });
  });
};

exports.testGetStoreQuotaAndOverwrite = function(test) {
  var ubs = GetUnifiedBlockStore('usera',
  function Loaded(err) {
    var newdata = 'This is some new data';
    var olddata = null;
    async.waterfall([
      function(callback) {
        ubs.size(callback);
      },
      function(size, callback) {
        test.equal((128 << 20) * 3, size);  // hard coded size for now.
        ubs.get('0123', callback);
      },
      function(data, callback) {
        olddata = data;
        ubs.store('0123', newdata, callback);
      },
      function(callback) {
        ubs.quota(callback);
      },
      function(quota, callback) {
        test.equal(63 - olddata.length + newdata.length, quota);
        ubs.store('0123', olddata, callback);
      },
      function(callback) {
        ubs.quota(callback);
      }
    ], function(err, quota) {
      test.ok(!err);
      test.equal(63, quota);
      test.done();
    });
  });
};

exports.testStoreDelete = function(test) {
  var ubs = GetUnifiedBlockStore('usera',
  function Loaded(err) {
    var key = crypto.randomBytes(16).toString('hex');
    var newdata = 'This is some new data';
    async.waterfall([
      function(callback) {
        ubs.store(key, newdata, callback);
      },   
      function(callback) {
        ubs.get(key, callback);
      },
      function(data, callback) {
        test.deepEqual(data.toString(), newdata);
        ubs.remove(key, callback);
      },
      function(callback) {
        ubs.get(key, function(err) {
          test.ok(err);  // Expect File Not Found error.
          callback();
        });
      },
    ], function(err, quota) {
      test.ok(!err);
      test.done();
    });
  });
};

