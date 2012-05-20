var async = require('async');
var blockstore = require('./blockstore');

var TESTDATA_PATH = './testdata/blockstore';

exports.testQuota = function(test) {
  var bs = new blockstore.BlockStore(TESTDATA_PATH, 
  function Loaded(err) {
    bs.quota(function GotQuota(err, quota) {
      test.equal(11, quota['usera']);
      test.equal(21, quota['userb']);
      test.equal(31, quota['userc']);
      test.done();
    });
  });
};

exports.testScanner = function(test) {
  var expected = {
    usera : { key: '0123', size: 11 },
    userb : { key: '0123', size: 21 },
    userc : { key: '0123', size: 31 },
  };
  var count = 3;

  var bs = new blockstore.BlockStore(TESTDATA_PATH, 
  function Loaded(err) {
    var scanner = new bs.scanner(function OnData(err, key, data, uid) {
      test.equal(null, err);
      test.ok(expected[uid] !== undefined);
      test.equal(expected[uid].key, key);
      test.equal(expected[uid].size, data.length);
      count -= 1;
      if (count == 0) {
        scanner.stop();
        scanner = null;
        test.done();
      }
    });
  });
};

exports.testGetStoreQuotaAndOverwrite = function(test) {
  var bs = new blockstore.BlockStore(TESTDATA_PATH, 
  function Loaded(err) {
    var newdata = 'This is some new data';
    var olddata = null;
    var accessor = new bs.accessor('usera');
    async.waterfall([
      function(callback) {
        accessor.get('0123', callback);
      },
      function(data, callback) {
        olddata = data;
        accessor.store('0123', newdata, callback);
      },
      function(callback) {
        accessor.quota(callback);
      },
      function(quota, callback) {
        test.equal(newdata.length, quota['usera']);
        accessor.store('0123', olddata, callback);
      },
      function(callback) {
        accessor.quota(callback);
      }
    ], function(err, quota) {
      test.equal(olddata.length, quota['usera']);
      test.ok(!err);
      test.done();
    });
  });
};
