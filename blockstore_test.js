var async = require('async');
var blockstore = require('./blockstore');

var TESTDATA_PATH = './testdata/blockstore';

exports.testQuota = function(test) {
  var bs = new blockstore.BlockStore(TESTDATA_PATH, 
  function Loaded(err) {
    bs.quota(function GotQuota(err, quota) {
      test.equal(null, err);
      test.equal(11, quota['usera']);
      test.equal(21, quota['userb']);
      test.equal(31, quota['userc']);
      test.done();
    });
  });
};

exports.testSize = function(test) {
  var bs = new blockstore.BlockStore(TESTDATA_PATH, 
  function Loaded(err) {
    bs.size(function GotSize(err, size) {
      test.equal(null, err);
      test.equal(128 << 20, size);  // hard coded for now.
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
  var count = 4;

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
        accessor.getuid(callback);
      },
      function(uid, callback) {
        test.equal('usera', uid);
        accessor.size(callback);
      },
      function(size, callback) {
        test.equal(128 << 20, size);  // hard coded size for now.
        accessor.get('0123', callback);
      },
      function(data, callback) {
        olddata = data;
        accessor.store('0123', newdata, callback);
      },
      function(filesize_delta, callback) {
        test.equal(newdata.length - 11, filesize_delta);
        accessor.quota(callback);
      },
      function(quota, callback) {
        test.equal(newdata.length, quota);
        accessor.store('0123', olddata, callback);
      },
      function(filesize_delta, callback) {
        test.equal(11 - newdata.length, filesize_delta);
        accessor.quota(callback);
      }
    ], function(err, quota) {
      test.ok(!err);
      test.equal(olddata.length, quota);
      test.done();
    });
  });
};

exports.testStoreDelete = function(test) {
  var bs = new blockstore.BlockStore(TESTDATA_PATH, 
  function Loaded(err) {
    var newdata = 'This is some new data';
    var accessor = new bs.accessor('usera');
    async.waterfall([
      function(callback) {
        accessor.store('abcd', newdata, callback);
      },   
      function(filesize_delta, callback) {
        test.equal(newdata.length, filesize_delta);
        accessor.get('abcd', callback);
      },
      function(data, callback) {
        test.deepEqual(data.toString(), newdata);
        accessor.remove('abcd', callback);
      },
      function(callback) {
        accessor.get('abcd', function(err) {
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

exports.testAccessorScanOnce = function(test) {
  var expected = { key: '0123', };
  var count = 1;

  var bs = new blockstore.BlockStore(TESTDATA_PATH, 
  function Loaded(err) {
    var accessor = new bs.accessor('usera');
    accessor.scan_once(function OnData(err, key, stop) {
      test.equal(null, err);
      test.equal(expected.key, key);
      count -= 1;
    }, function(err) {
      test.equal(null, err);
      test.equal(0, count);
      test.done();
    });
  });
};

exports.testAccessorScanner = function(test) {
  var expected = { key: '0123', };
  var count = 2;

  var bs = new blockstore.BlockStore(TESTDATA_PATH, 
  function Loaded(err) {
    var accessor = new bs.accessor('usera');
    accessor.scanner(function OnData(err, key, stop) {
      test.equal(null, err);
      test.equal(expected.key, key);
      count -= 1;
      if (count <= 0) {
        test.equal(0, count);
        stop();
        test.done();
      }
    }, function(err) {
      test.ok(false, "shouldn't get here.");
    });
  });
};
