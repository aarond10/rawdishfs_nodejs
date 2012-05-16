(function() {
  var async = require('./libs/async');
  var blockstore = require('./blockstore');
  var fs = require('fs');
  var fslayer = require('./fslayer');
  var fullmesh = require('./fullmesh');
  var webui = require('./webui/app');

  var mykey = null,
      mycert = null,
      myca = null,
      peer_addrs = [],
      stores = [],
      rpchost = '127.0.0.1',
      rpcport = 8124,
      webport = 8080;

  // Processes command line arguments, exiting on error.
  function ProcessCommandLine() {
    process.argv.forEach(function (val, index, array) {
      if (index < 2) return;
      if (val.indexOf('--mykey=') == 0) {
	mykey = val.substr('--mykey='.length);
      } else if (val.indexOf('--mycert=') == 0) {
	mycert = val.substr('--mycert='.length);
      } else if (val.indexOf('--myca=') == 0) {
	myca = val.substr('--myca='.length);
      } else if (val.indexOf('--peer=') == 0) {
	peer_addrs.push(val.substr('--peer='.length).split(':'));
      } else if (val.indexOf('--store=') == 0) {
	stores.push(val.substr('--store='.length).split(','));
      } else if (val.indexOf('--rpchost=') == 0) {
	rpchost = val.substr('--rpchost='.length);
      } else if (val.indexOf('--rpcport=') == 0) {
	rpcport = parseInt(val.substr('--rpcport='.length));
      } else if (val.indexOf('--webport=') == 0) {
	webport = parseInt(val.substr('--webport='.length));
      } else {
	console.error("Unknown command line option:", val);
	process.exit(1);
      }
    });

    var missing_args = !mykey || !mycert || stores.length == 0;
    if (missing_args) {
      console.error('Usage:', process.argv[0], '--mykey=<mykey.pem> --mycert=<mycert.pem} {--peercert=<peer.pem> ...} {--peer=<host:port> ...} {--store=/path/to/store,<number>}');
      process.exit(1);
    }
    peer_addrs.forEach(function(val, index, array) {
      if (val.length != 2 || parseInt(val[1]) <= 0) {
	console.error("Invalid peer address:", val);
	process.exit(1);
      }
    });

    try {
      mykey = fs.readFileSync(mykey);
    } catch(e) {
      console.error('Could not open key file:', e);
      process.exit(1);
    }
    try {
      mycert = fs.readFileSync(mycert);
    } catch(e) {
      console.error('Could not open cert file:', e);
      process.exit(1);
    }
    try {
      myca = fs.readFileSync(myca);
    } catch(e) {
      console.error('Could not open cacert file:', e);
      process.exit(1);
    }
  }

  function InitLocalBlockStores(stores, callback) {
    var blockstores = {};
    async.forEachSeries(stores, function initStores(store, callback) {
      if (store.length != 2 || parseInt(store[1]) < 1) {
	console.error("Invalid data store specification:", store);
	process.exit(1);
      }
      var paths = [];
      for (var i = 0; i < store[1]; ++i)
	paths.push(store[0] + '/' + i.toString(16));
      async.forEachSeries(paths, function initSingleStore(path, callback) {
	var bs = new blockstore.BlockStore(path, function(err) {
          if (!err)
            blockstores[bs.id()] = bs;
          callback(err);
        });
      }, function(err) {
	if (err) {
	  console.error("Failed to initialize block store:", err);
	  process.exit(1);
	}
        callback(err);
      });
    }, callback);
    return blockstores;
  }

  function InitMesh(blockstores) {
    var params = {
      host: rpchost,
      port: rpcport,
      key: mykey,
      cert: mycert,
      ca: [ myca ],
      requestCert: true,
      //rejectUnauthorized: true,
    };
    var mesh = new fullmesh.MeshNode(params, blockstores);
    return mesh;
  }

  //////////////////////////////////////////////////

  var blockstores = [];
  var mesh = null;
  var ui = null;

  ProcessCommandLine();

  async.waterfall([
    function(callback) {
      console.log('Initializing blockstores');
      blockstores = InitLocalBlockStores(stores, callback);
    },
    function(callback) {
      console.log('Initializing Mesh');
      mesh = InitMesh(blockstores);

      // Connect to peers provided on the command line.
      for (var i = 0; i < peer_addrs.length; i++) {
        mesh.connect(peer_addrs[i][0], parseInt(peer_addrs[i][1]));
      }

      // Start up local web server for admin, status and peers
      console.log('Initializing Web UI');
      ui = new webui(webport, mesh, blockstores);
      callback();
    }
  ], function(err) {
    if (err) {
      console.error("Initialization failed:", err);
      process.exit(1);
    }
  });
})();
