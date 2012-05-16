(function() {
  // Module dependencies.
  var express = require('express')
    , routes = require('./routes');

  var http = require('http');
  var jsDAV = require('jsDAV');

  module.exports = createServer;

  function createServer(port, mesh, blockstores) {
    var app = express.createServer();

    // Configuration
    app.configure(function(){
      app.set('views', __dirname + '/views');
      app.set('view engine', 'jade');
      app.use(express.bodyParser());
      app.use(express.methodOverride());
      app.use(require('stylus').middleware({ src: __dirname + '/public' }));
      app.use(app.router);
      app.use(express.static(__dirname + '/public'));
    });

    app.configure('development', function(){
      app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
    });

    app.configure('production', function(){
      app.use(express.errorHandler());
    });

    // TODO: Move this somewhere else.
    function GetNetworkStats() {
      // Helper function to make large numbers easier to parse
      function humanize_size(size) {
	if (size < 1024)
	  return '' + parseInt(size) + 'B';
	if (parseInt(size / 1024) < 1024)
	  return '' + parseInt(size / 1024) + 'KB';
	if (parseInt(size / 1024 / 1024) < 1024)
	  return '' + parseInt(size / 1024 / 1024) + 'MB';
	if (parseInt(size / 1024 / 1024 / 1024) < 1024)
	  return '' + parseInt(size / 1024 / 1024 / 1024) + 'GB';
	if (parseInt(size / 1024 / 1024 / 1024 / 1024) < 1024)
	  return '' + parseInt(size / 1024 / 1024 / 1024 / 1024) + 'TB';
	if (parseInt(size / 1024 / 1024 / 1024 / 1024 / 1024) < 1024)
	  return '' + parseInt(size / 1024 / 1024 / 1024 / 1024 / 1024) + 'PB';
      }
      
      var peers = mesh._peers;
      var blockstores = mesh.blockstores();

      var num_blockstores = 0;
      var num_peers = 0;
      for (var i in blockstores)
        num_blockstores += 1;
      for (var i in peers)
        num_peers += 1;

      var approx_size = 0;
      var approx_capacity = 0;
      for (var i in blockstores) {
        approx_size += blockstores[i]._size;
        approx_capacity += blockstores[i]._capacity;
      }

      return { 
        num_peers: num_peers,
        num_blockstores: num_blockstores,
        blockstores: blockstores,
        approx_size: approx_size,
        approx_capacity: approx_capacity,
        approx_size_human: humanize_size(approx_size),
        approx_capacity_human: humanize_size(approx_capacity),
      };
    };

    // Routes
    app.get('/', function(req, res) {
      res.render('index', { 
        title: 'RawDish Console', 
        stats: GetNetworkStats(),
      });
    });
    app.get('/status', function(req, res) {
      var stats = GetNetworkStats();

      res.render('status', { 
        title: 'System Status', 
        random_block: mesh.SelectBlockStore(),
        stats: GetNetworkStats(),
        peers: mesh._peers });
        
    });

    jsDAV.mount({
      node:'/tmp',
      mount:'data',
      server: app,
      //standaline: standalone
    });
    app.listen(port, function onListen() {
      console.log("Express server listening on port %d in %s mode", app.address().port, app.settings.env);
    });

    return app;
  }
})();
