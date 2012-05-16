(function() {
  var async = require('./libs/async');
  var crypto = require('crypto');
  var express = require('express');
  var fslayer = require('./fslayer');
  var http = require('http');
  var https = require('https');
  var formidable = require('./libs/formidable');
  var url = require('url');
  var util = require('util');

  module.exports = {createServer:createServer}

  // Helper function to make large numbers easier to parse
  function humanize_size(size) {
    if (size < 1024)
      return '' + parseInt(size) + 'B';
    if (parseInt(size / 1024) < 1024)
      return '' + parseInt(size / 1024) + 'K';
    if (parseInt(size / 1024 / 1024) < 1024)
      return '' + parseInt(size / 1024 / 1024) + 'M';
    if (parseInt(size / 1024 / 1024 / 1024) < 1024)
      return '' + parseInt(size / 1024 / 1024 / 1024) + 'G';
    if (parseInt(size / 1024 / 1024 / 1024 / 1024) < 1024)
      return '' + parseInt(size / 1024 / 1024 / 1024 / 1024) + 'T';
    if (parseInt(size / 1024 / 1024 / 1024 / 1024 / 1024) < 1024)
      return '' + parseInt(size / 1024 / 1024 / 1024 / 1024 / 1024) + 'P';
  }

  function WebInterface(blockstores, peers) {
    this.blockstores = blockstores;
    this.peers = peers;
    var self = this;
    this.server = express.createServer();

    // Enable sessions
    this.server.use(express.cookieParser());
    this.server.use(express.session({ secret: "secretsquirrel123" }));

    // Enable static content
    this.server.use(express.static('./static'));
    this.server.set('view engine', 'jqtpl');
    this.server.register(".jqtpl", require("jqtpl").express);

    // Routing
    this.server.get('/', function(req, res) {
      res.render('index.jqtpl', { stats:self.BlockStoreStats() });
    });
    this.server.get('/version', function(req, res) {
      res.writeHead(200, {'Content-Type': 'text/plain'});
      res.end('0.0.1,ghetto');
    });
    this.server.get('/download', function(req, res) { self.HandleDownload(req, res); });
    this.server.post('/upload', function(req, res) { self.HandleUpload(req, res); });
    this.listen = function(port) { this.server.listen(port); }
  }

  WebInterface.prototype.HandleDownload = function(req, res) {
    var filename = req.param('filename');
    console.log("Filename is " + filename);
    var stream = fslayer.getReadStream(filename, this.blockstores);
    if (!stream) {
      res.render('404');
('<html><h1>Download Failed</h1>', {'Content-Type': 'text/html'}, 404);
      return;
    }
    ees.writeHead(200, {'Content-Type': 'application/octet-stream', 
		        'Content-Disposition': 'attachment; filename=' + filename});
    stream.on('data', function(data) {
      res.write(data);
    });
    stream.on('end', function() {
      res.end();
    });
  }

  WebInterface.prototype.HandleUpload = function(req, res) {
    var form = new formidable.IncomingForm();
    var self = this;
    form.onPart = function(part) {
      var writer = fslayer.getWriteStream(part.filename, self.blockstores);
      console.log('got part', part);
      part.addListener('data', function(data) {
	console.log('got data', data);
	writer.write(data, function() {});
      });
    };
    form.parse(req, function(err, fields, files) {
      req.flash('Upload succeeded.');
      res.redirect('/');
    });
  }

  WebInterface.prototype.BlockStoreStats = function() {
    var total_capacity = 0, total_size = 0;
    for (var i = 0; i < this.blockstores.length; i++) {
      total_capacity += this.blockstores[i].capacity;
      total_size += this.blockstores[i].size;
    }
    return (
        '<table>' +
        '<tr><td>Num of BlockStores:</td><td>' + this.blockstores.length + '</td></tr>' +
        '<tr><td>Size / Capacity:</td><td>' + humanize_size(total_size) + ' of ' + humanize_size(total_capacity) + '</td></tr>' +
        '</table>');
  }

  function createServer(blockstores, peers) {
    return new WebInterface(blockstores, peers);
  }

})();
