"use strict";

var jsDAV_iNode       = require("jsDAV/lib/iNode").jsDAV_iNode;
var jsDAV_Directory   = require("jsDAV/lib/directory").jsDAV_Directory;
var jsDAV_iCollection = require("jsDAV/lib/iCollection").jsDAV_iCollection;
var jsDAV_iQuota      = require("jsDAV/lib/iQuota").jsDAV_iQuota;
var jsDAV_iFile       = require("jsDAV/lib/iFile").jsDAV_iFile;
var jsDAV_Tree        = require("jsDAV/lib/tree").jsDAV_Tree;
var util              = require("jsDAV/lib/util");

function jsDAV_DataStore_Node(name) {
    this.name = name;
}

exports.jsDAV_DataStore_Node = jsDAV_DataStore_Node;

(function() {
  this.getName = function() { return this.name; };
  this.setName = function(name, callback) {
    callback("Not implemented.");
  };
  this.getLastModified = function(callback) {
    callback(null, 0); // Not implemented.
  };
  this.exists = function(callback) {
    callback("Not implemented.");
  };
}).call(jsDAV_DataStore_Node.prototype = new jsDAV_iNode());

function jsDAV_DataStore_Directory(datastore, key, name) {
  this.datastore = datastore;
  this.key = key;
  this.name = name;
  this.dir = null;  // cache of directory data.
}

exports.jsDAV_DataStore_Directory = jsDAV_DataStore_Directory;

(function() {
  this.implement(jsDAV_Directory, jsDAV_iCollection, jsDAV_iQuota);

  this.createFile = function(name, data, enc, callback) {
    callback("Not implemented.");
  };
  this.createFileStream = function(handler, name, enc, callback) {
    callback("Not implemented.");
  };
  this.createDirectory = function(name, callback) {
    callback("Not implemented.");
  };
  this.getChild = function(name, callback) {
    function ReturnChild(dir) {
      if (dir.files[name])
        return callback(null, new jsDAV_DataStore_File(datastore, dir.files[name], name);
      if (dir.subdirs[name])
        return callback(null, new jsDAV_DataStore_Directory(datastore, dir.subdirs[name], name);
      callback("File not found.");
    }
    if (this.dir) {
      ReturnChild(this.dir);
    } else {
      var self = this;
      this.datastore.get(this.key, function onDir(err, data) {
        if (err) {
          return callback(err);
        } else {
          self.dir = JSON.parse(data.toString());
          ReturnChild(self.dir);
        }
      });
    }
  };
  this.getChildren = function(callback) {
    function MakeChildren(dir) {
      if (dir) {
        var ret = [];
        for (var i in dir.files)
          ret.push(new jsDAV_DataStore_File(datastore, dir.files[i], i);
        for (var i in dir.subdirs)
          ret.push(new jsDAV_DataStore_Directory(datastore, dir.subdirs[i], i);
        return ret;
    }
    if (this.dir)
      callback(MakeChildren(this.dir));
    } else {
      var self = this;
      this.datastore.get(this.key, function onDir(err, data) {
        if (err) {
          return callback(err);
        } else {
          self.dir = JSON.parse(data.toString());
          return callback(MakeChildren(self.dir));
        }
      });
    }
  };
  this["delete"] = function(callback) {
    callback("Not implemented.");
  };
  this.getQuotaInfo = function(callback) {
    callback("Not implemented.");
  };
}).call(jsDAV_DataStore_Directory.prototype = new jsDAV_DataStore_Node());

function jsDAV_DataStore_File(datastore, key, name) {
    this.datastore = datastore;
    this.key = key;
    this.name = name;
}

exports.jsDAV_DataStore_File = jsDAV_DataStore_File;

(function() {
  this.implement(jsDAV_iFile);

  this.put = function(data, type, callback) {
    callback("Not implemented.");
  };
  this.putStream = function(handler, type, callback) {
    callback("Not implemented.");
  };
  this.get = function(callback) {
    callback("Not implemented.");
  };
  this.getStream = function(start, end, callback) {
    callback("Not implemented.");
  };
  this["delete"] = function(callback) {
    callback("Not implemented.");
  };
  this.getSize = function(callback) {
    callback(null, 0); // not implemented.
  };
  this.getETag = function(callback) {
    callback(null, this.key); // Note: falsely assumes data is immutable.
  };
  this.getContentType = function(callback) {
    callback(null, "application/octet-stream"); // not implemented.
  };
}).call(jsDAV_DataStore_File.prototype = new jsDAV_DataStore_Node());

function jsDAV_DataStore_Tree(datastore, key) {
  this.datastore = datastore;
  this.key = key;
}

exports.jsDAV_DataStore_File = jsDAV_DataStore_File;
(function() {
  this.getNodeForPath = function(path, cbfstree) {
    var node = new jsDAV_DataStore_Directory(datastore, key, 'root');
    var splitpath = util.splitPath(path);

    async.forEachSerial(splitpath, function(name, callback) {
      node = node.getChild(name, callback);
    }, function(err) {
      if (err)
        return cbfstree(new Exc.jsDAV_Exception_FileNotFound("File at location " + path + " not found"));
        cbfstree(err);
      else
        return cbfstree(null, node);
    }
  };
}).call(jsDAV_DataStore_Tree.prototype = new jsDAV_Tree());

