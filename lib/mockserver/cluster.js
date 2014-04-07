"use strict";

var util = require('util');
var utils = require('./utils');
var certs = require('./certs');
var errors = require('./errors');

var EventEmitter = require('events').EventEmitter;
var MockNode = require('./node');
var MockBucket = require('./bucket');
var ConfigGenerator = require('./configgen');

function Cluster() {
  this.numVbuckets = 32;
  this.buckets = {};
  this.nodes = [];

  this.sslCreds = {
    key: certs.KEY,
    cert: certs.CERT
  };
}
util.inherits(Cluster, EventEmitter);

Cluster.prototype.prepare = function(options, callback) {
  if (options instanceof Function) {
    callback = options;
    options = {};
  }

  if (this.nodes.length !== 0) {
    throw new Error('tried to setup a cluster that already has nodes');
  }
  for (var bucketName in this.buckets) {
    if (this.buckets.hasOwnProperty(bucketName)) {
      throw new Error('tried to setup a cluster that already has buckets');
    }
  }

  if (options.numNodes === undefined) {
    options.numNodes = 3;
  }
  if (options.buckets === undefined) {
    options.buckets = {
      'default': {
        numReplicas: 1
      }
    };
  }

  var self = this;
  this.addNodes(options.numNodes, function() {
    for (var i in options.buckets) {
      if (options.buckets.hasOwnProperty(i)) {
        self.createBucket(i, options.buckets[i]);
      }
    }
    callback();
  });
};

Cluster.prototype.destroy = function(callback) {
  var maybeCallback = utils.waitForAllCallback(callback);
  var maybeFinish = maybeCallback();
  for (var i = 0; i < this.nodes.length; ++i) {
    var node = this.nodes[i];
    node.destroy(maybeCallback());
  }
  maybeFinish();
};

Cluster.prototype._configChanged = function() {
  this.emit('configChanged');
};

Cluster.prototype._generateBucketConfig = function(bucket) {
  return ConfigGenerator.generateConfig(this, bucket);
};

Cluster.prototype.bootstrapList = function(mode) {
  var hosts = [];
  for (var i = 0; i < this.nodes.length; ++i) {
    var node = this.nodes[i];
    if (mode === 'http') {
      hosts.push(node.host + ':' + node.mgmtSvc.port);
    } else if (mode === 'https') {
      hosts.push(node.host + ':' + node.mgmtSvc.sslPort);
    } else if (mode === 'cccp') {
      hosts.push(node.host + ':' + node.memdSvc.port);
    } else if (mode === 'cccps') {
      hosts.push(node.host + ':' + node.memdSvc.sslPort);
    } else if (mode === 'n1ql') {
      hosts.push(node.host + ':' + node.n1qlSvc.port);
    }
  }
  return hosts;
};

Cluster.prototype._createVbucketMap = function(lastIds, numIdxs) {
  var nodeIds = [];
  for (var nId = 0; nId < this.nodes.length; ++nId) {
    var nodeId = this.nodes[nId].nodeId;
    if (lastIds.indexOf(nodeId) !== -1) {
      continue;
    }
    nodeIds.push(nodeId);
  }
  /*
  for (var i = 0; i < lastIds.length; ++i) {
    nodeIds.push(lastIds[i]);
  }
  */

  var newMap = [];
  for (var repId = 0; repId < numIdxs; ++repId) {
    if (nodeIds.length > 0) {
      var nodeIdsIdx = Math.floor(Math.random() * nodeIds.length);
      newMap.push(nodeIds[nodeIdsIdx]);
      nodeIds.splice(nodeIdsIdx, 1);
    } else {
      newMap.push(-1);
    }
  }
  return newMap;
};

Cluster.prototype.createBucket = function(name, options) {
  if (this.buckets[name]) {
    throw new Error('tried to create duplicate bucket');
  }

  // Create the bucket
  var bucket = new MockBucket(this, name, options);

  // Assign random servers to its vbucket map
  for (var vbId = 0; vbId < bucket.vbMap.length; ++vbId) {
    bucket.vbMap[vbId] = this._createVbucketMap([], 1+bucket.numReplicas);
  }

  // Store it
  this.buckets[name] = bucket;
  this._configChanged();
  return bucket;
};

Cluster.prototype.bucketByName = function(bucketName) {
  var bucket = this.buckets[bucketName];
  if (!bucket) {
    return null;
  }
  return bucket;
};

Cluster.prototype.nodeById = function(nodeId) {
  if (nodeId === -1) {
    return null;
  }
  for (var nIdx = 0; nIdx < this.nodes.length; ++nIdx) {
    var node = this.nodes[nIdx];
    if (node.nodeId === nodeId) {
      return node;
    }
  }
  return null;
};

Cluster.prototype.addNode = function(callback) {
  var self = this;
  var newNode = new MockNode(self, function() {
    self.nodes.push(newNode);
    self._configChanged();
    callback();
  });
};

Cluster.prototype.addNodes = function(num, callback) {
  var maybeCallback = utils.waitForAllCallback(callback);
  for (var i = 0; i < num; ++i) {
    this.addNode(maybeCallback());
  }
  maybeCallback()();
};

Cluster.prototype.removeNode = function(nodeId) {
  for (var bucketName in this.buckets) {
    if (this.buckets.hasOwnProperty(bucketName)) {
      var bucket = this.buckets[bucketName];
      if (bucket.nodeHasVbs(nodeId)) {
        throw new Error('tried to remove a node while it was still hosting vbuckets');
      }
    }
  }

  var node = this.nodeById(nodeId);
  var nodeIdx = this.nodes.indexOf(node);
  if (nodeIdx !== -1) {
    this.nodes.splice(nodeIdx, 1);
  }

  this._configChanged();
};

Cluster.prototype._executeQuery = function(q, callback) {
  /*callback(
    new error.N1qlError(
      5000,
      'Internal Error',
      'Optimizer Error',
      'Bucket `?` does not exist'
    ), null);*/
  callback(null, []);
};

if (0 /*MOCK DEVELOPMENT*/) {
  var svcPortCounter = 50000;
  Cluster.prototype.pickSvcPort = function() {
    return svcPortCounter++;
  };
} else {
  Cluster.prototype.pickSvcPort = function() {
    return 0;
  };
}

module.exports = Cluster;
