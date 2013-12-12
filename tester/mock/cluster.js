"use strict";

var fs = require('fs');
var util = require('util');
var utils = require('./utils');

var EventEmitter = require('events').EventEmitter;
var MockNode = require('./node');
var MockBucket = require('./bucket');

var MOCK_SSL_KEY = fs.readFileSync(__dirname + '/local-ssl-key.pem');
var MOCK_SSL_CERT = fs.readFileSync(__dirname + '/local-ssl-cert.pem');

function Cluster() {
  this.numVbuckets = 32;
  this.buckets = {};
  this.nodes = [];
  this.nodeLookup = {};

  this.sslCreds = {
    key: MOCK_SSL_KEY,
    cert: MOCK_SSL_CERT
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

Cluster.prototype.destroy = function() {

};

Cluster.prototype._addConfigListener = function(bucket, res) {
  bucket.configStreams.push(res);
  var removeListener = function() {
    var listenerIdx = bucket.configStreams.indexOf(res);
    if (listenerIdx !== -1) {
      bucket.configStreams.splice(listenerIdx, 1);
    }
  };
  res.on('close', removeListener);
  res.on('finish', removeListener);
};

Cluster.prototype._configChanged = function() {
  this.emit('configChanged');
};

Cluster.prototype._generateBucketNodeConfig = function(bucket, node) {
  var config = {
    couchbaseApiBaseHTTPS: 'http://127.0.0.1:' + node.capiSvc.sslPort + '/' + bucket.name,
    couchbaseApiBase: 'http://127.0.0.1:' + node.capiSvc.port + '/' + bucket.name,
    hostname: '127.0.0.1:' + node.mgmtSvc.port,
    ports: {
      httpsMgmt: node.mgmtSvc.sslPort,
      httpsCAPI: node.capiSvc.sslPort,
      sslDirect: node.memdSvc.sslPort,
      direct: node.memdSvc.port
    }
  };
  return config;
};

Cluster.prototype._generateBucketVbMap = function(bucket) {
  var config = {
    hashAlgorithm: 'CRC',
    numReplicas: bucket.numReplicas,
    serverList: [],
    sslServerList: [],
    vBucketMap: []
  };

  for (var nId = 0; nId < this.nodes.length; ++nId) {
    var node = this.nodes[nId];
    config.serverList.push('127.0.0.1:' + node.memdSvc.port);
    config.sslServerList.push('127.0.0.1:' + node.memdSvc.sslPort);
  }

  for (var vbId = 0; vbId < bucket.vbMap.length; ++vbId) {
    var vb = bucket.vbMap[vbId];

    var vbEntry = [];
    for (var repId = 0; repId < vb.length; ++repId) {
      var vbNode = this.nodeById(vb[repId]);
      vbEntry.push(this.nodes.indexOf(vbNode));
    }
    config.vBucketMap.push(vbEntry);
  }

  return config;
};

Cluster.prototype._generateBucketConfig = function(bucket) {
  var config = {
    name: bucket.name,
    bucketType: 'membase',
    authType: 'sasl',
    saslPassword: '',
    uri: '/pools/default/buckets/' + bucket.name + '?bucket_uuid=' + bucket.uuid,
    streamingUri: '/pools/default/bucketsStreaming/' + bucket.name + '?bucket_uuid=' + bucket.uuid,
    nodes: [],
    stats: {},
    ddocs: {
      uri: '/pools/default/buckets/' + bucket.name + '/ddocs'
    },
    nodeLocator: 'vbucket',
    uuid: bucket.uuid,
    vBucketServerMap: this._generateBucketVbMap(bucket),
    bucketCapabilitiesVer: '',
    bucketCapabilities: [ 'touch', 'couchapi' ]
  };
  for (var i = 0; i < this.nodes.length; ++i) {
    config.nodes.push(this._generateBucketNodeConfig(bucket, this.nodes[i]));
  }
  return config;
};

Cluster.prototype.bootstrapList = function(mode) {
  var hosts = [];
  for (var i = 0; i < this.nodes.length; ++i) {
    var node = this.nodes[i];
    if (mode === 'http') {
      hosts.push('127.0.0.1:' + node.mgmtSvc.port);
    } else if (mode === 'https') {
      hosts.push('127.0.0.1:' + node.mgmtSvc.sslPort);
    } else if (mode === 'cccp') {
      hosts.push('127.0.0.1:' + node.memdSvc.port);
    } else if (mode === 'cccps') {
      hosts.push('127.0.0.1:' + node.memdSvc.sslPort);
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

if (1 /*MOCK DEVELOPMENT*/) {
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
