"use strict";

var uuid = require('node-uuid');
var crc32 = require('./crc32');

function Bucket(parent, name, options) {
  this.parent = parent;
  this.name = name;
  this.uuid = uuid.v4();
  this.numReplicas = options.numReplicas;
  this.configStreams = [];
  this.lostVbs = [];

  this.vbMap = [];
  for (var i = 0; i < parent.numVbuckets; ++i) {
    this.vbMap[i] = [];
    for (var j = 0; j < 1 + this.numReplicas; ++j) {
      this.vbMap[i][j] = -1;
    }
  }

  this.values = [];
  for (var k = 0; k < parent.numVbuckets + 1; ++k) {
    this.values[k] = {};
  }
}


Bucket.prototype.keyToVbId = function(key) {
  var keyCrc = crc32(key);
  return keyCrc % this.vbMap.length;
};

Bucket.prototype.realNumReplicas = function() {
  return Math.min(this.numReplicas, this.parent.nodes.length);
};



Bucket.prototype.vbIsLost = function(vbId, nodeId) {
  return this.lostVbs.indexOf(vbId + '-' + nodeId) !== -1;
};
Bucket.prototype.resetLostVbs = function() {
  this.lostVbs = [];
};
Bucket.prototype.loseVb = function(vbId, repId) {
  var vb = this.vbMap[vbId];
  if (vb.length <= repId) {
    throw new Error('invalid replica id');
  } else if (vb[repId] === -1) {
    throw new Error('vb isnt assigned anyways');
  }
  this.lostVbs.push(vbId + '-' + vb[repId]);
};



Bucket.prototype.changeVBucketServer = function(vbId, notify) {
  if (this.parent.nodes.length < 2) {
    throw new Error('not enough server to rotate vbucket');
  }

  var vb = this.vbMap[vbId];
  this.vbMap[vbId] = this.parent._createVbucketMap(vb, 1+this.numReplicas);

  if (notify) {
    this.parent._configChanged();
  }
};



/*
This likely has hilariously horrible performance...
 */
Bucket.prototype.forEachKey = function(repId, callback) {
  for (var i in this.values) {
    if (this.values.hasOwnProperty(i)) {
      var xKey = i.split('|');
      var keyRepId = parseInt(xKey[0], 10);
      var keyVbId = parseInt(xKey[1], 10);
      var keyKey = xKey[2];

      if (keyRepId === repId) {
        callback(keyKey, keyVbId, this.values[i]);
      }
    }
  }
};

Bucket.prototype.getKeyRef = function(vbId, repId, key) {
  return repId + '|' + vbId + '|' + key.toString('base64');
};
Bucket.prototype.getKeyData = function(keyRef) {
  var value = this.values[keyRef];
  if (!value) {
    return null;
  }
  return value;
};
Bucket.prototype.setKeyData = function(keyRef, value) {
  this.values[keyRef] = value;
};
Bucket.prototype.removeKeyData = function(keyRef) {
  delete this.values[keyRef];
};


Bucket.prototype.nodeHasVbs = function(nodeId) {
  for (var vbId = 0; vbId < this.vbMap.length; ++vbId) {
    if (this.nodeRepId(vbId, nodeId) !== -1) {
      return true;
    }
  }
  return false;
};

Bucket.prototype.nodeRepId = function(vbId, nodeId) {
  var vb = this.vbMap[vbId];
  for (var repId = 0; repId < vb.length; ++repId) {
    if (vb[repId] === nodeId) {
      return repId;
    }
  }
  return -1;
};

module.exports = Bucket;
