"use strict";

var uuid = require('node-uuid');
var crc32 = require('./crc32');
var utils = require('./utils');

var Cas = require('./cas');

function Bucket(parent, name, options) {
  this.parent = parent;
  this.name = name;
  this.uuid = uuid.v4();
  this.numReplicas = options.numReplicas;
  this.lostVbs = [];

  this.vbMap = [];
  for (var i = 0; i < parent.numVbuckets; ++i) {
    this.vbMap[i] = [];
    for (var j = 0; j < 1 + this.numReplicas; ++j) {
      this.vbMap[i][j] = -1;
    }
  }

  this.values = {};
  this.ddocs = {};
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
      var value = this.values[i];

      // Don't expose deleted keys
      if (value.deleted) {
        continue;
      }

      // Only callback for the right replication ids
      if (keyRepId === repId) {
        callback(keyKey, keyVbId, value);
      }
    }
  }
};

// TODO: Likely am unneccessarily copying data here.
// Should define some semantics on how to handle this nicely.
Bucket.prototype._cloneKeyData = function(data) {
  if (!data.deleted) {
    return {
      cas: data.cas,
      value: data.value,
      dataType: data.dataType,
      flags: data.flags,
      expiry: data.expiry,
      lockTime: data.lockTime
    };
  } else {
    return {
      cas: data.cas,
      deleted: true
    };
  }
};
Bucket.prototype.getKeyRef = function(vbId, key) {
  if (vbId === undefined || key === undefined) {
    throw new Error('invalid key reference');
  }

  // TODO: Replace this once testing is done.
  var encKey = key.toString('utf8');
  //var encKey = key.toString('base64');

  return {
    vbId: vbId,
    key: encKey
  };
};
Bucket.prototype._getKeyStr = function(keyRef, repId) {
  return repId + '|' + keyRef.vbId + '|' + keyRef.key;
};
Bucket.prototype._getKeyDataEx = function(keyRef, repId) {
  var kStr = this._getKeyStr(keyRef, repId);
  var value = this.values[kStr];
  if (!value) {
    return null;
  }
  if (value.expiry && value.expiry < utils.unixTimestamp() / 1000) {
    return this.removeKeyData(kStr);
  }
  if (value.lockTime && value.lockTime < utils.unixTimestamp() / 1000) {
    delete value.lockTime;
  }
  return value;
};
Bucket.prototype.getKeyData = function(keyRef) {
  var value = this._getKeyDataEx(keyRef, 1);
  if (!value || value.deleted) {
    return null;
  }
  return value;
};

// TODO: There will be an issue if the replicator delay is high, then reduced.
//  The callback could be invoked on the lower timeout set for a key, then
//  overwritten by the longer-delayed set later.  In order to fix this, the
//  replicator will need to be set up in a queued fashion so that turning down
//  the replicator delay can flush out previous delayed items.
var PERSIST_DELAY = 0;
var REPLICATION_DELAY = 0;
var INDEXER_DELAY = 0;
Bucket.prototype._setKeyDataEx = function(keyRef, repId, value) {
  var kStr = this._getKeyStr(keyRef, repId);

  value.persisted = false;
  this.values[kStr] = value;

  setTimeout(function() {
    value.persisted = true;
  }, PERSIST_DELAY);
};
Bucket.prototype.setKeyData = function(keyRef, value) {
  this._setKeyDataEx(keyRef, 1, value);

  var self = this;
  for (var i = 0; i < self.numReplicas; ++i) {
    (function(repId) {
      var repValue = self._cloneKeyData(value);
      setTimeout(function() {
        self._setKeyDataEx(keyRef, repId, repValue);
      }, REPLICATION_DELAY);
    })(2 + i);
  }

  var indexValue = self._cloneKeyData(value);
  setTimeout(function() {
    self._setKeyDataEx(keyRef, 0, indexValue);
  }, INDEXER_DELAY);
};
Bucket.prototype.removeKeyData = function(keyRef) {
  var value = this.getKeyData(keyRef);
  if (!value) {
    return null;
  }

  var newValue = {
    cas: value.cas,//new Cas(),
    deleted: true
  };
  this.setKeyData(keyRef, newValue);
  return newValue;
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
