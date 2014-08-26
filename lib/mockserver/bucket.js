"use strict";

var uuid = require('uuid');
var util = require('util');
var crc32 = require('./crc32');
var utils = require('./utils');
var EventEmitter = require('events').EventEmitter;
var Cas = require('./cas');

/**
 * @class DesignDocument
 * @property {Object} views
 * @property {Object} spatial
 */

/**
 * @param {Cluster} parent
 * @param {string} name
 * @param {BucketConfig} options
 * @param {number} [options.numReplicas]
 * @param {number} [options.numVbuckets]
 *
 * @constructor
 *
 * @property {Object.<string,DesignDocument>} ddocs
 */
function Bucket(parent, name, options) {
  this.parent = parent;
  this.name = name;
  this.password = '';
  this.uuid = uuid.v4();
  this.numReplicas = options.numReplicas;

  this.vbMap = [];
  for (var i = 0; i < parent.numVbuckets; ++i) {
    this.vbMap[i] = [];
    for (var j = 0; j < 1 + this.numReplicas; ++j) {
      this.vbMap[i][j] = -1;
    }
  }

  this.values = {};
  this.ddocs = {};

  this._data = {};

  var self = this;
  parent.on('bucketDeleted', function(bucket) {
    self.emit('close');
  });
}
util.inherits(Bucket, EventEmitter);

Bucket.prototype.bucketDataByNodeId = function(nodeId) {
  return this._data[nodeId];
};

/**
 * Calculates the vbucket number for a particular key based on the
 * couchbase standard crc32 mapping.
 *
 * @param {Buffer|string} key
 * @returns {number}
 */
Bucket.prototype.keyToVbId = function(key) {
  var keyCrc = crc32.hash(key);
  return keyCrc % this.vbMap.length;
};

/**
 * Determines if a particular node is the active main or replica for
 * any vbuckets.
 *
 * @param {integer} nodeId
 * @returns {boolean}
 */
Bucket.prototype.nodeHasVbs = function(nodeId) {
  for (var vbId = 0; vbId < this.vbMap.length; ++vbId) {
    if (this.nodeRepId(vbId, nodeId) !== -1) {
      return true;
    }
  }
  return false;
};

/**
 * Determines which replica set a particular node represents for
 * a given vbucket number.  Returns -1 if the node does not contain
 * a replica set for the specified vbucket.
 *
 * @param {number} vbId
 * @param {number} nodeId
 * @returns {number}
 */
Bucket.prototype.nodeRepId = function(vbId, nodeId) {
  var vb = this.vbMap[vbId];
  for (var repId = 0; repId < vb.length; ++repId) {
    if (vb[repId] === nodeId) {
      return repId;
    }
  }
  return -1;
};


/*
 *   DATA STORAGE STUFF
 */

/**
 * Loops through each key in a particular value set and executes
 * a callback for each item found.
 * @param {number} repId
 * @param {function(string,number,Object)} callback
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

/**
 * Represents a single item that has been stored within the Bucket.
 * This class holds all the details of a stored item and is mapped
 * by a KeyRef.
 *
 * @class
 * @property {Buffer} value
 * @property {CAS} cas
 * @property {number} dataType
 * @property {number} flags
 * @property {time} expiry
 * @property {time} lockTime
 * @property {boolean} deleted
 */
function KeyData(opts) {
  this.cas = opts.cas;
  this.value = opts.value;
  this.dataType = opts.dataType;
  this.flags = opts.flags;
  this.expiry = opts.expiry;
  this.lockTime = opts.lockTime;
  this.deleted = opts.deleted;
}

// TODO: Likely am unneccessarily copying data here.
// Should define some semantics on how to handle this nicely.
/**
 * @param {KeyData} data
 * @returns {KeyData}
 * @private
 */
Bucket.prototype._cloneKeyData = function(data) {
  if (!data.deleted) {
    return new KeyData({
      cas: data.cas,
      value: data.value,
      dataType: data.dataType,
      flags: data.flags,
      expiry: data.expiry,
      lockTime: data.lockTime
    });
  } else {
    return new KeyData({
      cas: data.cas,
      deleted: true
    });
  }
};

/**
 * Builds a KeyRef object from a vbId and a key for use
 * when performing operations against a buckets key storage.
 * @param {number} vbId
 * @param {Buffer|string} key
 * @returns {KeyRef}
 */
Bucket.prototype.getKeyRef = function(vbId, key) {
  if (vbId === undefined || key === undefined) {
    throw new Error('invalid key reference');
  }

  var encKey = key.toString('base64');
  return {
    vbId: vbId,
    key: encKey
  };
};

/**
 * Generates a string reference for a particular KeyRef.
 * @param {KeyRef} keyRef
 * @returns {string}
 * @private
 */
Bucket.prototype._getKeyStr = function(keyRef) {
  return keyRef.vbId + '|' + keyRef.key;
};

/**
 * @param {KeyRef} keyRef
 * @param {number} repId
 * @returns {string}
 * @private
 */
Bucket.prototype._getKeyRepStr = function(keyRef, repId) {
  return repId + '|' + this._getKeyStr(keyRef);
};
Bucket.prototype._getKeyDataEx = function(keyRef, repId) {
  var kStr = this._getKeyRepStr(keyRef, repId);
  var value = this.values[kStr];
  if (!value) {
    return null;
  }
  if (value.expiry && value.expiry < this.parent.clock.curMs()) {
    return this.removeKeyData(kStr);
  }
  if (value.lockTime && value.lockTime < this.parent.clock.curMs()) {
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
  var kStr = this._getKeyRepStr(keyRef, repId);

  value.persisted = false;
  this.values[kStr] = value;

  this.parent.clock.newTimeout(PERSIST_DELAY, function() {
    value.persisted = true;
  });
};
Bucket.prototype.setKeyData = function(keyRef, value) {
  this._setKeyDataEx(keyRef, 1, value);

  var self = this;
  for (var i = 0; i < self.numReplicas; ++i) {
    (function(repId) {
      var repValue = self._cloneKeyData(value);
      self.parent.clock.newTimeout(REPLICATION_DELAY, function() {
        self._setKeyDataEx(keyRef, repId, repValue);
      });
    })(2 + i);
  }

  var indexValue = self._cloneKeyData(value);
  this.parent.clock.newTimeout(INDEXER_DELAY, function() {
    self._setKeyDataEx(keyRef, 0, indexValue);
  });
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

module.exports = Bucket;
