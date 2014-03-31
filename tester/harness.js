"use strict";

var assert = require('assert');
var utils = require('./mock/utils');

var Cas = require('./mock/cas');
var CbConn = require('../lib/couchbase').Connection;

function CbClient(options, callback) {
  this.me = new CbConn(options, callback);
}
CbClient.prototype.destroy = function(callback) {
  this.me.shutdown();
  callback();
};
CbClient.prototype.set = function(key, value, options, callback) {
  this.me.set(key, value, options, function(err, res) {
    callback(err, res);
  });
};
CbClient.prototype.add = function(key, value, options, callback) {
  this.me.add(key, value, options, function(err, res) {
    callback(err, res);
  });
};
CbClient.prototype.replace = function(key, value, options, callback) {
  this.me.replace(key, value, options, function(err, res) {
    callback(err, res);
  });
};
CbClient.prototype.get = function(key, options, callback) {
  this.me.get(key, options, function(err, res) {
    callback(err, res);
  });
};

/* Fake Client For Now (Only RI Client) */

function Harness() {
  this.srv = null;

  this.keySerial = 0;
  this.bkeySerial = 0;
  this.clients = [];

  this.internalClient = null;
}

Harness.prototype.setKey = function(key, value, callback) {
  // TODO: This needs to not look so terrible in the Harness....
  var bucket = this.srv.bucketByName('default');
  var vbId = bucket.keyToVbId(key);
  var kRef = bucket.getKeyRef(vbId, new Buffer(key, 'utf8'));
  bucket.setKeyData(kRef, {
    cas: new Cas(),
    flags: 0,
    dataType: 0,
    value: new Buffer(value)
  });
  callback();
};
Harness.prototype.removeKey = function(key, callback) {
  // TODO: This needs to not look so terrible in the Harness....
  var bucket = this.srv.bucketByName('default');
  var vbId = bucket.keyToVbId(key);
  var kRef = bucket.getKeyRef(vbId, new Buffer(key, 'utf8'));
  bucket.removeKeyData(kRef);
  callback();
};

Harness.prototype.newClient = function(options, callback) {
  var client = new CbClient(options, callback);
  this.clients.push(client);
  return client;
};

Harness.prototype.destroyClient = function(client, callback) {
  var clientIdx = this.clients.indexOf(client);
  if (clientIdx !== -1) {
    this.clients.splice(clientIdx, 1);
  }

  client.destroy(callback);
};

Harness.prototype.destroy = function(callback) {
  var maybeCallback = utils.waitForAllCallback(callback);
  var maybeFinish = maybeCallback();
  while (this.clients.length > 0) {
    this.destroyClient(this.clients[0], maybeCallback());
  }
  maybeFinish();
};

Harness.prototype.genKey = function(prefix) {
  if (!prefix) {
    prefix = "generic";
  }

  var ret = "TEST-" + prefix + this.keySerial;
  this.keySerial++;
  return ret;
};

Harness.prototype.genBKey = function(len) {
  var key = new Buffer(len);
  var rngVal = this.bkeySerial;
  for (var i = 0; i < len; ++i) {
    rngVal = 1103515245 * rngVal + 12345;
    key[i] = Math.floor(rngVal % 256);
  }
  this.bkeySerial++;
  return key;
};

Harness.prototype.okCallback = function(target) {
  // Get the stack
  var origStack = new Error().stack;

  return function(err, result) {
    if (err) {
      assert(!err, 'Received unexpected error:' + err.stack + origStack);
    }
    if (result) {
      assert(result, 'Missing expected result:' + origStack);
    }
    target(result);
  };
};

module.exports = Harness;
