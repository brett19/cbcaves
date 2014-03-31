"use strict";

var assert = require('assert');

var CbConn = require('../../lib/couchbase').Connection;

function CbClient(options) {
  this.me = new CbConn(options);
}
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

function Harness() {
  this.keySerial = 0;
  this.bkeySerial = 0;
}

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

var H = new Harness();



/**
 * @test basic add tests
 */
exports.basicAdd = function(srv, done) {
  var httpHosts = srv.bootstrapList('http');
  var cli = new CbClient({
    hosts: httpHosts
  });

  var testKey = H.genKey('add');

  cli.add(testKey, 'bar', H.okCallback(function(res) {
    done();
  }));
};

/**
 * @test secondary add tests
 */
exports.addWorks = function(srv, done) {
  var httpHosts = srv.bootstrapList('http');
  var cli = new CbClient({
    hosts: httpHosts
  });

  var testKey = H.genKey('add');

  cli.add(testKey, 'bar', H.okCallback(function(res) {
    cli.add(testKey, 'baz', function(err, res) {
      assert(err, 'Should fail to add object second time.');
      done();
    });
  }));
};

/**
 * @test binary key add tests
 * @needs binary_key
 */
exports.bkeyAdd = function(srv, done) {
  var httpHosts = srv.bootstrapList('http');
  var cli = new CbClient({
    hosts: httpHosts
  });

  var testKey = H.genBKey(32);

  cli.add(testKey, 'bar', H.okCallback(function(res) {
    cli.add(testKey, 'baz', function(err, res) {
      assert(err, 'Should fail to add object second time.');
      done();
    });
  }));
};
