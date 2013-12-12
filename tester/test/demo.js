"use strict";

var CbConn = require('../../lib/couchbase').Connection;

function CbClient(options) {
  this.me = new CbConn(options);
}
CbClient.prototype.set = function(key, value, options, callback) {
  this.me.set(key, value, options, function(err, res) {
    callback(err, res);
  });
};
CbClient.prototype.get = function(key, options, callback) {
  this.me.get(key, options, function(err, res) {
    callback(err, res);
  });
};

/**
 * @test some test
 * @needs mock
 */
exports.someTest = function(srv, done) {
  var testBucket = srv.bucketByName('default');

  var bsHosts = srv.bootstrapList('http');
  var testClient = new CbClient({
    hosts: bsHosts
  });

  testClient.set('testkeya', 'franklyn', function(err, res) {
    console.log('tst.set', err, res);

    var vbId = testBucket.keyToVbId('testkeya');
    //testBucket.changeVBucketServer(vbId,  true);

    //testBucket.loseVb(vbId, 0);
    //testBucket.loseVb(vbId, 1);

    testClient.get('testkeya', function(err, res) {
      console.log('tst.get', err, res);
    });

  });

  console.log('some test executed');
};
