"use strict";

var net = require('net');

var CouchbaseClient = require('../client/couchbase').Connection;
var MockCluster = require('../mockserver/cluster');

require('buffer').INSPECT_MAX_BYTES = 100;

var testCluster = new MockCluster();
testCluster.prepare({}, function() {
  console.log('[testcode] mock cluster started');

  var hosts = testCluster.bootstrapList('cccp');
  console.log('[testcode] cccp bootstrap list:');
  for (var i = 0; i < hosts.length; ++i) {
    console.log('[testcode]   ' + hosts[i]);
  }

  testCluster._executeQuery = function(q, callback) {
    if (q === 'SELECT * FROM invalid_bucket') {
      return callback(
        new error.N1qlError(
          5000,
          'Internal Error',
          'Optimizer Error',
          'Bucket `?` does not exist'
        ), null);
    }
    callback(null, []);
  };

  //var testBucket = testCluster.bucketByName('default');

  var testClient = new CouchbaseClient({
    uri: hosts,
    bucket: 'default'
  });

  testClient.set('testkeya', 'franklyn', function(err, res) {
    console.log('[testcode] tst.set', err, res);

    var vbId = testClient._mapBucket('testkeya');
    //testBucket.changeVBucketServer(vbId,  true);

    //testBucket.loseVb(vbId, 0);
    //testBucket.loseVb(vbId, 1);

    testClient.get('testkeya', function(err, res) {
      console.log('[testcode] tst.get', err, res);
    });

  });

  //testCluster.removeNode(1);
  /*
   setInterval(function() {
   testCluster.addNodes(1, function(){});
   }, 2500);
   */

});

var server = net.createServer(function(c) {

});
server.listen(9944);
