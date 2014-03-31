"use strict";

var net = require('net');

var CouchbaseClient = require('../lib/couchbase').Connection;
var MockCluster = require('./mock/cluster');

require('buffer').INSPECT_MAX_BYTES = 100;

var testCluster = new MockCluster();
testCluster.prepare({}, function() {

  console.log('mock cluster online');

  var testBucket = testCluster.bucketByName('default');

  var hosts = testCluster.bootstrapList('cccp');
  console.log('host list', hosts);

  var testClient = new CouchbaseClient({
    uri: hosts,
    bucket: 'default'
  });

  testClient.set('testkeya', 'franklyn', function(err, res) {
    console.log('tst.set', err, res);

    var vbId = testClient._mapBucket('testkeya');
    //testBucket.changeVBucketServer(vbId,  true);

    //testBucket.loseVb(vbId, 0);
    //testBucket.loseVb(vbId, 1);

    testClient.get('testkeya', function(err, res) {
      console.log('tst.get', err, res);
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
