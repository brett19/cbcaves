"use strict";

/*
Supports Flags:
  couchbase       - Supports Couchbase Buckets
  memcached       - Supports Memcached Buckets
  cccp_config     - Supports config via CCCP
  http_config     - Supports config via HTTP
  ssl             - Supports SSL
  observe         - Supports the OBSERVE operation
  endure          - Supports the ENDURE operation
  op_durability   - Supports durability requirements on operations
  bigint          - Supports 64-bit integers (INCR/DECR)
  multi_op        - Supports pipelining operations
  config_cache    - Supports configuration cacheing
  replica_get     - Supports retrieving values from replicas
  binary_key      - Supports binary keys
  async           - Supports async operations

@test
@needs

 */

var net = require('net');

var CouchbaseClient = require('../lib/couchbase').Connection;
var MockCluster = require('./mock/cluster');

require('buffer').INSPECT_MAX_BYTES = 100;

var testCluster = new MockCluster();
testCluster.prepare({}, function() {

  console.log('mock cluster online');

  var testBucket = testCluster.createBucket('default', {numReplicas: 1});

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
