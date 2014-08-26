'use strict';

var HtModule = require('../htmodule');

var mod = new HtModule();

mod.htGet('/pools/:pool/buckets/:bucket',
    function(req, resp, next) {
  if (req.params.pool !== 'default') {
    return resp.send(404);
  }

  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    return resp.send(404);
  }

  var config = this.parent._generateBucketConfig(bucket);
  resp.send(200, config);
});

mod.htGet('/pools/:pool/bucketsStreaming/:bucket',
    function(req, resp, next) {
  if (req.params.pool !== 'default') {
    return resp.send(404);
  }

  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    return resp.send(404);
  }

  resp.writeHead(200, {
    'Content-Type': 'application/json',
    'Transfer-Encoding': 'chunked'
  });

  var config = this.parent._generateBucketConfig(bucket);
  var configStr = JSON.stringify(config);
  resp.write(configStr + '\n\n\n\n');

  bucket.on('deleted', function() {
    resp.close();
  });

  this._addConfigListener(bucket, resp);
});

module.exports = mod;
