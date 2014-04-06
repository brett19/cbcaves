"use strict";

var util = require('util');

var HttpService = require('./httpsvc');

function MgmtService(parent, parentNode) {
  HttpService.call(this, parent, parentNode);

  this.configStreams = [];

  var self = this;

  this.parent.on('configChanged', function(bucket) {
    self._writeConfigToStreamers();
  });
}
util.inherits(MgmtService, HttpService);

MgmtService.prototype._addConfigListener = function(bucket, stream) {
  var self = this;

  stream.bucket = bucket;

  this.configStreams.push(stream);
  var removeListener = function() {
    var streamIdx = self.configStreams.indexOf(stream);
    if (streamIdx !== -1) {
      self.configStreams.splice(streamIdx, 1);
    }
  };
  stream.on('close', removeListener);
  stream.on('finish', removeListener);
};
MgmtService.prototype._writeConfigToStreamers = function() {
  for (var i = 0; i < this.configStreams.length; ++i) {
    var stream = this.configStreams[i];

    var config = this.parent._generateBucketConfig(stream.bucket);
    stream.write(JSON.stringify(config) + '\n\n\n\n');
  }
};

MgmtService.prototype.disconnectAll = function() {
  for (var i = 0; i < this.configStreams.length; ++i) {
    var stream = this.configStreams[i];
    stream.end();
  }
};

MgmtService.prototype._setupRoutes = function(app) {
  app.get('/pools/:pool/buckets/:bucket', this._handleBucket.bind(this));
  app.get('/pools/:pool/bucketsStreaming/:bucket', this._handleBucketStreaming.bind(this));
};

MgmtService.prototype._handleBucket = function(req, res, next) {
  if (req.params.pool !== 'default') {
    return res.send(404);
  }

  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    return res.send(404);
  }

  var config = this.parent._generateBucketConfig(bucket);
  res.send(200, config);
};

MgmtService.prototype._handleBucketStreaming = function(req, res, next) {
  if (req.params.pool !== 'default') {
    return res.send(404);
  }

  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    return res.send(404);
  }

  res.writeHead(200, {
    'Content-Type': 'application/json',
    'Transfer-Encoding': 'chunked'
  });

  var config = this.parent._generateBucketConfig(bucket);
  var configStr = JSON.stringify(config);
  res.write(configStr + '\n\n\n\n');

  this._addConfigListener(bucket, res);
};

module.exports = MgmtService;
