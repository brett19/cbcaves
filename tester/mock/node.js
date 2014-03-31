"use strict";

var fs = require('fs');
var net = require('net');
var tls = require('tls');
var http = require('http');
var https = require('https');
var express = require('express');
var utils = require('./utils');

var MockMemdClient = require('./memdclient');
var MgmtService = require('./mgmtsvc');
var CapiService = require('./capisvc');
var MemdService = require('./memdsvc');

var MOCK_SSL_KEY = fs.readFileSync(__dirname + '/local-ssl-key.pem');
var MOCK_SSL_CERT = fs.readFileSync(__dirname + '/local-ssl-cert.pem');

var globalNodeId = 1;

function Node(parent, callback) {
  this.parent = parent;
  this.nodeId = globalNodeId++;

  this.host = '127.0.0.1';

  this.capiSvc = new CapiService(this.parent, this);
  this.mgmtSvc = new MgmtService(this.parent, this);
  this.memdSvc = new MemdService(this.parent, this);

  var self = this;

  this.startAllServices(function() {
    console.info('NEW NODE STARTED');
    console.info('  capi: ' + self.capiSvc.port + ', ' + self.capiSvc.sslPort);
    console.info('  mgmt: ' + self.mgmtSvc.port + ', ' + self.mgmtSvc.sslPort);
    console.info('  memd: ' + self.memdSvc.port + ', ' + self.memdSvc.sslPort);

    callback();
  });

  /*
  var creds = {
    key: MOCK_SSL_KEY,
    cert: MOCK_SSL_CERT
  };

  var self = this;
  var waitRemain = 6;
  var maybeCallback = function() {
    if (--waitRemain === 0) {
      self.capiPort = self.capiSrv.address().port;
      self.capiSslPort = self.capiSrvSsl.address().port;
      self.mgmtPort = self.mgmtSrv.address().port;
      self.mgmtSslPort = self.mgmtSrvSsl.address().port;
      self.memdPort = self.memdSrv.address().port;
      self.memdSslPort = self.memdSrvSsl.address().port;

      console.info('NEW NODE STARTED');
      console.info('  capi: ' + self.capiPort + ', ' + self.capiSslPort);
      console.info('  mgmt: ' + self.mgmtPort + ', ' + self.mgmtSslPort);
      console.info('  memd: ' + self.memdPort + ', ' + self.memdSslPort);

      callback();
    }
  };

  var capiSrv = express();
  this.capiSrv = http.createServer(capiSrv);
  this.capiSrv.listen(this.parent.pickSvcPort(), maybeCallback);
  this.capiSrvSsl = https.createServer(creds, capiSrv);
  this.capiSrvSsl.listen(this.parent.pickSvcPort(), maybeCallback);

  var mgmtSrv = express();
  this._setupMgmtRoutes(mgmtSrv);
  this.mgmtSrv = http.createServer(mgmtSrv);
  this.mgmtSrv.listen(this.parent.pickSvcPort(), maybeCallback);
  this.mgmtSrvSsl = https.createServer(creds, mgmtSrv);
  this.mgmtSrvSsl.listen(this.parent.pickSvcPort(), maybeCallback);

  var handleMemdClient = function(c) {
    self._handleMemdClient(c);
  };
  this.memdSrv = net.createServer(handleMemdClient);
  this.memdSrv.listen(this.parent.pickSvcPort(), maybeCallback);
  this.memdSrvSsl = tls.createServer(creds, handleMemdClient);
  this.memdSrvSsl.listen(this.parent.pickSvcPort(), maybeCallback);
  */
}

Node.prototype.startAllServices = function(callback) {
  var maybeCallback = utils.waitForAllCallback(callback);
  this.capiSvc.startAll(maybeCallback());
  this.mgmtSvc.startAll(maybeCallback());
  this.memdSvc.startAll(maybeCallback());
};
Node.prototype.stopAllServices = function(callback) {
  var maybeCallback = utils.waitForAllCallback(callback);
  this.capiSvc.stopAll(maybeCallback());
  this.mgmtSvc.stopAll(maybeCallback());
  this.memdSvc.stopAll(maybeCallback());
};
Node.prototype.pauseAllServices = function() {
  this.capiSvc.pauseAll();
  this.mgmtSvc.pauseAll();
  this.memdSvc.pauseAll();
};
Node.prototype.resumeAllServices = function() {
  this.capiSvc.resumeAll();
  this.mgmtSvc.resumeAll();
  this.memdSvc.resumeAll();
};

Node.prototype._setupMgmtRoutes = function(app) {
  app.get('/pools/default/buckets/:bucket', this._rMgmtBucket.bind(this));
  app.get('/pools/default/bucketsStreaming/:bucket', this._rMgmtBucketStreaming.bind(this));
};

Node.prototype._rMgmtBucket = function(req, res, next) {
  var bucket = this.parent.buckets[req.params.bucket];
  if (!bucket) {
    return res.send(404);
  }

  var config = this.parent._generateBucketConfig(bucket);
  res.send(200, config);
};

Node.prototype._rMgmtBucketStreaming = function(req, res, next) {
  var bucket = this.parent.buckets[req.params.bucket];
  if (!bucket) {
    return res.send(404);
  }

  res.writeHead(200, {
    'Content-Type': 'application/json',
    'Transfer-Encoding': 'chunked'
  });

  var config = this.parent._generateBucketConfig(bucket);
  var configStr = JSON.stringify(config);
  res.write(configStr);
  res.write('\n\n\n\n');

  this.parent._addConfigListener(bucket, res);
};

Node.prototype.close = function() {

};

Node.prototype.isVBucketMaster = function(bucket, vbId) {
  if (bucket.vbIsLost(vbId, this.nodeId)) {
    return false;
  }

  var maps = bucket.vbMap[vbId];
  return maps[0] === this.nodeId;
};
Node.prototype.isVBucketReplica = function(bucket, vbId) {
  if (bucket.vbIsLost(vbId, this.nodeId)) {
    return false;
  }

  var maps = bucket.vbMap[vbId];
  for (var i = 1; i < maps.length; ++i) {
    if (maps[i] === this.nodeId) {
      return true;
    }
  }
  return false;
};

Node.prototype._handleCapiReq = function(req, res, next) {
  console.log('capi request: ' + req.path);
};

Node.prototype._handleMgmtReq = function(req, res, next) {

};

Node.prototype._handleMemdClient = function(c) {
  var self = this;

  var client = new MockMemdClient(this.parent, this, c);

  this.memdClients.push(client);
  client.on('close', function() {
    var clientIdx = this.memdClients.indexOf(client);
    if (clientIdx !== -1) {
      this.memdClients.splice(clientIdx, 1);
    }
  });
};

module.exports = Node;
