"use strict";

var util = require('util');
var net = require('net');
var tls = require('tls');
var utils = require('./utils');
var memdproto = require('./memdproto');

var BaseService = require('./basesvc');
var MemdSocket = require('./memdsocket');
var Cas = require('./cas');


function MemdStatusError(code) {
  Error.call(this);
  Error.captureStackTrace(this, MemdStatusError);
  if (code === memdproto.status.EINVAL) {
    this.message = 'invalid arguments';
  } else {
    this.message = 'unknown error : ' + code;
  }
  this.code = code;
  this.name = 'MemdStatusError';
};
util.inherits(MemdStatusError, Error);


function MemdService(parent) {
  BaseService.call(this, parent);

  this.clients = [];

  this.routes = {};
  this.routes[memdproto.cmd.SASL_AUTH] = this._handleSaslAuth;
  this.routes[memdproto.cmd.GET_CLUSTER_CONFIG] = this._handleGetConfig;
  this.routes[memdproto.cmd.SET] = this._handleSet;
  this.routes[memdproto.cmd.GET] = this._handleGet;

  var self = this;

  this.server = net.createServer(function(sock) {
    self._handleNewClient(sock, function(data) {
      if (!self.paused) {
        self._handlePacket(sock, data);
      }
    });
  });

  this.sslServer = tls.createServer(this.parent.sslCreds, function(sock) {
    self._handleNewClient(sock, function(data) {
      if (!self.sslPaused) {
        self._handlePacket(sock, data);
      }
    });
  });
}
util.inherits(MemdService, BaseService);

MemdService.prototype._handleNewClient = function(sock, packetHandler) {
  var self = this;

  // Wrap the socket for Memcached handling
  MemdSocket.upgradeSocket(sock);

  // Set up our packet handler
  sock.on('packet', packetHandler);

  // Add this socket to our client list
  this.clients.push(sock);
  sock.on('close', function() {
    var clientIdx = self.clients.indexOf(sock);
    if (clientIdx !== -1) {
      self.clients.splice(clientIdx, 1);
    }
  });
};

MemdService.prototype._routePacket = function(socket, packet) {
  if (packet.magic !== memdproto.magic.REQUEST) {
    // TODO: Confirm this is the correct behaviour
    return;
  }

  var handler = this.routes[packet.op];
  if (handler) {
    handler.call(this, socket, packet);
  } else {
    throw new MemdStatusError(memdproto.status.UNKNOWN_COMMAND);
  }
};

MemdService.prototype._handlePacket = function(socket, packet) {
  try {
    this._routePacket(socket, packet);
  } catch (e) {
    console.warn(e);
    if (e instanceof MemdStatusError) {
      if (e.code === memdproto.status.NOT_MY_VBUCKET) {
        // TODO: CCCP NMV
        socket.writeErrorResp(packet, e.code);
      } else {
        socket.writeErrorResp(packet, e.code);
      }
    } else {
      throw e;
    }
  }
};

MemdService.prototype._getSocketBucket = function(socket) {
  var bucket = socket.bucket;
  if (!bucket) {
    // TODO: Confirm this is the correct behaviour
    throw new MemdStatusError(memdproto.status.ETMPFAIL);
  }
  return bucket;
};

MemdService.prototype._handleSaslAuth = function(socket, req) {
  var authType = req.key.toString('utf8');

  var parts = req.value.split(0);
  if (parts.length !== 3) {
    throw new MemdStatusError(memdproto.status.EINVAL);
  }

  var authzid = parts[0].toString('utf8');
  var authcid = parts[1].toString('utf8');
  var passwd = parts[2].toString('utf8');

  var bucket = this.parent.bucketByName(authcid);
  if (!bucket) {
    // TODO: This can't be accurate...
    throw new MemdStatusError(memdproto.status.EINVAL);
  }

  socket.bucket = bucket;
  socket.writeSaslAuthSuccessResp(req, 'success');
};

MemdService.prototype._handleGetConfig = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var config = this.parent._generateBucketConfig(bucket);
  socket.writeSVResp(req,
    memdproto.status.SUCCESS,
    new Buffer(JSON.stringify(config), 'utf8')
  );
};

MemdService.prototype._handleSet = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, 0, req.key);
  var kVal = bucket.getKeyData(kRef);

  if (kVal && !Cas.compare(kVal.cas, req.cas)) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  } else if (!kVal && !Cas.compare(null, req.cas)) {
    throw new MemdStatusError(memdproto.status.KEY_EXISTS);
  }

  kVal = {
    cas: new Cas(),
    dataType: req.dataType,
    flags: req.flags,
    value: req.value
  };
  bucket.setKeyData(kRef, kVal);

  socket.writeSCResp(req,
    memdproto.status.SUCCESS,
    kVal.cas
  );
};

MemdService.prototype._handleGet = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, 0, req.key);
  var kVal = bucket.getKeyData(kRef);

  if (!kVal) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  }

  socket.writeGetResp(req,
    memdproto.status.SUCCESS,
    kVal.cas,
    kVal.dataType,
    kVal.flags,
    kVal.value
  );
};

module.exports = MemdService;
