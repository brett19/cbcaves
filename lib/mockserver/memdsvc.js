"use strict";

var util = require('util');
var net = require('net');
var tls = require('tls');
var utils = require('./utils');
var memdproto = require('./memdproto');

var Long = require('long');
var BaseService = require('./basesvc');
var MemdSocket = require('./memdsocket');
var Cas = require('./cas');


function MemdStatusError(code) {
  Error.call(this);
  Error.captureStackTrace(this, MemdStatusError);
  if (code === memdproto.status.SUCCESS) {
    this.message = 'success';
  } else if (code === memdproto.status.KEY_ENOENT) {
    this.message = 'key not found';
  } else if (code === memdproto.status.KEY_EXISTS) {
    this.message = 'key already exists';
  } else if (code === memdproto.status.E2BIG) {
    this.message = 'value is too big';
  } else if (code === memdproto.status.EINVAL) {
    this.message = 'invalid arguments';
  } else if (code === memdproto.status.NOT_STORED) {
    this.message = 'not stored';
  } else if (code === memdproto.status.DELTA_BADVAL) {
    this.message = 'bad delta';
  } else if (code === memdproto.status.NOT_MY_VBUCKET) {
    this.message = 'not my vbucket';
  } else if (code === memdproto.status.AUTH_ERROR) {
    this.message = 'authentication error';
  } else if (code === memdproto.status.AUTH_CONTINUE) {
    this.message = 'authentication step continue';
  } else if (code === memdproto.status.ERANGE) {
    this.message = 'bad range';
  } else if (code === memdproto.status.UNKNOWN_COMMAND) {
    this.message = 'unknown command';
  } else if (code === memdproto.status.ENOMEM) {
    this.message = 'not enough memory';
  } else if (code === memdproto.status.NOT_SUPPORTED) {
    this.message = 'not supported';
  } else if (code === memdproto.status.EINTERNAL) {
    this.message = 'internal error';
  } else if (code === memdproto.status.EBUSY) {
    this.message = 'too busy';
  } else if (code === memdproto.status.ETMPFAIL) {
    this.message = 'temporary failure';
  } else {
    this.message = 'unknown error : ' + code;
  }
  this.code = code;
  this.name = 'MemdStatusError';
};
util.inherits(MemdStatusError, Error);

/**
 * @param parent
 * @param parentNode
 * @constructor
 */
function MemdService(parent, parentNode) {
  BaseService.call(this, parent, parentNode);

  this.clients = [];
  this.handlers = [];

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

  require('./memdmods/v25').init(this);
}
util.inherits(MemdService, BaseService);

MemdService.prototype.disconnectAll = function() {
  for (var i = 0; i < this.clients.length; ++i) {
    var stream = this.clients[i];
    stream.end();
  }
};

MemdService.prototype._handleNewClient = function(sock, packetHandler) {
  var self = this;

  // Wrap the socket for Memcached handling
  MemdSocket.upgradeSocket(sock);

  // Set up our packet handler
  sock.on('packet', packetHandler);

  // Set to default bucket
  sock.bucket = this.parent.bucketByName('default');

  // Add this socket to our client list
  this.clients.push(sock);
  sock.on('close', function() {
    var clientIdx = self.clients.indexOf(sock);
    if (clientIdx !== -1) {
      self.clients.splice(clientIdx, 1);
    }
  });
};

MemdService.prototype.registerHandler = function(handler) {
  this.handlers.push(handler);
};

MemdService.prototype._defaultHandler = function(socket, req) {
  console.log('received unknown packet', req);
  socket.writeReply(req, { status: memdproto.status.NOT_SUPPORTED });
};

MemdService.prototype._handlePacket = function(socket, packet) {
  var self = this;
  var curIdx = 0;
  (function callNext() {
    if (curIdx < self.handlers.length) {
      self.handlers[curIdx++].call(self, socket, packet, callNext);
    } else {
      self._defaultHandler(socket, packet);
    }
  })();
};

/**
 * @param {MemdSvcSocket} socket
 * @returns {Bucket}
 */
MemdService.prototype._getSocketBucket = function(socket) {
  return socket.bucket;
};

module.exports = MemdService;
