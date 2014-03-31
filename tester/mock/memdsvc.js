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


function MemdService(parent, parentNode) {
  BaseService.call(this, parent, parentNode);

  this.clients = [];

  // TODO: Make Quiet versions operate properly for each operation
  this.routes = {};
  this.routes[memdproto.cmd.SASL_AUTH] = this._handleSaslAuth;
  this.routes[memdproto.cmd.GET_CLUSTER_CONFIG] = this._handleGetConfig;
  this.routes[memdproto.cmd.SET] = this._handleSet;
  this.routes[memdproto.cmd.SETQ] = this._handleSetQ;
  this.routes[memdproto.cmd.ADD] = this._handleAdd;
  this.routes[memdproto.cmd.ADDQ] = this._handleAddQ;
  this.routes[memdproto.cmd.APPEND] = this._handleAppend;
  this.routes[memdproto.cmd.APPENDQ] = this._handleAppendQ;
  this.routes[memdproto.cmd.PREPEND] = this._handlePrepend;
  this.routes[memdproto.cmd.PREPENDQ] = this._handlePrependQ;
  this.routes[memdproto.cmd.REPLACE] = this._handleReplace;
  this.routes[memdproto.cmd.REPLACEQ] = this._handleReplaceQ;
  this.routes[memdproto.cmd.GET] = this._handleGet;
  this.routes[memdproto.cmd.GETQ] = this._handleGetQ;
  this.routes[memdproto.cmd.DELETE] = this._handleRemove;
  this.routes[memdproto.cmd.DELETEQ] = this._handleRemoveQ;
  this.routes[memdproto.cmd.TOUCH] = this._handleTouch;
  this.routes[memdproto.cmd.GAT] = this._handleGetAndTouch;
  this.routes[memdproto.cmd.GATQ] = this._handleGetAndTouchQ;
  this.routes[memdproto.cmd.INCREMENT] = this._handleIncr;
  this.routes[memdproto.cmd.INCREMENTQ] = this._handleIncrQ;
  this.routes[memdproto.cmd.DECREMENT] = this._handleDecr;
  this.routes[memdproto.cmd.DECREMENTQ] = this._handleDecrQ;
  this.routes[memdproto.cmd.GET_LOCKED] = this._handleGetLocked;
  this.routes[memdproto.cmd.UNLOCK_KEY] = this._handleUnlock;
  this.routes[memdproto.cmd.OBSERVE] = this._handleObserve;

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

MemdService.prototype._expiryToTs = function(expiry) {
  if (expiry === 0) {
    return 0;
  }
  if (expiry <= 60*60*24*30) {
    return (utils.unixTimestamp()/1000) + expiry;
  }
  return expiry;
};

MemdService.prototype._lockTimeToTs = function(lockTime) {
  if (lockTime <= 0) {
    lockTime = 15;
  } else if (lockTime > 30) {
    lockTime = 30;
  }
  return (utils.unixTimestamp()/1000) + lockTime;
};

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
    if (e instanceof MemdStatusError) {
      //console.warn('memd handler error', e);
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

MemdService.prototype._handleStoreC = function(socket, req, mustExist) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);

  if (mustExist === true && !kVal) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  } else if (mustExist === false && kVal) {
    throw new MemdStatusError(memdproto.status.KEY_EXISTS);
  }

  if (kVal && !Cas.compare(kVal.cas, req.cas)) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  } else if (!kVal && !Cas.compare(null, req.cas)) {
    throw new MemdStatusError(memdproto.status.KEY_EXISTS);
  }

  // We already ensure that the CAS matches if it is specified, so now
  //   we simply need to make sure it was specified.
  if (kVal && kVal.lockTime && Cas.compare(null, req.cas)) {
    // TODO: Probably the wrong error again...
    throw new MemdStatusError(memdproto.status.ETMPFAIL);
  }

  kVal = {
    cas: new Cas(),
    dataType: req.dataType,
    flags: req.flags,
    value: req.value,
    expiry: this._expiryToTs(req.expiry)
  };
  bucket.setKeyData(kRef, kVal);

  socket.writeSCResp(req,
    memdproto.status.SUCCESS,
    kVal.cas
  );
};
MemdService.prototype._handleSet = function(socket, req) {
  return this._handleStoreC(socket, req, undefined);
};
MemdService.prototype._handleSetQ = function(socket, req) {
  req.quiet = true;
  return this._handleStoreC(socket, req, undefined);
};
MemdService.prototype._handleAdd = function(socket, req) {
  return this._handleStoreC(socket, req, false);
};
MemdService.prototype._handleAddQ = function(socket, req) {
  req.quiet = true;
  return this._handleStoreC(socket, req, false);
};
MemdService.prototype._handleReplace = function(socket, req) {
  return this._handleStoreC(socket, req, true);
};
MemdService.prototype._handleReplaceQ = function(socket, req) {
  req.quiet = true;
  return this._handleStoreC(socket, req, true);
};

MemdService.prototype._handleConcatC = function(socket, req, prepend) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);

  if (!kVal) {
    throw new MemdStatusError(memdproto.status.NOT_STORED);
  } else if (kVal && !Cas.compare(kVal.cas, req.cas)) {
    throw new MemdStatusError(memdproto.status.KEY_EXISTS);
  }

  if (!prepend) {
    kVal.value = Buffer.concat([kVal.value, req.value]);
  } else {
    kVal.value = Buffer.concat([req.value, kVal.value]);
  }
  kVal.cas = new Cas();

  bucket.setKeyData(kRef, kVal);

  socket.writeSCResp(req,
    memdproto.status.SUCCESS,
    kVal.cas
  );
};
MemdService.prototype._handleAppend = function(socket, req) {
  this._handleConcatC(socket, req, false);
};
MemdService.prototype._handleAppendQ = function(socket, req) {
  req.quiet = true;
  this._handleConcatC(socket, req, false);
};
MemdService.prototype._handlePrepend = function(socket, req) {
  this._handleConcatC(socket, req, true);
};
MemdService.prototype._handlePrependQ = function(socket, req) {
  req.quiet = true;
  this._handleConcatC(socket, req, true);
};

MemdService.prototype._handleGet = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, req.key);
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
MemdService.prototype._handleGetQ = function(socket, req) {
  req.quiet = true;
  return this._handleGet(socket, req);
};

MemdService.prototype._handleRemove = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);

  // TODO: Confirm this logic.
  if (!kVal) {
    throw new MemdStatusError(memdproto.status.KEY_EXISTS);
  } else if (kVal && !Cas.compare(kVal.cas, req.cas)) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  }

  var newVal = bucket.removeKeyData(kRef);
  if (!newVal) {
    // Should exist since we check above...
    throw new MemdStatusError(memdproto.status.EINTERNAL);
  }

  socket.writeSCResp(req,
    memdproto.status.SUCCESS,
    newVal.cas
  );
};
MemdService.prototype._handleRemoveQ = function(socket, req) {
  req.quiet = true;
  return this._handleRemove(socket, req);
};

MemdService.prototype._handleTouch = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);

  if (!kVal) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  }

  // TODO: Its possible this is supposed to change the CAS.
  kVal.expiry = this._expiryToTs(req.expiry);

  // TODO: Not sure if this is the right response
  socket.writeSCResp(req,
    memdproto.status.SUCCESS,
    kVal.cas
  );
};

MemdService.prototype._handleGetAndTouch = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);

  if (!kVal) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  }

  // TODO: Its possible this is supposed to change the CAS.
  kVal.expiry = this._expiryToTs(req.expiry);

  // TODO: Not sure if this is the right response.
  socket.writeGetResp(req,
    memdproto.status.SUCCESS,
    kVal.cas,
    kVal.dataType,
    kVal.flags,
    kVal.value
  );
};
MemdService.prototype._handleGetAndTouchQ = function(socket, req) {
  req.quiet = true;
  return this._handleGetAndTouch(socket, req);
};

MemdService.prototype._handleArithmetic = function(socket, req, decr) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);

  if (!kVal && req.expiry === 0xFFFFFFFF) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  }

  var lVal = Long.fromInt(0);
  if (!kVal) {
    lVal = req.initial;
    kVal = {
      cas: new Cas(),
      dataType: 0,
      flags: 0,
      value: new Buffer(lVal.toString(10), 'utf8'),
      expiry: this._expiryToTs(req.expiry)
    };
  } else {
    var strVal = kVal.value.toString('utf8');
    lVal = Long.fromString(strVal, true, 10);

    if (!decr) {
      lVal = lVal.add(req.delta);
    } else {
      if (lVal.greaterThan(req.delta)) {
        lVal = lVal.subtract(req.delta);
      } else {
        lVal = Long.fromInt(0);
      }
    }

    kVal.value = new Buffer(lVal.toString(10), 'utf8');
  }

  bucket.setKeyData(kRef, kVal);

  socket.writeArithmeticResp(req,
    memdproto.status.SUCCESS,
    kVal.cas,
    lVal
  );
};
MemdService.prototype._handleIncr = function(socket, req) {
  return this._handleArithmetic(socket, req, false);
};
MemdService.prototype._handleIncrQ = function(socket, req) {
  req.quiet = true;
  return this._handleArithmetic(socket, req, false);
};
MemdService.prototype._handleDecr = function(socket, req) {
  return this._handleArithmetic(socket, req, true);
};
MemdService.prototype._handleDecrQ = function(socket, req) {
  req.quiet = true;
  return this._handleArithmetic(socket, req, true);
};

MemdService.prototype._handleGetLocked = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);

  if (!kVal) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  } else if (kVal.lockTime) {
    throw new MemdStatusError(memdproto.status.ETMPFAIL);
  } else if (!Cas.compare(kVal.cas, req.cas)) {
    throw new MemdStatusError(memdproto.status.KEY_EXISTS);
  }

  kVal.lockTime = this._lockTimeToTs(req.lockTime);

  socket.writeGetResp(req,
    memdproto.status.SUCCESS,
    kVal.cas,
    kVal.dataType,
    kVal.flags,
    kVal.value
  );
};

MemdService.prototype._handleUnlock = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);

  // TODO: These are likely the wrong errors
  if (!kVal) {
    throw new MemdStatusError(memdproto.status.KEY_ENOENT);
  } else if (Cas.compare(null, req.cas)) {
    throw new MemdStatusError(memdproto.status.KEY_EXISTS);
  } else if (!Cas.compare(kVal.cas, req.cas)) {
    throw new MemdStatusError(memdproto.status.KEY_EXISTS);
  }

  // Not sure if this is supposed to change the CAS
  delete kVal.lockTime;
  kVal.cas = new Cas();

  socket.writeSCResp(req,
    memdproto.status.SUCCESS,
    kVal.cas
  );
};

MemdService.prototype._handleObserve = function(socket, req) {
  var bucket = this._getSocketBucket(socket);

  var idata = req.value;
  var odata = new Buffer(0);
  var off = 0;
  while (off < idata.length) {
    // Parse out one item from the packet
    if (off + 4 > idata.length) {
      throw new MemdStatusError(memdproto.status.EINVAL);
    }
    var vbId = idata.readUInt16BE(off+0);
    var nkey = idata.readUInt16BE(off+2);

    if (off + 4 + nkey > idata.length ) {
      throw new MemdStatusError(memdproto.status.EINVAL);
    }
    var key = idata.slice(off + 4, off + 4 + nkey);

    // Process this key
    var status = memdproto.obsstate.NOT_FOUND;
    var cas = null;

    var repId = bucket.nodeRepId(vbId, this.parentNode.nodeId);
    if (repId !== -1) {
      var kRef = bucket.getKeyRef(vbId, key);
      var kVal = bucket._getKeyDataEx(kRef, 1 + repId);

      if (kVal) {
        cas = kVal.cas;
        if (kVal.deleted) {
          if (kVal.persisted) {
            status = memdproto.obsstate.NOT_FOUND;
          } else {
            status = memdproto.obsstate.LOGICAL_DEL;
          }
        } else {
          if (kVal.persisted) {
            status = memdproto.obsstate.PERSISTED;
          } else {
            status = memdproto.obsstate.NOT_PERSISTED;
          }
        }
      }
    }

    var thisbuf = new Buffer(4 + nkey + 9);
    idata.copy(thisbuf, 0, off, off+4+nkey);
    thisbuf.writeUInt8(status, 4+nkey+0);
    Cas.writeToBuffer(thisbuf, 4+nkey+1, cas);
    odata = Buffer.concat([odata, thisbuf]);

    off += 4 + nkey;
  }

  console.log(idata);
  console.log(odata);

  socket.writeObserveResp(req,
    memdproto.status.SUCCESS,
    1000,
    1000,
    odata
  );
};


module.exports = MemdService;
