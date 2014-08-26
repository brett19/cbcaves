'use strict';

var memdproto = require('../memdproto');
var Cas = require('../cas');
var MemdCmdModule = require('../memdcmdmodule');

var mod = new MemdCmdModule();

var _hlpStore = function(socket, req, mustExist, quiet) {
  if (req.extrasLength() !== 8) {
    return socket.writeReply(req, { status: memdproto.status.EINVAL });
  }
  req.flags = req.extras.readUInt32BE(0);
  req.expiry = req.extras.readUInt32BE(4);

  var bucket = this._getSocketBucket(socket);
  if (!bucket) {
    return socket.writeReply(req, { status: memdproto.status.ETMPFAIL });
  }

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);
  if (mustExist === true && !kVal) {
    return socket.writeReply(req, { status: memdproto.status.KEY_ENOENT });
  } else if (mustExist === false && kVal) {
    return socket.writeReply(req, { status: memdproto.status.KEY_EXISTS });
  }

  if (kVal && !Cas.compare(kVal.cas, req.cas)) {
    return socket.writeReply(req, { status: memdproto.status.KEY_ENOENT });
  } else if (!kVal && !Cas.compare(null, req.cas)) {
    return socket.writeReply(req, { status: memdproto.status.KEY_EXISTS });
  }

  // We already ensure that the CAS matches if it is specified, so now
  //   we simply need to make sure it was specified.
  if (kVal && kVal.lockTime && Cas.compare(null, req.cas)) {
    // TODO: Probably the wrong error again...
    return socket.writeReply(req, { status: memdproto.status.ETMPFAIL });
  }

  kVal = {
    cas: new Cas(),
    dataType: req.dataType,
    flags: req.flags,
    value: req.value,
    expiry: memdproto.expiryToTs(this.parent.clock, req.expiry)
  };
  bucket.setKeyData(kRef, kVal);

  if (!quiet) {
    socket.writeReply(req, {
      cas: kVal.cas
    });
  }
};
mod.reqOp(memdproto.cmd.SET, function(socket, req) {
  return _hlpStore.call(this, socket, req, undefined, false);
});
mod.reqOp(memdproto.cmd.SETQ, function(socket, req) {
  return _hlpStore.call(this, socket, req, undefined, true);
});
mod.reqOp(memdproto.cmd.REPLACE, function(socket, req) {
  return _hlpStore.call(this, socket, req, true, false);
});
mod.reqOp(memdproto.cmd.REPLACEQ, function(socket, req) {
  return _hlpStore.call(this, socket, req, true, true);
});
mod.reqOp(memdproto.cmd.ADD, function(socket, req) {
  return _hlpStore.call(this, socket, req, false, false);
});
mod.reqOp(memdproto.cmd.ADDQ, function(socket, req) {
  return _hlpStore.call(this, socket, req, false, true);
});

var _hlpConcat = function(socket, req, prepend, quiet) {
  if (req.extrasLength() !== 0) {
    return socket.writeReply(req, { status: memdproto.status.EINVAL });
  }

  var bucket = this._getSocketBucket(socket);
  if (!bucket) {
    return socket.writeReply(req, { status: memdproto.status.ETMPFAIL });
  }

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);
  if (!kVal) {
    return socket.writeReply(req, { status: memdproto.status.NOT_STORED });
  } else if (kVal && !Cas.compare(kVal.cas, req.cas)) {
    return socket.writeReply(req, { status: memdproto.status.KEY_EXISTS });
  }

  if (!prepend) {
    kVal.value = Buffer.concat([kVal.value, req.value]);
  } else {
    kVal.value = Buffer.concat([req.value, kVal.value]);
  }
  kVal.cas = new Cas();
  bucket.setKeyData(kRef, kVal);

  if (!quiet) {
    socket.writeReply(req, {
      cas: kVal.cas
    });
  }
};
mod.reqOp(memdproto.cmd.APPEND, function(socket, req) {
  return _hlpConcat.call(this, socket, req, true, false);
});
mod.reqOp(memdproto.cmd.APPENDQ, function(socket, req) {
  return _hlpConcat.call(this, socket, req, true, true);
});
mod.reqOp(memdproto.cmd.PREPEND, function(socket, req) {
  return _hlpConcat.call(this, socket, req, false, false);
});
mod.reqOp(memdproto.cmd.PREPENDQ, function(socket, req) {
  return _hlpConcat.call(this, socket, req, false, true);
});

var _hlpGet = function(socket, req, quiet) {
  if (req.extrasLength() !== 0) {
    return socket.writeReply(req, { status: memdproto.status.EINVAL });
  }

  var bucket = this._getSocketBucket(socket);
  if (!bucket) {
    return socket.writeReply(req, { status: memdproto.status.ETMPFAIL });
  }

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);
  if (!kVal) {
    return socket.writeReply(req, { status: memdproto.status.KEY_ENOENT });
  }

  if (!quiet) {
    var extras = new Buffer(4);
    extras.fastWrite(0, kVal.flags, 4);
    socket.writeReply(req, {
      cas: kVal.cas,
      dataType: kVal.dataType,
      extras: extras,
      value: kVal.value
    });
  }
};
mod.reqOp(memdproto.cmd.GET, function(socket, req) {
  return _hlpGet.call(this, socket, req, false);
});
mod.reqOp(memdproto.cmd.GETQ, function(socket, req) {
  return _hlpGet.call(this, socket, req, true);
});

var _hlpRemove = function(socket, req, quiet) {
  if (req.extrasLength() !== 0) {
    return socket.writeReply(req, { status: memdproto.status.EINVAL });
  }

  var bucket = this._getSocketBucket(socket);
  if (!bucket) {
    return socket.writeReply(req, { status: memdproto.status.ETMPFAIL });
  }

  var kRef = bucket.getKeyRef(req.vbId, req.key);
  var kVal = bucket.getKeyData(kRef);
  if (!kVal) {
    return socket.writeReply(req, { status: memdproto.status.KEY_ENOENT });
  } else if (kVal && !Cas.compare(kVal.cas, req.cas)) {
    return socket.writeReply(req, { status: memdproto.status.KEY_EXISTS });
  }

  var newVal = bucket.removeKeyData(kRef);
  if (!newVal) {
    // Should exist since we check above...
    return socket.writeReply(req, { status: memdproto.status.EINTERNAL });
  }

  if (!quiet) {
    socket.writeReply(req, {
      cas: newVal.cas
    });
  }
};
mod.reqOp(memdproto.cmd.DELETE, function(socket, req) {
  return _hlpRemove.call(this, socket, req, false);
});
mod.reqOp(memdproto.cmd.DELETEQ, function(socket, req) {
  return _hlpRemove.call(this, socket, req, true);
});

module.exports = mod;
