'use strict';

var crypto = require('crypto');
var memdproto = require('../memdproto');
var MemdCmdModule = require('../memdcmdmodule');

var mod = new MemdCmdModule();

/******************************************************************************
 * PLAIN Mech
 ******************************************************************************/
var _saslPlainInit = function(socket, req) {
  if (req.valueLength() === 0) {
    return socket.writeReply(req, { status: memdproto.status.AUTH_ERROR });
  }

  var parts = req.value.split(0x00);
  if (parts.length !== 3) {
    return socket.writeReply(req, { status: memdproto.status.AUTH_ERROR });
  }

  // TODO: Implement proper authentication stufff
  //var authzid = parts[0].toString('utf8');
  var authcid = parts[1].toString('utf8');
  var passwd = parts[2].toString('utf8');

  var bucket = this.parent.bucketByName(authcid);
  if (!bucket) {
    return socket.writeReply(req, { status: memdproto.status.AUTH_ERROR });
  }

  if (passwd !== bucket.password) {
    return socket.writeReply(req, { status: memdproto.status.AUTH_ERROR });
  }

  socket.bucket = bucket;

  socket.writeReply(req, {
    value: new Buffer('success', 'utf8')
  });
};

/******************************************************************************
 * SASL Mech
 ******************************************************************************/
function _generateChallenge() {
  return '<' + crypto.randomBytes(8).toString('hex') + '.0@127.0.0.1>';
}
function _generateHash(challenge, password) {
  return crypto.createHmac('md5', password).update(challenge).digest('hex');
}
var _saslCramMd5Init = function(socket, req) {
  socket.saslChallenge = _generateChallenge();

  socket.writeReply(req, {
    status: memdproto.status.AUTH_CONTINUE,
    value: new Buffer(socket.saslChallenge, 'utf8')
  });
};
var _saslCramMd5Step = function(socket, req) {
  if (req.valueLength() === 0) {
    return socket.writeReply(req, { status: memdproto.status.AUTH_ERROR });
  }

  var auth = req.value.split(0x20);
  var authusr = auth[0].toString('utf8');
  var authpas = auth[1].toString('utf8');

  var bucket = this.parent.bucketByName(authusr);
  if (!bucket) {
    return socket.writeReply(req, { status: memdproto.status.AUTH_ERROR });
  }

  var bucketHash = _generateHash(socket.saslChallenge, bucket.password);
  if (authpas !== bucketHash) {
    return socket.writeReply(req, { status: memdproto.status.AUTH_ERROR });
  }

  socket.bucket = bucket;

  socket.writeReply(req, {
    value: new Buffer('success', 'utf8')
  });
};

/******************************************************************************
 * Mech Dispatching Handlers
 ******************************************************************************/
mod.reqOp(memdproto.cmd.SASL_LIST_MECHS, function(socket, req) {
  socket.writeReply(req, {
    value: new Buffer('PLAIN CRAM-MD5')
  });
});

mod.reqOp(memdproto.cmd.SASL_AUTH, function(socket, req) {
  var authType = req.keyLength() > 0 ? req.key.toString('utf8') : null;
  if (authType === 'PLAIN') {
    return _saslPlainInit.call(this, socket, req);
  } else if (authType === 'CRAM-MD5') {
    return _saslCramMd5Init.call(this, socket, req);
  } else {
    return socket.writeReply(req, { status: memdproto.status.AUTH_ERROR });
  }
});

mod.reqOp(memdproto.cmd.SASL_STEP, function(socket, req) {
  var authType = req.keyLength() > 0 ? req.key.toString('utf8') : null;
  if (authType === 'CRAM-MD5') {
    return _saslCramMd5Step.call(this, socket, req);
  } else {
    return socket.writeReply(req, { status: memdproto.status.AUTH_ERROR });
  }
});

module.exports = mod;
