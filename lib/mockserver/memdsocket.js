"use strict";

var util = require('util');
var net = require('net');
var memdproto = require('./memdproto');

var EventEmitter = require('events').EventEmitter;
var Long = require('long');
var Cas = require('./cas');


function MemdProtocolError(msg, data) {
  Error.call(this);
  Error.captureStackTrace(this, MemdProtocolError);
  this.message = msg;
  this.data = data;
  this.name = 'MemdProtocolError';
};
util.inherits(MemdProtocolError, Error);


/*
This double constructor shenanigans is neccessary to allow 'late inherits'
for sockets created via a net.Server.
 */
function MemdSocket$() {
  this.dataBuf = null;
  this.on('data', this._handleData);
  this.on('error', this._handleError);
}
function MemdSocket(options) {
  net.Socket.call(this, options);
  MemdSocket$.call(this);
}
util.inherits(MemdSocket, net.Socket);


MemdSocket.upgradeSocket = function(socket) {
  // Copy prototypes
  for (var i in MemdSocket.prototype) {
    if (MemdSocket.prototype.hasOwnProperty(i)) {
      socket[i] = MemdSocket.prototype[i];
    }
  }

  // Call constructor
  MemdSocket$.call(socket);

  return socket;
};

MemdSocket.prototype._handleError = function(e) {
  console.warn('MEMD SOCK ERROR', e);
};

function memdPacket(magic, op, seqNo, vbIdOrStatus, dataType, cas, extLen, key, value) {
  var keyLen = key ? key.length : 0;
  var valueLen = value ? value.length : 0;

  var buf = new Buffer(24 + extLen + keyLen + valueLen);

  buf.fastWrite(0, magic, 1);
  buf.fastWrite(1, op, 1);
  buf.fastWrite(2, keyLen, 2);
  buf.fastWrite(4, extLen, 1);
  buf.fastWrite(5, dataType, 1);
  buf.fastWrite(6, vbIdOrStatus, 2);
  buf.fastWrite(8, extLen + keyLen + valueLen, 4);
  buf.fastWrite(12, seqNo, 4);

  Cas.writeToBuffer(buf, 16, cas);

  if (keyLen > 0) {
    buf.write(key, 24+extLen);
  }
  if (valueLen > 0) {
    value.copy(buf, 24+extLen+keyLen);
  }

  return buf;
}

function memdReqPacket(op, seqNo, status, dataType, cas, extLen, key, value) {
  return memdPacket(memdproto.magic.REQUEST,
    op, seqNo, status, dataType, cas, extLen, key, value);
}

function memdRespPacket(op, seqNo, status, dataType, cas, extLen, key, value) {
  return memdPacket(memdproto.magic.RESPONSE,
      op, seqNo, status, dataType, cas, extLen, key, value);
}

MemdSocket.prototype.writeSVResp = function(req, status, value) {
  var buf = memdRespPacket(
    req.op,
    req.seqNo,
    status,
    0,
    null,
    0,
    null,
    value
  );
  this.write(buf);
};
MemdSocket.prototype.writeSCResp = function(req, status, cas) {
  var buf = memdRespPacket(
    req.op,
    req.seqNo,
    status,
    0,
    cas,
    0,
    null,
    null
  );
  this.write(buf);
};
MemdSocket.prototype.writeSResp = function(req, status) {
  var buf = memdRespPacket(
    req.op,
    req.seqNo,
    status,
    0,
    null,
    0,
    null,
    null
  );
  this.write(buf);
};

MemdSocket.prototype.writeGetResp =
    function(req, status, cas, dataType, flags, value) {
  var buf = memdRespPacket(
    req.op,
    req.seqNo,
    status,
    dataType,
    cas,
    4,
    null,
    value
  );
  buf.fastWrite(24, flags, 4);
  this.write(buf);
};

MemdSocket.prototype.writeObserveResp =
  function(req, status, ttr, ttp, value) {
    var fakeCas = new Cas();
    fakeCas.hi = ttp;
    fakeCas.lo = ttr;

    var buf = memdRespPacket(
      req.op,
      req.seqNo,
      status,
      0,
      fakeCas,
      0,
      null,
      value
    );
    this.write(buf);
  };

MemdSocket.prototype.writeArithmeticResp = function(req, status, cas, value) {
  var valueBuf = new Buffer(8);
  valueBuf.fastWrite(4, value.getLowBitsUnsigned(), 4);
  valueBuf.fastWrite(0, value.getHighBitsUnsigned(), 4);

  var buf = memdRespPacket(
    req.op,
    req.seqNo,
    status,
    0,
    cas,
    0,
    null,
    valueBuf
  );
  this.write(buf);
};


MemdSocket.prototype.writeErrorResp = function(req, error) {
  this.writeSResp(req, error);
};

MemdSocket.prototype.writeInvalidArgsResp = function(req) {
  this.writeSResp(req, memdproto.status.EINVAL);
};


MemdSocket.prototype.writeSaslAuthSuccessResp = function(req, statusStr) {
  this.writeSVResp(req,
    memdproto.status.SUCCESS,
    new Buffer(statusStr, 'utf8')
  );
};



function parseMemdPacket(data) {
  var packet = {};

  packet.magic = data.readUInt8(0);
  if (packet.magic !== memdproto.magic.REQUEST &&
      packet.magic !== memdproto.magic.RESPONSE) {
    throw new MemdProtocolError('unknown magic', data);
  }
  packet.op = data.readUInt8(1);
  var keyLen = data.readUInt16BE(2);
  var extLen = data.readUInt8(4);
  packet.dataType = data.readUInt8(5);
  if (packet.magic === memdproto.magic.REQUEST) {
    packet.vbId = data.readUInt16BE(6);
  } else if (packet.magic === memdproto.magic.RESPONSE) {
    packet.status = data.readUInt16BE(6);
  }
  packet.seqNo = data.readUInt32BE(12);

  packet.cas = Cas.readFromBuffer(data, 16);

  packet.key = null;
  if (keyLen > 0) {
    packet.key = data.slice(24+extLen, 24+extLen+keyLen);
  }

  packet.value = null;
  if (data.length > 24+extLen+keyLen) {
    packet.value = data.slice(24+extLen+keyLen);
  }

  function throwUnexpectedPacket() {
    throw new MemdProtocolError('unexpected packet or extras', data);
  }

  if (packet.magic === memdproto.magic.REQUEST) {
    if (packet.op === memdproto.cmd.SET ||
        packet.op === memdproto.cmd.SETQ ||
        packet.op === memdproto.cmd.ADD ||
        packet.op === memdproto.cmd.ADDQ ||
        packet.op === memdproto.cmd.REPLACE ||
        packet.op === memdproto.cmd.REPLACEQ) {
      if (extLen === 8) {
        packet.flags = data.readUInt32BE(24);
        packet.expiry = data.readUInt32BE(28);
      } else {
        throwUnexpectedPacket();
      }
    } else if (packet.op === memdproto.cmd.INCREMENT ||
        packet.op === memdproto.cmd.INCREMENTQ ||
        packet.op === memdproto.cmd.DECREMENT ||
        packet.op === memdproto.cmd.DECREMENTQ) {
      if (extLen === 20) {
        packet.delta = Long.fromBits(
          data.readUInt32BE(28),
          data.readUInt32BE(24)
        );
        packet.initial = Long.fromBits(
          data.readUInt32BE(36),
          data.readUInt32BE(32)
        );
        packet.expiry = data.readUInt32BE(40);
      } else {
        throwUnexpectedPacket();
      }
    } else if (packet.op === memdproto.cmd.VERBOSITY) {
      if (extLen === 4) {
        packet.verbosity = data.readUInt32BE(24);
      } else {
        throwUnexpectedPacket();
      }
    } else if (packet.op === memdproto.cmd.TOUCH ||
        packet.op === memdproto.cmd.GAT) {
      if (extLen === 4) {
        packet.expiry = data.readUInt32BE(24);
      } else {
        throwUnexpectedPacket();
      }
    } else if (packet.op === memdproto.cmd.GET_LOCKED) {
      if (extLen === 4) {
        packet.lockTime = data.readUInt32BE(24);
      } else {
        throwUnexpectedPacket();
      }
    } else if (packet.op === memdproto.cmd.GET ||
        packet.op === memdproto.cmd.GETQ ||
        packet.op === memdproto.cmd.GETK ||
        packet.op === memdproto.cmd.GETKQ ||
        packet.op === memdproto.cmd.DELETE ||
        packet.op === memdproto.cmd.DELETEQ ||
        packet.op === memdproto.cmd.QUIT ||
        packet.op === memdproto.cmd.QUITQ ||
        packet.op === memdproto.cmd.FLUSH ||
        packet.op === memdproto.cmd.FLUSHQ ||
        packet.op === memdproto.cmd.NOOP ||
        packet.op === memdproto.cmd.VERSION ||
        packet.op === memdproto.cmd.APPEND ||
        packet.op === memdproto.cmd.APPENDQ ||
        packet.op === memdproto.cmd.PREPEND ||
        packet.op === memdproto.cmd.PREPENDQ ||
        packet.op === memdproto.cmd.STAT ||
        packet.op === memdproto.cmd.SASL_LIST_MECHS ||
        packet.op === memdproto.cmd.SASL_AUTH ||
        packet.op === memdproto.cmd.SASL_STEP ||
        packet.op === memdproto.cmd.GET_CLUSTER_CONFIG ||
        packet.op === memdproto.cmd.UNLOCK_KEY ||
        packet.op === memdproto.cmd.OBSERVE) {
      if (extLen !== 0) {
        throwUnexpectedPacket();
      }
    } else {
      throwUnexpectedPacket();
    }
  } else if (packet.magic === memdproto.magic.RESPONSE) {
    if (packet.op === memdproto.cmd.GET) {
      if (extLen === 4) {
        packet.flags = data.readUInt32BE(24);
      } else {
        throwUnexpectedPacket();
      }
    } else {
      if (extLen !== 0) {
        throwUnexpectedPacket();
      }
    }
  } else {
    throwUnexpectedPacket();
  }

  return packet;
}

MemdSocket.prototype._handleData = function(data) {
  if (this.dataBuf === null) {
    this.dataBuf = data;
  } else {
    var totalLen = this.dataBuf.length + data.length;
    this.dataBuf = Buffer.concat([this.dataBuf, data], totalLen);
  }

  var offset = 0;
  while (offset < this.dataBuf.length) {
    if (this.dataBuf.length >= offset + 24) {
      var bodyLen = this.dataBuf.readUInt32BE(offset+8);
      var packetLen = 24 + bodyLen;
      if (this.dataBuf.length >= offset + packetLen) {
        var packetData = this.dataBuf.slice(offset, offset + packetLen);
        try {
          var packet = parseMemdPacket(packetData);
          this.emit('packet', packet);
        } catch (e) {
          console.warn('memd packet error', e);
          console.warn(e.stack);
        }

        offset += packetLen;
      } else {
        break;
      }
    } else {
      break;
    }
  }

  if (offset === this.dataBuf.length) {
    this.dataBuf = null;
  } else {
    this.dataBuf = this.dataBuf.slice(offset);
  }
};

module.exports = MemdSocket;
