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

MemdSocket.prototype.writePacket = function(pak) {
  var extLen = pak.extras ? pak.extras.length : 0;
  var keyLen = pak.key ? pak.key.length : 0;
  var valueLen = pak.value ? pak.value.length : 0;

  var buf = new Buffer(24 + extLen + keyLen + valueLen);

  buf.fastWrite(0, pak.magic, 1);
  buf.fastWrite(1, pak.op, 1);
  buf.fastWrite(2, keyLen, 2);
  buf.fastWrite(4, extLen, 1);
  buf.fastWrite(5, pak.dataType, 1);
  buf.fastWrite(6, pak.status, 2);
  buf.fastWrite(8, extLen + keyLen + valueLen, 4);
  buf.fastWrite(12, pak.seqNo, 4);

  Cas.writeToBuffer(buf, 16, pak.cas);

  if (extLen) {
    pak.extras.copy(buf, 24);
  }
  if (keyLen > 0) {
    pak.key.copy(buf, 24+extLen);
  }
  if (valueLen > 0) {
    pak.value.copy(buf, 24+extLen+keyLen);
  }

  this.write(buf);
};

MemdSocket.prototype.writeReply = function(req, resp) {
  if (!resp.magic) {
    resp.magic = memdproto.magic.RESPONSE;
  }
  if (!resp.seqNo) {
    resp.seqNo = req.seqNo;
  }
  if (!resp.op) {
    resp.op = req.op;
  }
  if (!resp.status) {
    resp.status = memdproto.status.SUCCESS;
  }

  this.writePacket(resp);
};

MemdSocket.prototype._handleError = function(e) {
  console.warn('MEMD SOCK ERROR', e);
};

/**
 * @constructor
 *
 * @property {number} magic
 */
function MemdPacket() {
}

MemdPacket.prototype.keyLength = function() {
  if (!this.key) {
    return 0;
  }
  return this.key.length;
};

MemdPacket.prototype.extrasLength = function() {
  if (!this.extras) {
    return 0;
  }
  return this.extras.length;
};

MemdPacket.prototype.valueLength = function() {
  if (!this.value) {
    return 0;
  }
  return this.value.length;
};

MemdPacket.prototype.isRequest = function(cmd) {
  if (this.magic === memdproto.magic.REQUEST && this.op === cmd) {
    return true;
  }
  return false;
};

function parseMemdPacket(data) {
  var packet = new MemdPacket();

  packet.magic = data.readUInt8(0);
  packet.op = data.readUInt8(1);
  packet.dataType = data.readUInt8(5);
  packet.seqNo = data.readUInt32BE(12);
  packet.cas = Cas.readFromBuffer(data, 16);

  if (packet.magic === memdproto.magic.REQUEST) {
    packet.vbId = data.readUInt16BE(6);
  } else if (packet.magic === memdproto.magic.RESPONSE) {
    packet.status = data.readUInt16BE(6);
  }

  var extLen = data.readUInt8(4);
  packet.extras = null;
  if (extLen > 0) {
    packet.extras = data.slice(24, 24+extLen);
  }

  var keyLen = data.readUInt16BE(2);
  packet.key = null;
  if (keyLen > 0) {
    packet.key = data.slice(24+extLen, 24+extLen+keyLen);
  }

  packet.value = null;
  if (data.length > 24+extLen+keyLen) {
    packet.value = data.slice(24+extLen+keyLen);
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
