"use strict";

var util = require('util');

var EventEmitter = require('events').EventEmitter;
var Long = require('long');

function bufferFastWrite(buf, off, val, bytes) {
  for (var i = 0; i < bytes; ++i) {
    buf[off+bytes-i-1] = val >> (i * 8);
  }
}

function bufferFastRead64(buf, off) {
  return (new Long(
    buf.readUInt32BE(off+4),
    buf.readUInt32BE(off+0)
  )).toNumber();
}
function bufferFastWrite64(buf, off, val) {
  var lval = Long.fromNumber(val);
  bufferFastWrite(buf, off+4, lval.getLowBitsUnsigned(), 4);
  bufferFastWrite(buf, off+0, lval.getHighBitsUnsigned(), 4);
}

function memdRespPacket(op, statusCode, dataType, seqNo, cas, extLen, key, value) {
  var keyLength = 0;
  if (key) {
    keyLength = Buffer.byteLength(key);
  }

  var valueLength = 0;
  if (value) {
    valueLength = value.length;
  }

  var buf = new Buffer(24 + extLen + keyLength + valueLength);

  bufferFastWrite(buf, 0, MEMCACHED_RESPONSE_MAGIC, 1);
  bufferFastWrite(buf, 1, op, 1);
  bufferFastWrite(buf, 2, keyLength, 2);
  bufferFastWrite(buf, 4, extLen, 1);
  bufferFastWrite(buf, 5, dataType, 1);
  bufferFastWrite(buf, 6, statusCode, 2);
  bufferFastWrite(buf, 8, extLen+keyLength+valueLength, 4);
  bufferFastWrite(buf, 12, seqNo, 4);

  if (cas && Array.isArray(cas) && cas.length === 2) {
    bufferFastWrite(buf, 16, cas[0], 4);
    bufferFastWrite(buf, 20, cas[1], 4);
  } else {
    bufferFastWrite(buf, 16, 0, 4);
    bufferFastWrite(buf, 20, 0, 4);
  }

  if (keyLength > 0) {
    buf.write(key, 24+extLen, keyLength);
  }
  if (valueLength > 0) {
    value.copy(buf, 24+extLen+keyLength);
  }

  return buf;
}

function bufferSplit(buf, splitByte) {
  var beginPos = 0;
  var out = [];
  for (var i = 0; i < buf.length; ++i) {
    if (buf[i] === splitByte) {
      out.push(buf.slice(beginPos, i));
      beginPos = i + 1;
    }
  }
  out.push(buf.slice(beginPos));
  return out;
}

var MEMCACHED_REQUEST_MAGIC = 0x80;
var MEMCACHED_RESPONSE_MAGIC = 0x81;

var MEMCACHED_CMD = {
  GET: 0x00,
  SET: 0x01,
  ADD: 0x02,
  REPLACE: 0x03,
  DELETE: 0x04,
  INCREMENT: 0x05,
  DECREMENT: 0x06,
  QUIT: 0x07,
  FLUSH: 0x08,
  GETQ: 0x09,
  NOOP: 0x0a,
  VERSION: 0x0b,
  GETK: 0x0c,
  GETKQ: 0x0d,
  APPEND: 0x0e,
  PREPEND: 0x0f,
  STAT: 0x10,
  SETQ: 0x11,
  ADDQ: 0x12,
  REPLACEQ: 0x13,
  DELETEQ: 0x14,
  INCREMENTQ: 0x15,
  DECREMENTQ: 0x16,
  QUITQ: 0x17,
  FLUSHQ: 0x18,
  APPENDQ: 0x19,
  PREPENDQ: 0x1a,
  VERBOSITY: 0x1b,
  TOUCH: 0x1c,
  GAT: 0x1d,
  GATQ: 0x1e,

  GET_REPLICA: 0x83,

  SASL_LIST_MECHS: 0x20,
  SASL_AUTH: 0x21,
  SASL_STEP: 0x22,

  UPR_OPEN: 0x50,
  UPR_ADD_STREAM: 0x51,
  UPR_CLOSE_STREAM: 0x52,
  UPR_STREAM_REQ: 0x53,
  UPR_FAILOVER_LOG_REQ: 0x54,
  UPR_SNAPSHOT_MARKER: 0x56,
  UPR_MUTATION: 0x57,
  UPR_DELETION: 0x58,
  UPR_EXPIRATION: 0x59,
  UPR_FLUSH: 0x5a,
  UPR_SET_VBUCKET_STATE: 0x5b,

  OBSERVE: 0x92,
  EVICT_KEY: 0x93,
  GET_LOCKED: 0x94,
  UNLOCK_KEY: 0x95,

  GET_CLUSTER_CONFIG: 0xb5
};

var MEMCACHED_STATUS = {
  SUCCESS: 0x00,
  KEY_ENOENT: 0x01,
  KEY_EXISTS: 0x02,
  E2BIG: 0x03,
  EINVAL: 0x04,
  NOT_STORED: 0x05,
  DELTA_BADVAL: 0x06,
  NOT_MY_VBUCKET: 0x07,
  AUTH_ERROR: 0x20,
  AUTH_CONTINUE: 0x21,
  ERANGE: 0x22,
  UNKNOWN_COMMAND: 0x81,
  ENOMEM: 0x82,
  NOT_SUPPORTED: 0x83,
  EINTERNAL: 0x84,
  EBUSY: 0x85,
  ETMPFAIL: 0x86
};

function MemdClient(parent, parentNode, socket) {
  this.parent = parent;
  this.parentNode = parentNode;
  this.socket = socket;
  this.bucket = null;

  this.dataBuf = null;

  this.socket.on('error', this._onError.bind(this));
  this.socket.on('data', this._handleData.bind(this));
  this.socket.on('close', this._onClose.bind(this));
}
util.inherits(MemdClient, EventEmitter);

MemdClient.prototype._onClose = function() {
  console.log('MockMemdClient::close');
  this.emit('close');
};

MemdClient.prototype._onError = function(err) {
  console.log('CbMemdClient::error', err);
};

MemdClient.prototype._handleReqPacket = function(data) {
  // Early out!
  var seqNo = data.readUInt32BE(12);
  var opCode = data.readUInt8(1);
  var keyLen = data.readUInt16BE(2);
  var extLen = data.readUInt8(4);
  var datatype = data.readUInt8(5);
  var vbid = data.readUInt16BE(6);
  var key = null;
  if (keyLen > 0) {
    key = data.slice(24+extLen, 24+extLen+keyLen);
  }
  var value = null;
  if (data.length > 24+extLen+keyLen) {
    value = data.slice(24+extLen+keyLen);
  }

  var cas = [
    data.readUInt32BE(16),
    data.readUInt32BE(20)
  ];

  if (opCode === MEMCACHED_CMD.SASL_AUTH) {
    this._handleSaslAuth({
      seqNo: seqNo,
      data: value
    });
  } else {
    if (!this.parentNode.isVBucketMaster(this.bucket, vbid)) {
      return this.notMyVBucket({
        seqNo: seqNo,
        op: opCode
      });
    }

    if (opCode === MEMCACHED_CMD.SET) {
      if (extLen < 8) {
        throw new Error('invalid set command');
      }

      var flags = data.readUInt32BE(24);
      var expiry = data.readUInt32BE(28);

      this._handleSet({
        seqNo: seqNo,
        vbId: vbid,
        key: key,
        cas: cas,
        datatype: datatype,
        flags: flags,
        value: value
      });
    } else if (opCode === MEMCACHED_CMD.GET) {
      this._handleGet({
        seqNo: seqNo,
        vbId: vbid,
        key: key
      });
    } else {
      this.unknownCommand({
        seqNo: seqNo,
        op: opCode
      });
    }
  }
};

MemdClient.prototype._handleSaslAuth = function(options) {
  var value = options.data;

  var splitValue = bufferSplit(options.data, 0);
  console.log(splitValue);

  if (splitValue.length !== 3) {
    return this.invalidArguments({
      seqNo: options.seqNo,
      op: MEMCACHED_CMD.SASL_AUTH
    });
  }

  var authzid = splitValue[0].toString('utf8');
  var authcid = splitValue[1].toString('utf8');
  var passwd = splitValue[2].toString('utf8');

  var bucket = this.parent.buckets[authcid];
  if (!bucket) {
    return this.saslAuth({
      seqNo: options.seqNo,
      result: MEMCACHED_STATUS.AUTH_ERROR
    });
  }

  this.bucket = bucket;
  this.saslAuth({
    seqNo: options.seqNo,
    result: MEMCACHED_STATUS.SUCCESS
  });
};

function generateCas() {
  return [
    Math.floor(Math.random() * 0xFFFFFFFE) + 1,
    Math.floor(Math.random() * 0xFFFFFFFE) + 1
  ];
}
function compareCas(oldCas, newCas) {
  if (newCas[0] === 0 && newCas[1] === 0) {
    return true;
  }
  if (!oldCas) {
    return false;
  }
  if (newCas[0] === oldCas[0] && newCas[1] === oldCas[1]) {
    return true;
  }
  return false;
}

MemdClient.prototype._handleSet = function(options) {
  var kRef = this.bucket.getKeyRef(options.vbId, 0, options.key);
  var kVal = this.bucket.getKeyData(kRef);

  if (kVal && !compareCas(kVal.cas, options.cas)) {
    return this.keyNotFound({
      op: MEMCACHED_CMD.SET,
      seqNo: options.seqNo
    });
  } else if (!kVal && !compareCas(null, options.cas)) {
    return this.keyAlreadyExists({
      op: MEMCACHED_CMD.SET,
      seqNo: options.seqNo
    });
  }

  kVal = {
    cas: generateCas(),
    datatype: options.datatype,
    flags: options.flags,
    value: options.value
  };
  this.bucket.setKeyData(kRef, kVal);

  var buf = memdRespPacket(
    MEMCACHED_CMD.SET,
    MEMCACHED_STATUS.SUCCESS,
    0,
    options.seqNo,
    kVal.cas,
    0,
    null,
    null
  );
  this.socket.write(buf);
};

MemdClient.prototype._handleGet = function(options) {
  var kRef = this.bucket.getKeyRef(options.vbId, 0, options.key);
  var kVal = this.bucket.getKeyData(kRef);

  if (!kVal) {
    return this.keyNotFound({
      op: MEMCACHED_CMD.GET,
      seqNo: options.seqNo
    });
  }

  var buf = memdRespPacket(
    options.op,
    MEMCACHED_STATUS.SUCCESS,
    kVal.datatype,
    options.seqNo,
    kVal.cas,
    4,
    null,
    kVal.value
  );
  buf.writeUInt32BE(kVal.flags, 24);
  this.socket.write(buf);
};

var BLANK_BUFFER = new Buffer('something...', 'utf8');

MemdClient.prototype.saslAuth = function(options) {
  var buf = memdRespPacket(
    MEMCACHED_CMD.SASL_AUTH,
    options.result,
    0,
    options.seqNo,
    null,
    0,
    null,
    BLANK_BUFFER
  );
  this.socket.write(buf);
};

MemdClient.prototype.invalidArguments = function(options) {
  var buf = memdRespPacket(
    options.op,
    MEMCACHED_STATUS.EINVAL,
    0,
    options.seqNo,
    null,
    0,
    null,
    null
  );
  this.socket.write(buf);
};

MemdClient.prototype.unknownCommand = function(options) {
  var buf = memdRespPacket(
    options.op,
    MEMCACHED_STATUS.UNKNOWN_COMMAND,
    0,
    options.seqNo,
    null,
    0,
    null,
    null
  );
  this.socket.write(buf);
};

MemdClient.prototype.notMyVBucket = function(options) {
  var buf = memdRespPacket(
    options.op,
    MEMCACHED_STATUS.NOT_MY_VBUCKET,
    0,
    options.seqNo,
    null,
    0,
    null,
    null
  );
  this.socket.write(buf);
};

MemdClient.prototype.keyNotFound = function(options) {
  var buf = memdRespPacket(
    options.op,
    MEMCACHED_STATUS.KEY_ENOENT,
    0,
    options.seqNo,
    null,
    0,
    null,
    null
  );
  this.socket.write(buf);
};

MemdClient.prototype.keyAlreadyExists = function(options) {
  var buf = memdRespPacket(
    options.op,
    MEMCACHED_STATUS.KEY_EXISTS,
    0,
    options.seqNo,
    null,
    0,
    null,
    null
  );
  this.socket.write(buf);
};

MemdClient.prototype._handlePacket = function(data) {
  var magic = data.readUInt8(0);
  if (magic === MEMCACHED_REQUEST_MAGIC) {
    this._handleReqPacket(data);
  } else {
    // TODO: Send some kind of response
  }
};

MemdClient.prototype._tryReadPacket = function(data, off) {
  if (data.length >= off+24) {
    var bodyLen = data.readUInt32BE(off+8);
    var packetLen = 24 + bodyLen;
    if (data.length >= off+packetLen) {
      this._handlePacket(data.slice(off, off+packetLen));
      return packetLen;
    } else {
      return 0;
    }
  } else {
    return 0;
  }
};

MemdClient.prototype._handleData = function(data) {
  if (this.dataBuf === null) {
    this.dataBuf = data;
  } else {
    var totalLen = this.dataBuf.length + data.length;
    this.dataBuf = Buffer.concat([this.dataBuf, data], totalLen);
  }

  var offset = 0;
  while (offset < this.dataBuf.length) {
    var packetLen = this._tryReadPacket(this.dataBuf, offset);
    if (packetLen <= 0) {
      break;
    }

    offset += packetLen;
  }

  if (offset === this.dataBuf.length) {
    this.dataBuf = null;
  } else {
    this.dataBuf = this.dataBuf.slice(offset);
  }
};

module.exports = MemdClient;
