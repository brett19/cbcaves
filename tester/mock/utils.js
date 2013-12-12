"use strict";

var Long = require('long');

/*
  A wait for all callback waits for all the created callbacks to be executed
  before executing the wrapped callback itself.  This allows you to perform
  async functions in a loop which only execute the callback once all of them
  have completed.
 */
module.exports.waitForAllCallback = function(callback) {
  var waitCount = 0;
  var handler = function() {
    if (--waitCount === 0) {
      callback();
    }
  };
  return function() {
    waitCount++;
    return handler;
  };
};

Buffer.prototype.fastWrite = function(off, val, bytes) {
  for (var i = 0; i < bytes; ++i) {
    this[off+bytes-i-1] = val >> (i * 8);
  }
};

Buffer.prototype.fastRead64 = function(off) {
  return (new Long(
    this.readUInt32BE(off+4),
    this.readUInt32BE(off+0)
  )).toNumber();
};

Buffer.prototype.fastWrite64 = function(off, val) {
  var lval = Long.fromNumber(val);
  this.fastWrite(off+4, lval.getLowBitsUnsigned(), 4);
  this.fastWrite(off+0, lval.getHighBitsUnsigned(), 4);
};

Buffer.prototype.split = function(needleByte) {
  var curPartStartPos = 0;
  var parts = [];
  for (var i = 0; i < this.length; ++i) {
    if (this[i] === needleByte) {
      parts.push(this.slice(curPartStartPos, i));
      curPartStartPos = i + 1;
    }
  }
  parts.push(this.slice(curPartStartPos));
  return parts;
};
