"use strict";

var utils = require('./utils');

function Cas() {
  this.hi = Math.floor(Math.random() * 0xFFFFFFFE) + 1;
  this.lo = Math.floor(Math.random() * 0xFFFFFFFE) + 1;
}

Cas.readFromBuffer = function(buf, off) {
  var cas = new Cas();
  cas.hi = buf.readUInt32BE(off+0);
  cas.lo = buf.readUInt32BE(off+4);
  return cas;
};

Cas.writeToBuffer = function(buf, off, cas) {
  if (cas) {
    buf.fastWrite(off+0, cas.hi, 4);
    buf.fastWrite(off+4, cas.lo, 4);
  } else {
    buf.fastWrite(off+0, 0, 4);
    buf.fastWrite(off+4, 0, 4);
  }
};

Cas.compare = function(old, check) {
  if (!check || check.hi === 0 && check.lo === 0) {
    return true;
  }
  if (old && old.hi === check.hi && old.lo === check.lo) {
    return true;
  }
  return false;
};

module.exports = Cas;
