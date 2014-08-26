'use strict';

var util = require('util');
var memdproto = require('./memdproto');

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

MemdPacket.prototype.inspect = function() {
  function getPropName(enm, val) {
    for (var i in enm) {
      if (enm.hasOwnProperty(i)) {
        if (enm[i] === val) {
          return i;
        }
      }
    }
    return '0x' + val.toString(16);
  }
  var str = '';
  str += '{\n';
  for (var i in this) {
    if (this.hasOwnProperty(i)) {
      var desc = '';
      var val = this[i];
      if (i === 'magic') {
        desc = getPropName(memdproto.magic, val);
      } else if (i === 'dataType') {
        desc = '0x' + val.toString(16);
      } else if (i === 'op') {
        desc = getPropName(memdproto.cmd, val);
      } else if (i === 'status') {
        desc = getPropName(memdproto.status, val);
      }
      if (typeof val === 'string') {
        val = '"' + val + '"';
      } else if (Buffer.isBuffer(val)) {
        desc = '"' + val.toString('utf8') + '"';
        val = util.inspect(val);
      }
      str += '  ' + i + ': ' + val;
      if (desc) {
        str += ' (' + desc + ')';
      }
      str += '\n';
    }
  }
  str += '}';
  return str;
};

module.exports = MemdPacket;
