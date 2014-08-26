'use strict';

module.exports.magic = {
  REQUEST: 0x80,
  RESPONSE: 0x81
};

module.exports.cmd = {
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
  HELLO: 0x1f,

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

module.exports.status = {
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

module.exports.obsstate = {
  NOT_PERSISTED: 0x00,
  PERSISTED: 0x01,
  NOT_FOUND: 0x80,
  LOGICAL_DEL: 0x81
};

/**
 * Takes an expiry time as input and generates an absolute time in milliseconds
 * from it based on Couchbase semantics.  Must pass a clock source.
 *
 * @param {MockTime} clock
 * @param {number} expiry
 * @returns {number}
 */
module.exports.expiryToTs = function(clock, expiry) {
  if (expiry === 0) {
    return 0;
  }
  if (expiry <= 60*60*24*30) {
    return clock.curMs() + (expiry * 1000);
  }
  return expiry * 1000;
};

/**
 * Takes an lock time as input and generates an absolute time in milliseconds
 * from it based on Couchbase semantics.  Must pass a clock source.
 *
 * @param {MockTime} clock
 * @param {number} lockTime
 * @returns {number}
 */
module.exports.lockTimeToTs = function(clock, lockTime) {
  if (lockTime <= 0) {
    lockTime = 15;
  } else if (lockTime > 30) {
    lockTime = 30;
  }
  return clock.curMs() + (lockTime * 1000);
};

