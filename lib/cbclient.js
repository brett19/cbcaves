"use strict";

var http = require('http');
var https = require('https');
var crc32 = require('./crc32');
var util = require('util');

var EventEmitter = require('events').EventEmitter;
var CbMemdClient = require('./memdclient');
var HttpConfigMgr = require('./httpconfigmgr');
var CccpConfigMgr = require('./cccpconfigmgr');

var VERSION = [0x000002, '0.0.2'];

var FORMAT = {
  json: 0,
  raw: 2,
  utf8: 4,
  auto: 0x777777
};

var ERRORS = {
  deltaBadVal: 1,
  objectTooBig: 2,
  invalidRange: 3,
  keyAlreadyExists: 4,
  keyNotFound: 5,
  networkError: 6,
  timedOut: 7,
  bucketNotFound: 8,
  invalidArguments: 9,
  checkResults: 10,
  durabilityFailed: 11,
  restError: 12,
  protocolError: 13,
  temporaryError: 14
};

var OLD_ERRORS = {
  success: 0,
  authContinue: -1,
  authError: -1,
  deltaBadVal: ERRORS.deltaBadVal,
  objectTooBig: ERRORS.objectTooBig,
  serverBusy: -1,
  cLibInternal: -1,
  cLibInvalidArgument: -1,
  cLibOutOfMemory: -1,
  invalidRange: ERRORS.invalidRange,
  cLibGenericError: -1,
  temporaryError: ERRORS.temporaryError,
  keyAlreadyExists: ERRORS.keyAlreadyExists,
  keyNotFound: ERRORS.keyNotFound,
  failedToOpenLibrary: -1,
  failedToFindSymbol: -1,
  networkError: ERRORS.networkError,
  wrongServer: -1,
  notMyVBucket: -1,
  // The only time we receive NOT_STORED is when doing append/prepend
  //   against a non-existant key.
  notStored: ERRORS.keyNotFound,
  notSupported: -1,
  unknownCommand: -1,
  unknownHost: -1,
  protocolError: ERRORS.protocolError,
  timedOut: ERRORS.timedOut,
  connectError: -1,
  bucketNotFound: ERRORS.bucketNotFound,
  clientOutOfMemory: -1,
  clientTemporaryError: -1,
  badHandle: -1,
  serverBug: -1,
  invalidHostFormat: -1,
  notEnoughNodes: -1,
  duplicateItems: -1,
  noMatchingServerForKey: -1,
  badEnvironmentVariable: -1,
  outOfMemory: -1,
  invalidArguments: ERRORS.invalidArguments,
  schedulingError: -1,
  checkResults: ERRORS.checkResults,
  genericError: -1,
  durabilityFailed: ERRORS.durabilityFailed,
  restError: ERRORS.restError
};

var ERRORTEXTS = {
  1: 'bad delta value',
  2: 'value too large',
  3: 'invalid range',
  4: 'key already exists',
  5: 'key not found',
  6: 'network error',
  7: 'operation timed out',
  8: 'bucket not found',
  9: 'invalid arguments',
  10: 'multiple errors occurred',
  11: 'durability requirements failed to be met',
  12: 'generic rest error',
  13: 'protocol error',
  14: 'transient error, try again'
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

function makeError(errCode) {
  var err = null;
  if (ERRORTEXTS[errCode]) {
    err = new Error(ERRORTEXTS[errCode]);
  } else {
    err = new Error('unknown error: 0x' + errCode.toString(16));
  }
  err.code = errCode;
  return err;
}

function normalizeFormat(format) {
  if (format === undefined) {
    format = FORMAT.auto;
  }

  if (format === 'json') {
    format = FORMAT.json;
  } else if (format === 'utf8') {
    format = FORMAT.utf8;
  } else if (format === 'raw') {
    format = FORMAT.raw;
  } else if (format === 'auto') {
    format = FORMAT.auto;
  }

  if (format !== FORMAT.auto && format !== FORMAT.json &&
      format !== FORMAT.utf8 && format !== FORMAT.raw) {
    throw makeError(ERRORS.invalidArguments);
  }

  return format;
}

function encodeValue(value, format) {
  format = normalizeFormat(format);

  if (format === FORMAT.auto) {
    if (Buffer.isBuffer(value)) {
      format = FORMAT.raw;
    } else if (typeof(value) === 'string') {
      format = FORMAT.utf8;
    } else if (typeof(value) === 'number' || value instanceof Object) {
      format = FORMAT.json;
    } else {
      format = FORMAT.utf8;
    }
  }

  var data = null;
  var flags = format;
  var datatype = 0;

  if (format === FORMAT.raw) {
    if (!Buffer.isBuffer(value)) {
      throw makeError(ERRORS.invalidArguments);
    }
    data = value;
  } else if (format === FORMAT.utf8) {
    data = new Buffer(value, 'utf8');
  } else if (format === FORMAT.json) {
    data = new Buffer(JSON.stringify(value), 'utf8');
  }

  return [data, flags, datatype];
}

function decodeValue(data, flags, datatye, format) {
  if (!data) {
    return null;
  }

  format = normalizeFormat(format);
  if (format !== FORMAT.auto) {
    flags = format;
  }

  if (flags === FORMAT.json) {
    return JSON.parse(data.toString('utf8'));
  } else if (flags === FORMAT.utf8) {
    return data.toString('utf8');
  } else {
    return data;
  }
}

/**
 * @struct
 */
function BucketConfig() {
  this.vBucketServerMap = null;
}

/**
 * @struct
 */
function BucketMap() {
  this.hashAlgorithm = null;
  this.serverList = null;
  this.sslServerList = null;
  this.vBucketMap = null;
}

var CONFIGMODE = {
  http: 1,
  cccp: 2
}

function CouchbaseClient(options) {
  if (!options) {
    options = {};
  }

  // Read the options
  if (!options.hosts && options.host) {
    options.hosts = options.host;
  }
  if (!options.hosts && options.hostname) {
    options.hosts = options.hostname;
  }
  if (!options.hosts && options.hostnames) {
    options.hosts = options.hostnames;
  }

  if (options.uri) {
    this.hosts = options.uri;
    this.ssl = false;
    this.configMode = CONFIGMODE.cccp;
  } else if (options.hosts) {
    if (typeof(options.hosts) === 'string') {
      options.hosts = options.hosts.split(';');
    }

    this.hosts = options.hosts;
    this.ssl = options.ssl ? true : false;
    this.configMode = CONFIGMODE.http;
  } else {
    this.hosts = ['127.0.0.1:8091'];
    this.ssl = false;
    this.configMode = CONFIGMODE.http;
  }

  this.bucket = options.bucket ? options.bucket : 'default';
  this.password = options.password ? options.password : '';

  // Set up some state tracking stuff
  this._bucketMap = null;
  this._serverList = {};
  this._serverLookup = [];

  // Set up some empty command queues
  this._pending = [];
  this._pendingFor = [];

  // Setup our special properties
  Object.defineProperty(this, "clientVersion", {
    enumerable: false,
    configurable: false,
    writable: false,
    value: VERSION
  });

  // Begin bootstrapping to the cluster
  this._beginBootstrap();
}
util.inherits(CouchbaseClient, EventEmitter);

CouchbaseClient.prototype._beginBootstrap = function() {
  console.info('[ core] initializing (' + this.ssl + ',' + this.bucket + ')');
  for (var i = 0; i < this.hosts.length; ++i) {
    console.info('[ core]   ' + this.hosts[i]);
  }

  if (this.configMode === CONFIGMODE.http) {
    this._config = new HttpConfigMgr(this.hosts, this);
  } else if (this.configMode === CONFIGMODE.cccp) {
    this._config = new CccpConfigMgr(this.hosts, this);
  } else {
    throw new Error('unknown config mode');
  }

  var self = this;
  this._config.on('newConfig', function(config) {
    console.info('[core ] received new config');
    self._onNewConfig(config);
  });
};

CouchbaseClient.prototype._mapBucket = function(key) {
  var keyCrc = crc32(key);
  return keyCrc % this._bucketMap.vBucketMap.length;
};

CouchbaseClient.prototype._mapVBucket = function(vbId, replicaId) {
  if (vbId < 0 || vbId >= this._bucketMap.vBucketMap.length) {
    throw new Error('invalid bucket id.');
  }
  var repList = this._bucketMap.vBucketMap[vbId];

  if (replicaId < 0 || replicaId >= repList.length) {
    throw new Error('invalid replica id.');
  }
  var serverId = repList[replicaId];

  return serverId;
};

CouchbaseClient.prototype.vbMappingInfo = function(key) {
  var vbId = this._mapBucket(key);
  var serverIdx = this._mapVBucket(vbId, 0);
  return [serverIdx, vbId];
};

CouchbaseClient.prototype._onNewConfig = function(config) {
  var bucketMap = config.vBucketServerMap;

  if (bucketMap.hashAlgorithm !== 'CRC') {
    throw new Error('Bad Hashing Algorithm');
  }

  var serverList = bucketMap.serverList;
  if (this.ssl) {
    serverList = bucketMap.sslServerList;
  }

  var self = this;
  var boundDeschedule = function() {
    self._descheduleFor();
  };

  this._serverLookup = [];
  for (var i = 0; i < serverList.length; ++i) {
    var serverName = serverList[i];

    var client = this._getMemdClient(serverName, boundDeschedule);
    this._serverLookup[i] = client;
  }

  this._bucketMap = bucketMap;
  this._deschedule();
  this._descheduleFor();
};

CouchbaseClient.prototype._getMemdClient = function(name, callback) {
  // Check if we already have a client ready
  var client = this._serverList[name];
  if (client && client.connected) {
    process.nextTick(callback);
    return client;
  }

  if (!client) {
    // set up a new client!
    var hpSplit = name.split(':');
    var host = hpSplit[0];
    var port = parseInt(hpSplit[1], 10);

    client = new CbMemdClient(host, port, this.ssl, this.bucket, this.password);

    var self = this;
    client.on('nmvConfig', function(config) {
      self._config.injectNewConfig(config, client.host);
    });

    this._serverList[name] = client;
  }

  client.on('bucketConnect', callback);
  return client;
};

CouchbaseClient.prototype._getServer = function(vbId, replicaId) {
  var serverId = this._mapVBucket(vbId, replicaId);

  if (serverId === null) {
    // This map value has been marked as incorrect
    return null;
  }
  var server = this._serverLookup[serverId];

  if (!server || !server.connected) {
    return null;
  }
  return server;
};

CouchbaseClient.prototype._tryDispatch = function(op) {
  var server = this._getServer(op.vbId, op.replicaId);
  if (server) {
    // This writes to the wire, beyond this point, the operation is now
    //   uncancellable or we could end up executing the operation multiple
    //   times on the cluster side.
    var self = this;
    var seqNo = op.handler.call(server, op.options, function(errCode, data) {
      self._handleOpResult(op, errCode, data);
    });

    // Store a reference to where the op was dispatched so we can
    //   cancel the callback in the future if needed.
    op.server = server;
    op.seqNo = seqNo;

    return true;
  }

  return false;
};

CouchbaseClient.prototype._rescheduleOp = function(op) {
  // No longer attached to a server!
  op.server = null;
  op.seqNo = 0;

  // Reschedule the command, note that we don't try to execute the
  //   command immediately since we just invalided the entry above
  //   and we need a new config first.
  this._pendingFor.push(op);
};

CouchbaseClient.prototype._handleOpResult = function(op, errCode, data) {
// Handle errors that we handle internally by rescheduling them.
  if (errCode === MEMCACHED_STATUS.NOT_MY_VBUCKET) {
    // Mark the map entry as invalid
    this._bucketMap.vBucketMap[op.vbId][op.replicaId] = null;
    this._config.markInvalid();

    return this._rescheduleOp(op);
  } else if (errCode === MEMCACHED_STATUS.EBUSY) {
    return this._rescheduleOp(op);
  } else if (errCode === MEMCACHED_STATUS.EINTERNAL) {
    return this._rescheduleOp(op);
  }

  // Kill the timeout timer since the command completed.
  if (op.timer) {
    clearTimeout(op.timer);
    op.timer = null;
  }

  var callback = op.callback;

  // Handle errors that we have to push back onto the application.
  if (errCode === MEMCACHED_STATUS.DELTA_BADVAL) {
    return callback(makeError(ERRORS.deltaBadVal), data);
  } else if (errCode === MEMCACHED_STATUS.E2BIG) {
    return callback(makeError(ERRORS.objectTooBig), data);
  } else if (errCode === MEMCACHED_STATUS.EINVAL) {
    return callback(makeError(ERRORS.networkError), data);
  } else if (errCode === MEMCACHED_STATUS.ERANGE) {
    return callback(makeError(ERRORS.invalidRange), data);
  } else if (errCode === MEMCACHED_STATUS.KEY_ENOENT) {
    return callback(makeError(ERRORS.keyNotFound), data);
  } else if (errCode === MEMCACHED_STATUS.KEY_EXISTS) {
    return callback(makeError(ERRORS.keyAlreadyExists), data);
  } else if (errCode === MEMCACHED_STATUS.NOT_STORED) {
    return callback(makeError(ERRORS.keyNotFound), data);
  } else if (errCode === MEMCACHED_STATUS.ETMPFAIL) {
    return callback(makeError(ERRORS.temporaryError), data);
  } else if (errCode === 0x1000) {
    return callback(makeError(ERRORS.networkError), data);

  } else if (errCode === MEMCACHED_STATUS.UNKNOWN_COMMAND) {
    console.info('[core ] received UNKNOWN_COMMAND status from server');
    return callback(makeError(ERRORS.protocolError), data);
  } else if (errCode === MEMCACHED_STATUS.NOT_SUPPORTED) {
    console.info('[core ] received NOT_SUPPORTED status from server');
    return callback(makeError(ERRORS.protocolError), data);
  } else if (errCode) {
    console.info('[core ] received unknown status from server (' + errCode + ')');
    return callback(makeError(ERRORS.protocolError), data);
  }

  // Call the callback!
  callback(null, data);
};

CouchbaseClient.prototype._handleOpTimeout = function(op) {
  if (!op.server) {
    var pendingIdx = this._pendingFor.indexOf(op);
    if (pendingIdx !== -1) {
      this._pendingFor.splice(pendingIdx, 1);
    }
  } else {
    op.server.cancelOp(op.seqNo);
  }

  op.callback(makeError(ERRORS.timedOut), {
    key: op.key
  });
};

CouchbaseClient.prototype._scheduleFor = function(vbId, replicaId, timeout, handler, options, callback) {
  var op = {
    vbId: vbId,
    replicaId: replicaId,
    handler: handler,
    options: options,
    callback: callback,
    server: null,
    seqNo: 0
  };

  // Try to dispatch this operation to the server, if we are unable to,
  //   add it to our pending queue.
  if (!this._tryDispatch(op)) {
    this._pendingFor.push(op);
  }

  // timeout of -1 means to use the default operation timeout
  if (timeout === -1) {
    timeout = 2500;
  }

  // If the caller wants this command to timeout after a certain period,
  //   it is handled here.
  if (timeout > 0) {
    var self = this;
    op.timer = setTimeout(function() {
      self._handleOpTimeout(op);
    }, timeout);
  }
};

CouchbaseClient.prototype._descheduleFor = function() {
  var newPendingFor = [];
  for (var i = 0; i < this._pendingFor.length; ++i) {
    var op = this._pendingFor[i];

    if (!this._tryDispatch(op)) {
      newPendingFor.push(op);
    }
  }
  this._pendingFor = newPendingFor;
};


CouchbaseClient.prototype._store = function(options, callback, create) {
  var vbId = this._mapBucket(options.key);

  var valueinfo = null;
  try {
    valueinfo = encodeValue(options.value, options.format);
  } catch (e) {
    return callback(e, null);
  }
  var value = valueinfo[0];
  var flags = valueinfo[1];
  var datatype = valueinfo[2];

  if (options.flags !== undefined) {
    flags = options.flags;
  }

  this._scheduleFor(vbId, 0, -1, CbMemdClient.prototype.store, {
    key: options.key,
    vbId: vbId,
    cas: options.cas,
    expiry: options.expiry,
    flags: flags,
    datatype: datatype,
    value: value,
    create: create
  }, function(err, data) {
    if (err) {
      return callback(err, null, data.key);
    }

    callback(null, {
      key: data.key,
      cas: data.cas
    }, data.key);
  });
};
CouchbaseClient.prototype._set = function(options, callback) {
  return this._store(options, callback, undefined);
};
CouchbaseClient.prototype._add = function(options, callback) {
  return this._store(options, callback, true);
};
CouchbaseClient.prototype._replace = function(options, callback) {
  return this._store(options, callback, false);
};

CouchbaseClient.prototype._concatStore = function(options, callback, prepend) {
  var vbId = this._mapBucket(options.key);

  var valueinfo = null;
  try {
    valueinfo = encodeValue(options.value, options.format);
  } catch (e) {
    return callback(e, null);
  }
  var value = valueinfo[0];

  this._scheduleFor(vbId, 0, -1, CbMemdClient.prototype.concatStore, {
    key: options.key,
    vbId: vbId,
    cas: options.cas,
    value: value,
    prepend: prepend
  }, function(err, data) {
    if (err) {
      return callback(err, null, data.key);
    }

    callback(null, {
      key: data.key,
      cas: data.cas
    }, data.key);
  });
};
CouchbaseClient.prototype._prepend = function(options, callback) {
  return this._concatStore(options, callback, true);
};
CouchbaseClient.prototype._append = function(options, callback) {
  return this._concatStore(options, callback, false);
};

CouchbaseClient.prototype._arithmetic = function(options, callback, dir) {
  var vbId = this._mapBucket(options.key);

  if (options.offset === undefined) {
    options.offset = 1;
  }
  if (options.initial === undefined) {
    options.initial = 0;
    options.expiry = 0xFFFFFFFF;
  }

  if (dir === -1) {
    options.offset = -options.offset;
  }

  this._scheduleFor(vbId, 0, -1, CbMemdClient.prototype.arithmetic, {
    key: options.key,
    vbId: vbId,
    cas: options.cas,
    expiry: options.expiry,
    initial: options.initial,
    delta: options.offset
  }, function(err, data) {
    if (err) {
      return callback(err, null, data.key);
    }

    callback(null, {
      key: data.key,
      value: data.value
    }, data.key);
  });
};
CouchbaseClient.prototype._incr = function(options, callback) {
  return this._arithmetic(options, callback, +1);
};
CouchbaseClient.prototype._decr = function(options, callback) {
  return this._arithmetic(options, callback, -1);
};

CouchbaseClient.prototype._retrieve = function(options, callback) {
  var vbId = this._mapBucket(options.key);

  this._scheduleFor(vbId, 0, -1, CbMemdClient.prototype.get, {
    key: options.key,
    vbId: vbId,
    expiry: options.expiry,
    locktime: options.locktime
  }, function(err, data) {
    if (err) {
      return callback(err, null, data.key);
    }

    var value = decodeValue(data.value, data.flags, data.datatype, options.format);

    callback(null, {
      key: data.key,
      value: value,
      flags: data.flags,
      cas: data.cas
    }, data.key);
  });
};
CouchbaseClient.prototype._get = function(options, callback) {
  delete options.locktime;
  return this._retrieve(options, callback);
};
CouchbaseClient.prototype._lock = function(options, callback) {
  delete options.expiry;
  return this._retrieve(options, callback);
};

CouchbaseClient.prototype._unlock = function(options, callback) {
  var vbId = this._mapBucket(options.key);

  this._scheduleFor(vbId, 0, -1, CbMemdClient.prototype.unlock, {
    key: options.key,
    vbId: vbId,
    cas: options.cas
  }, function(err, data) {
    if (err) {
      return callback(err, null, data.key);
    }

    callback(null, {
      key: data.key
    }, data.key);
  });
};

CouchbaseClient.prototype._remove = function(options, callback) {
  var vbId = this._mapBucket(options.key);

  this._scheduleFor(vbId, 0, -1, CbMemdClient.prototype.remove, {
    key: options.key,
    vbId: vbId,
    cas: options.cas
  }, function(err, data) {
    if (err) {
      return callback(err, null, data.key);
    }

    callback(null, {}, data.key);
  });
};

CouchbaseClient.prototype._test = function(options, callback) {
  var vbId = this._mapBucket(options.key);

  this._scheduleFor(vbId, 0, 0, CbMemdClient.prototype.uprOpenChannel, {
    name: 'teststream'
  }, function(err, data) {
    if (err) {
      return callback(err, null);
    }

    this._scheduleFor(vbId, 0, 0, CbMemdClient.prototype.uprStreamRequest, {
      vbId: vbId
    }, function(err, data) {
      if (err) {
        return callback(err, null, data.key);
      }

      callback(null, data, data.key);
    }.bind(this));

  }.bind(this));
};

function _quickMerge(target) {
  for (var j = 1; j < arguments.length; ++j) {
    var src = arguments[j];
    for (var i in src) {
      if (src.hasOwnProperty(i)) {
        if (target[i] === undefined) {
          target[i] = src[i];
        }
      }
    }
  }
  return target;
}





CouchbaseClient.prototype._deschedule = function() {
  for (var i = 0; i < this._pending.length; ++i) {
    var opInfo = this._pending[i];
    opInfo[0].call(this, opInfo[1], opInfo[2]);
  }
  this._pending = [];
};

CouchbaseClient.prototype._schedule = function(op, options, callback) {
  if (this._bucketMap) {
    op.call(this, options, callback);
  } else {
    this._pending.push([op, options, callback]);
  }
};

CouchbaseClient.prototype._wrapAndSchedule = function(op, options, callback) {
  if (!options.persist_to && !options.replicate_to) {
    return this._schedule(op, options, callback);
  }

  // Do durability checking stuff
  return this._schedule(op, options, function(err, res, key) {
    callback(err, res, key);
  });
};

function wrapMKOC(func) {
  return function(keys, options, callback) {
    if (typeof(options) === 'function') {
      callback = options;
      options = {};
    }
    if (!options) {
      options = {};
    }

    // Validate keys
    if (Array.isArray(keys)) {
      for (var i = 0; i < keys.length; ++i) {
        if (typeof(keys[i]) !== 'string') {
          return callback(new Error('key is not a string'), null);
        }
      }
    } else if (keys instanceof Object) {
      for (var i in keys) {
        if (keys.hasOwnProperty(i)) {
          if (!(keys[i] instanceof Object)) {
            return callback(new Error('key is not a string'), null);
          }
        }
      }
    } else {
      return callback(makeError(ERRORS.invalidArguments), null);
    }

    var spoolCallback = callback;
    var spoolsRemain = 0;
    var spoolObj = {};
    var spoolErrs = 0;
    if (options.spooled !== false) {
      spoolCallback = function(err, res, key) {
        if (err) {
          spoolObj[key] = {error: err};
          spoolErrs++;
        } else {
          spoolObj[key] = res;
        }

        if (--spoolsRemain === 0) {
          if (spoolErrs === 0) {
            callback(null, spoolObj);
          } else {
            callback(makeError(ERRORS.checkResults), spoolObj);
          }
        }
      };
    }

    if (Array.isArray(keys)) {
      for (var j = 0; j < keys.length; ++j) {
        spoolsRemain++;

        var opts = _quickMerge({}, options);
        opts.key = keys[j];
        this._wrapAndSchedule(func, opts, spoolCallback);
      }
    } else if (keys instanceof Object) {
      for (var k in keys) {
        if (keys.hasOwnProperty(k)) {
          spoolsRemain++;

          var optss = _quickMerge(keys[k], options);
          optss.key = k;
          this._wrapAndSchedule(func, optss, spoolCallback);
        }
      }
    }
  };
};

function wrapKOC(func) {
  return function(key, options, callback) {
    if (typeof(options) === 'function') {
      callback = options;
      options = {};
    }
    if (!options) {
      options = {};
    }

    if (typeof(key) !== 'string') {
      return callback(new Error('key is not a string'), null);
    }

    options.key = key;
    return this._wrapAndSchedule(func, options, callback);
  };
}
function wrapKVOC(func) {
  return function(key, value, options, callback) {
    if (typeof(options) === 'function') {
      callback = options;
      options = {};
    }
    if (!options) {
      options = {};
    }

    if (typeof(key) !== 'string') {
      return callback(new Error('key is not a string'), null);
    }

    options.key = key;
    options.value = value;
    return this._wrapAndSchedule(func, options, callback);
  };
}

CouchbaseClient.prototype.set =
  wrapKVOC(CouchbaseClient.prototype._set);
CouchbaseClient.prototype.add =
  wrapKVOC(CouchbaseClient.prototype._add);
CouchbaseClient.prototype.replace =
  wrapKVOC(CouchbaseClient.prototype._replace);
CouchbaseClient.prototype.prepend =
  wrapKVOC(CouchbaseClient.prototype._prepend);
CouchbaseClient.prototype.append =
  wrapKVOC(CouchbaseClient.prototype._append);
CouchbaseClient.prototype.get =
  wrapKOC(CouchbaseClient.prototype._get);
CouchbaseClient.prototype.lock =
  wrapKOC(CouchbaseClient.prototype._lock);
CouchbaseClient.prototype.unlock =
  wrapKOC(CouchbaseClient.prototype._unlock);
CouchbaseClient.prototype.incr =
  wrapKOC(CouchbaseClient.prototype._incr);
CouchbaseClient.prototype.decr =
  wrapKOC(CouchbaseClient.prototype._decr);
CouchbaseClient.prototype.remove =
  wrapKOC(CouchbaseClient.prototype._remove);

CouchbaseClient.prototype.setMulti =
  wrapMKOC(CouchbaseClient.prototype._set);
CouchbaseClient.prototype.addMulti =
  wrapMKOC(CouchbaseClient.prototype._add);
CouchbaseClient.prototype.replaceMulti =
  wrapMKOC(CouchbaseClient.prototype._replace);
CouchbaseClient.prototype.prependMulti =
  wrapMKOC(CouchbaseClient.prototype._prepend);
CouchbaseClient.prototype.appendMulti =
  wrapMKOC(CouchbaseClient.prototype._append);
CouchbaseClient.prototype.getMulti =
  wrapMKOC(CouchbaseClient.prototype._get);
CouchbaseClient.prototype.lockMulti =
  wrapMKOC(CouchbaseClient.prototype._lock);
CouchbaseClient.prototype.unlockMulti =
  wrapMKOC(CouchbaseClient.prototype._unlock);
CouchbaseClient.prototype.incrMulti =
  wrapMKOC(CouchbaseClient.prototype._incr);
CouchbaseClient.prototype.decrMulti =
  wrapMKOC(CouchbaseClient.prototype._decr);
CouchbaseClient.prototype.removeMulti =
  wrapMKOC(CouchbaseClient.prototype._remove);

CouchbaseClient.prototype.test =
  wrapKOC(CouchbaseClient.prototype._test);

CouchbaseClient.errors = OLD_ERRORS;
CouchbaseClient.format = FORMAT;

module.exports = CouchbaseClient;
