"use strict";

var util = require('util');

var EventEmitter = require('events').EventEmitter;


function normalizeConfig(config) {
  for (var i = 0; i < config.nodes.length; ++i) {
    var node = config.nodes[i];

    var hostnameSplit = node.hostname.split(':');
    node.host = hostnameSplit[0];
    node.ports.httpMgmt = parseInt(hostnameSplit[1], 10);
  }
}

function ConfigMgr() {
  this.invalidTimer = null;
}
util.inherits(ConfigMgr, EventEmitter);

ConfigMgr.prototype.markInvalid = function() {
  if (this.invalidTimer) {
    return;
  }

  console.info('[confm] config invalidated');

  var self = this;
  this.invalidTimer = setTimeout(function() {
    console.info('[confm] invalid config failed to clear within timeout period, forcing refresh');

    self._forceRefresh();
  }, 2000);
};

ConfigMgr.prototype._handleNewConfig = function(configStr, srcHost) {
  if (srcHost) {
    configStr = configStr.replace(/\$HOST/g, srcHost);
  }

  var config = null;
  try {
    config = JSON.parse(configStr);
  } catch (e) {
    // Config was not valid JSON
    return;
  }

  if (this.invalidTimer) {
    clearTimeout(this.invalidTimer);
    this.invalidTimer = null;
  }

  normalizeConfig(config);

  this._updateNodesFromConfig(config);
  this.emit('newConfig', config);
};

ConfigMgr.prototype.injectNewConfig = function(config, srcHost) {
  throw new Error('unoverriden base function');
};
ConfigMgr.prototype._forceRefresh = function() {
  throw new Error('unoverriden base function');
};
ConfigMgr.prototype._updateNodesFromConfig = function(config) {
  throw new Error('unoverriden base function');
};

module.exports = ConfigMgr;
