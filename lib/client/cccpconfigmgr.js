"use strict";

var util = require('util');
var http = require('http');
var https = require('https');

var ConfigMgr = require('./configmgr');
var BurnoutList = require('./burnoutlist');

function CccpConfigMgr(hosts, parent) {
  ConfigMgr.call(this);

  this.hosts = new BurnoutList(5000, hosts);
  this.parent = parent;

  this._nextNode();
}
util.inherits(CccpConfigMgr, ConfigMgr);

CccpConfigMgr.prototype.injectNewConfig = function(config, srcHost) {
  this._handleNewConfig(config, srcHost);
};

CccpConfigMgr.prototype._updateNodesFromConfig = function(config) {
  var hostlist = [];
  for (var i = 0; i < config.nodes.length; ++i) {
    var node = config.nodes[i];

    if (!this.parent.ssl) {
      hostlist.push(node.host + ':' + node.ports.direct);
    } else {
      hostlist.push(node.host + ':' + node.ports.sslDirect);
    }
  }

  this.hosts.set(hostlist);

  console.info('[cpcfg] updated node list');
  for (var i = 0; i < hostlist.length; ++i) {
    console.info('[cpcfg]   ' + hostlist[i]);
  }
};

CccpConfigMgr.prototype._forceRefresh = function() {
  this._nextNode();
};

CccpConfigMgr.prototype._nextNode = function() {
  var self = this;

  var thisHost = this.hosts.poll();
  if (!thisHost) {
    console.info('[cpcfg] node list exhausted, waiting');

    setTimeout(function() {
      self._nextNode();
    }, 1000);
    return;
  }

  this._makeRequest(thisHost);
};

CccpConfigMgr.prototype._makeRequest = function(hostString) {
  console.info('[cpcfg] attempting retrieval (' + hostString + ')');

  var self = this;
  var client = this.parent._getMemdClient(hostString, function() {
    client.getClusterConfig({}, function(err, data) {
      if (err) {
        console.info('[cpcfg] retrieval failed (' + hostString + ',' + err + ')');
        self._nextNode();
        return;
      }

      self._handleNewConfig(data.config, client.host);
    });
  });
};

module.exports = CccpConfigMgr;
