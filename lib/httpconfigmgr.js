"use strict";

var util = require('util');
var http = require('http');
var https = require('https');

var ConfigMgr = require('./configmgr');
var BurnoutList = require('./burnoutlist');

function HttpConfigMgr(hosts, parent) {
  ConfigMgr.call(this);

  this.hosts = new BurnoutList(5000, hosts);
  this.parent = parent;
  this.activeReq = null;

  this._nextNode();
}
util.inherits(HttpConfigMgr, ConfigMgr);

HttpConfigMgr.prototype.injectNewConfig = function(config, srcHost) {
  // HTTP does not handle NMV configs
};

HttpConfigMgr.prototype._updateNodesFromConfig = function(config) {
  var hostlist = [];
  for (var i = 0; i < config.nodes.length; ++i) {
    var node = config.nodes[i];

    if (!this.parent.ssl) {
      hostlist.push(node.host + ':' + node.ports.httpMgmt);
    } else {
      hostlist.push(node.host + ':' + node.ports.httpsMgmt);
    }
  }

  this.hosts.set(hostlist);

  console.info('[htcfg] updated node list');
  for (var i = 0; i < hostlist.length; ++i) {
    console.info('[htcfg]   ' + hostlist[i]);
  }
};

HttpConfigMgr.prototype._forceRefresh = function() {
  if (!this.activeReq) {
    throw new Error('tried to force refresh while not streaming');
  }

  this.activeReq.socket.destroy();
  this.activeReq = null;
};

HttpConfigMgr.prototype._nextNode = function() {
  var self = this;

  var thisHost = this.hosts.poll();
  if (!thisHost) {
    console.info('[htcfg] node list exhausted, waiting');

    setTimeout(function() {
      self._nextNode();
    }, 1000);
    return;
  }

  this._startStream(thisHost);
};

HttpConfigMgr.prototype._startStream = function(hostString) {
  var self = this;

  var configStream = '';
  var hostInfo = hostString.split(':');

  var host = hostInfo[0];
  var port = parseInt(hostInfo[1], 10);

  console.info('[htcfg] opening stream (' + hostString + ',' + self.parent.ssl + ',' + self.parent.bucket + ')');

  var options = {
    hostname: host,
    port: port,
    method: 'GET',
    //auth: '', // username:password
    path: '/pools/default/bucketsStreaming/' + this.parent.bucket,
    agent: false
  };
  var handler = function(res) {
    if (res.statusCode !== 200) {
      console.info('[htcfg] request error (' + hostString + ',' + self.parent.ssl + ',' + self.parent.bucket + ',' + res.statusCode + ')');
      self.activeReq = null;
      return self._nextNode();
    }

    self.activeReq = res;

    res.setEncoding('utf8');
    res.on('data', function(chunk) {
      configStream += chunk;

      var configBlockEnd = configStream.indexOf('\n\n\n\n');
      if (configBlockEnd >= 0) {
        var myBlock = configStream.substr(0, configBlockEnd);
        configStream = configStream.substr(configBlockEnd+4);

        self._handleNewConfig(myBlock, host);
      }
    });

    res.on('error', function(e) {
      console.info('[htcfg] stream error (' + hostString + ',' + self.parent.bucket + ',' + e.toString() + ')');
    });

    res.on('close', function() {
      console.info('[htcfg] stream closed (' + hostString + ',' + self.parent.bucket + ')');
      self.activeReq = null;
      self._nextNode();
    });
  };

  var req = null;
  if (!this.ssl) {
    req = http.request(options, handler);
  } else {
    options.rejectUnauthorized = false;
    req = https.request(options, handler);
  }

  req.end();
};

module.exports = HttpConfigMgr;
