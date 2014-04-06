"use strict";

var utils = require('./utils');

var MgmtService = require('./mgmtsvc');
var CapiService = require('./capisvc');
var MemdService = require('./memdsvc');
var N1qlService = require('./n1qlsvc');

var globalNodeId = 1;

function Node(parent, callback) {
  this.parent = parent;
  this.nodeId = globalNodeId++;

  this.host = '127.0.0.1';

  this.capiSvc = new CapiService(this.parent, this);
  this.mgmtSvc = new MgmtService(this.parent, this);
  this.memdSvc = new MemdService(this.parent, this);
  this.n1qlSvc = new N1qlService(this.parent, this);

  var self = this;

  this.startAllServices(function() {
    console.info('started new node with services:');
    console.info('  capi: ' + self.capiSvc.port + ', ' + self.capiSvc.sslPort);
    console.info('  mgmt: ' + self.mgmtSvc.port + ', ' + self.mgmtSvc.sslPort);
    console.info('  memd: ' + self.memdSvc.port + ', ' + self.memdSvc.sslPort);
    console.info('  n1ql: ' + self.n1qlSvc.port + ', ' + self.n1qlSvc.sslPort);

    callback();
  });
}

Node.prototype.destroy = function(callback) {
  this.memdSvc.disconnectAll();
  this.mgmtSvc.disconnectAll();
  this.stopAllServices(callback);
};

Node.prototype.startAllServices = function(callback) {
  var maybeCallback = utils.waitForAllCallback(callback);
  this.capiSvc.startAll(maybeCallback());
  this.mgmtSvc.startAll(maybeCallback());
  this.memdSvc.startAll(maybeCallback());
  this.n1qlSvc.startAll(maybeCallback());
};
Node.prototype.stopAllServices = function(callback) {
  var maybeCallback = utils.waitForAllCallback(callback);
  this.capiSvc.stopAll(maybeCallback());
  this.mgmtSvc.stopAll(maybeCallback());
  this.memdSvc.stopAll(maybeCallback());
  this.n1qlSvc.stopAll(maybeCallback());
};
Node.prototype.pauseAllServices = function() {
  this.capiSvc.pauseAll();
  this.mgmtSvc.pauseAll();
  this.memdSvc.pauseAll();
  this.n1qlSvc.pauseAll();
};
Node.prototype.resumeAllServices = function() {
  this.capiSvc.resumeAll();
  this.mgmtSvc.resumeAll();
  this.memdSvc.resumeAll();
  this.n1qlSvc.resumeAll();
};

module.exports = Node;