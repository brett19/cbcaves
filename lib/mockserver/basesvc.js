"use strict";

var utils = require('./utils');

function BaseService(parent, parentNode) {
  this.parent = parent;
  this.parentNode = parentNode;

  this.server = null;
  this.port = 0;
  this.online = false;
  this.paused = false;

  this.sslServer = null;
  this.sslPort = 0;
  this.sslOnline = false;
  this.sslPaused = false;
}

BaseService.prototype.startAll = function(callback) {
  var maybeCallback = utils.waitForAllCallback(callback);
  this.start(maybeCallback());
  this.startSsl(maybeCallback());
};
BaseService.prototype.stopAll = function(callback) {
  var maybeCallback = utils.waitForAllCallback(callback);
  this.stop(maybeCallback());
  this.stopSsl(maybeCallback());
};
BaseService.prototype.pauseAll = function() {
  this.pause();
  this.pauseSsl();
};
BaseService.prototype.resumeAll = function() {
  this.resume();
  this.resumeSsl();
};

BaseService.prototype.start = function(callback) {
  if (this.online) {
    return callback();
  }

  var self = this;
  this.server.listen(this.parent.pickSvcPort(), function() {
    self.port = self.server.address().port;
    callback();
  });
  this.online = true;
};

BaseService.prototype.stop = function(callback) {
  if (!this.online) {
    return callback();
  }

  this.server.close(callback);
  this.port = 0;
  this.online = false;
};

BaseService.prototype.pause = function() {
  this.paused = true;
};
BaseService.prototype.resume = function() {
  this.paused = false;
};

BaseService.prototype.startSsl = function(callback) {
  if (this.sslOnline) {
    return callback();
  }

  var self = this;
  this.sslServer.listen(this.parent.pickSvcPort(), function() {
    self.sslPort = self.sslServer.address().port;
    callback();
  });
  this.sslOnline = true;
};

BaseService.prototype.stopSsl = function(callback) {
  if (!this.sslOnline) {
    return callback();
  }

  this.sslServer.close(callback);
  this.sslPort = 0;
  this.sslOnline = false;
};

BaseService.prototype.pauseSsl = function() {
  this.sslPaused = true;
};
BaseService.prototype.resumeSsl = function() {
  this.sslPaused = false;
};

module.exports = BaseService;
