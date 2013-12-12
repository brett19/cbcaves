"use strict";

var util = require('util');
var express = require('express');
var http = require('http');
var https = require('https');

var BaseService = require('./basesvc');

function HttpService(parent) {
  BaseService.call(this, parent);

  var self = this;

  var server = express();
  server.use(function(req, res, next) {
    if (!self.paused) {
      next();
    }
  });
  this._setupRoutes(server);
  this.server = http.createServer(server);

  var sslServer = express();
  sslServer.use(function(req, res, next) {
    if (!self.sslPaused) {
      next();
    }
  });
  this._setupRoutes(sslServer);
  this.sslServer = https.createServer(this.parent.sslCreds, sslServer);
}
util.inherits(HttpService, BaseService);

HttpService.prototype._setupRoutes = function(app) {
  throw new Error('unoverriden base function');
};

module.exports = HttpService;
