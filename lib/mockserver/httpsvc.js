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
  this._setupServer(server);
  server.use(function(req, res, next) {
    if (!self.paused) {
      next();
    }
  });
  this._setupRoutes(server);
  this.server = http.createServer(server);

  var sslServer = express();
  this._setupServer(sslServer);
  sslServer.use(function(req, res, next) {
    if (!self.sslPaused) {
      next();
    }
  });
  this._setupRoutes(sslServer);
  this.sslServer = https.createServer(this.parent.sslCreds, sslServer);
}
util.inherits(HttpService, BaseService);

HttpService.prototype._setupServer = function(app) {
  app.set('json spaces', 0);
  app.use(express.json());
  app.use(express.urlencoded());
  app.use(function(req, res, next) {
    if (req._body) return next();
    var data='';
    req.setEncoding('utf8');
    req.on('data', function(chunk) {
      data += chunk;
    });

    req.on('end', function() {
      req.rawBody = data;
      next();
    });
  });
};

HttpService.prototype._setupRoutes = function(app) {
  throw new Error('unoverriden base function');
};

module.exports = HttpService;
