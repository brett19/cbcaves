"use strict";

var util = require('util');
var path = require('path');
var error = require('./errors');

var HttpService = require('./httpsvc');

function N1qlService(parent, parentNode) {
  HttpService.call(this, parent, parentNode);
}
util.inherits(N1qlService, HttpService);

N1qlService.prototype._setupRoutes = function(app) {
  app.get('/query', this._handleQueryGet.bind(this));
  app.post('/query', this._handleQueryPost.bind(this));
};

N1qlService.prototype._handleQuery = function(q, req, res, next) {
  if (!q) {
    return res.send(500, 'Missing required query string');
  }

  this.parent._executeQuery(q, function(err, resultset) {
    if (err) {
      if (!(err instanceof error.N1qlError)) {
        return res.send(200, {
          'error': {
            caller: '',
            cause: '',
            code: -1,
            key: 'js_error',
            message: err.message
          }
        });
      }

      return res.send(200, {
        'error': {
          caller: err.caller,
          cause: err.cause,
          code: err.code,
          key: err.key,
          message: err.message
        }
      });
    }

    res.send(200, {
      'resultset': resultset
    });
  });
};
N1qlService.prototype._handleQueryGet = function(req, res, next) {
  this._handleQuery(req.query.q, req, res, next);
};
N1qlService.prototype._handleQueryPost = function(req, res, next) {
  this._handleQuery(req.rawBody, req, res, next);
};

module.exports = N1qlService;
