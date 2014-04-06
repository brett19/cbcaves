"use strict";

var util = require('util');
var path = require('path');

var HttpService = require('./httpsvc');

function N1qlService(parent, parentNode) {
  HttpService.call(this, parent, parentNode);
}
util.inherits(N1qlService, HttpService);

N1qlService.prototype._setupRoutes = function(app) {
  app.get('/query', this._handleQueryGet.bind(this));
  app.post('/query', this._handleQueryPost.bind(this));
};

function N1qlError(code, key, message, cause) {
  Error.call(this);
  // Capture stack trace and store the stack object
  var defaultPrepareStackTrace = Error.prepareStackTrace;
  Error.prepareStackTrace = function(_, stack){ return stack; };
  Error.captureStackTrace(this, N1qlError);
  this._stack = this.stack;
  Error.prepareStackTrace = defaultPrepareStackTrace;
  // Recapture for proper stack trace now.
  Error.captureStackTrace(this, N1qlError);

  this.name = this.constructor.name;
  this.code = code;
  this.key = key;
  this.message = message;
  this.cause = cause;

  var sRoot = this._stack[0];
  var relFilePath =
    path.relative(process.cwd(), sRoot.getFileName());
  this.caller = 'MOCK::' + relFilePath + ':' + sRoot.getLineNumber();
}

N1qlService.prototype._executeQuery = function(q, callback) {
  callback(
    new N1qlError(
      5000,
      'Internal Error',
      'Optimizer Error',
      'Bucket `?` does not exist'
    ), null);
  //callback(null, []);
};

N1qlService.prototype._handleQuery = function(q, req, res, next) {
  if (!q) {
    return res.send(500, 'Missing required query string');
  }

  this._executeQuery(res.body, function(err, resultset) {
    if (err) {
      if (!(err instanceof N1qlError)) {
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

      // TODO: Check to make sure we have the right error type
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
  this._handleQuery(req.body, req, res, next);
};

module.exports = N1qlService;
