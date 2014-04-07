"use strict";

var path = require('path');
var util = require('util');

function N1qlError(code, key, message, cause) {

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
util.inherits(N1qlError, Error);
module.exports.N1qlError = N1qlError;




function MockError(key, message, other) {
  // Capture the stack trace object itself.
  var defaultPrepareStackTrace = Error.prepareStackTrace;
  Error.prepareStackTrace = function(_, stack){ return _._stack; };
  Error.captureStackTrace(this, MockError);
  Error.prepareStackTrace = defaultPrepareStackTrace;

  // Recapture for normal, formatted stack trace
  Error.captureStackTrace(this, MockError);

  this.key = key;
  this.message = message;
  if (other) {
    for (var i in other) {
      if (other.hasOwnProperty(i)) {
        this[i] = other[i];
      }
    }
  }
}
util.inherits(MockError, Error);
module.exports.MockError = MockError;

function NmvError(message, other) {
  MockError.call(this, 'not_my_vbucket', message, other);
}
util.inherits(NmvError, MockError);
module.exports.NmvError = NmvError;

