"use strict";

var path = require('path');

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
module.exports.N1qlError = N1qlError;