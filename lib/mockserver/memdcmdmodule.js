'use strict';

function MemdCmdModule() {
  this.handlers = {};
}

MemdCmdModule.prototype.inherit = function(cmdmod) {
  for (var i in cmdmod.handlers) {
    if (cmdmod.handlers.hasOwnProperty(i)) {
      this.handlers[i] = cmdmod.handlers[i];
    }
  }
};

MemdCmdModule.prototype.init = function(svc) {
  function registerOneHandler(op, handler) {
    var wrapperHandler = function(socket, req, next) {
      if (!req.isRequest(op)) {
        return next();
      }
      return handler.call(this, socket, req, next);
    };
    svc.registerHandler(wrapperHandler);
  }
  for (var i in this.handlers) {
    if (this.handlers.hasOwnProperty(i)) {
      registerOneHandler(parseInt(i), this.handlers[i]);
    }
  }
};

MemdCmdModule.prototype.reqOp = function(op, handler) {
  this.handlers[op] = handler;
};

module.exports = MemdCmdModule;
