'use strict';

function HtModule() {
  this.getHandlers = [];
  this.postHandlers = [];
  this.putHandlers = [];
  this.delHandlers = [];
}

var _htOp = function(list, argsIn) {
  var args = [];
  for (var i = 0; i < argsIn.length; ++i) {
    args.push(argsIn[i]);
  }
  list.push(args);
};

HtModule.prototype.htGet = function(path, handler) {
  _htOp.call(this, this.getHandlers, arguments);
};
HtModule.prototype.htPost = function(path, handler) {
  _htOp.call(this, this.postHandlers, arguments);
};
HtModule.prototype.htPut = function(path, handler) {
  _htOp.call(this, this.putHandlers, arguments);
};
HtModule.prototype.htDel = function(path, handler) {
  _htOp.call(this, this.delHandlers, arguments);
};

module.exports = HtModule;