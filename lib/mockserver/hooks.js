"use strict";

function call(self, func) {
  var hookArgs = [];
  var callArgs = [];
  for (var i = 2; i < arguments.length; ++i) {
    hookArgs.push(arguments[i]);
    callArgs.push(arguments[i]);
  }
  hookArgs.push(function hookDone() {});
  func.apply(self, callArgs);
}

function ensureHooks(func) {
  if (!func._beforeHooks) {
    func._beforeHooks = [];
  }
  if (!func._afterHooks) {
    func._afterHooks = [];
  }
}
function hookBefore(func, hook) {
  ensureHooks(func);
  func._beforeHooks.push(hook);
}
function unhookBefore(func, hook) {
  ensureHooks(func);
}

function hookAfter(func, hook) {
  ensureHooks(func);
  func._afterHooks.push(hook);
}
function unhookAfter(func, hook) {
  ensureHooks(func);
}

module.exports = {
  call: call,
  hookBefore: hookBefore,
  unhookBefore: unhookBefore,
  hookAfter: hookAfter,
  unhookAfter: unhookAfter
};
