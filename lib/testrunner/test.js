"use strict";

global.describe = function(name, needs, func) {
  console.log('DESCRIBE ' + name);
  func(function() {});
};

global.it = function(name, func) {
  console.log('IT ' + name);
  func(function() {});
};

function needsOne(cap) {
  console.log('NEEDS ' + cap);
}
global.needs = function() {
  for (var i = 0; i < arguments.length; ++i) {
    needsOne(arguments[i]);
  }
};
