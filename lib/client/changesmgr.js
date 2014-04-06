"use strict";

function ChangeStreamMgr(parent) {
  this.parent = parent;

  this.parent.on('newConfig', function() {

  });
}

ChangeStreamMgr.prototype._start = function() {

};

ChangeStreamMgr.prototype._stop = function() {

};