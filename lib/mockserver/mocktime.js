'use strict';

function MockTime() {
  this.offset = 0;
  this.timers = [];
  this.timerIdx = 1;
  this.activeTimer = null;
}

MockTime.prototype.timeTravel = function(amountInMs) {
  this.offset += amountInMs;
};

MockTime.prototype.curMs = function() {
  return (new Date()).getTime() + this.offset;
};

MockTime.prototype._reschedule = function() {
  if (this.activeTimer) {
    clearTimeout(this.activeTimer);
    this.activeTimer = null;
  }
  var nextTimerEnd = 0;
  var callbackNow = [];
  for (var i = 0; i < this.timers.length; ++i) {
    var timer = this.timers[i];
    if (timer[1] <= this.curMs()) {
      callbackNow.push(timer[2]);
      this.timers.splice(i, 1);
      i--;
      return;
    }
    if (nextTimerEnd === 0 || nextTimerEnd > timer[1]) {
      nextTimerEnd = timer[1];
    }
  }
  if (nextTimerEnd !== 0) {
    this.activeTimer = setTimeout(this._reschedule.bind(this), nextTimerEnd);
  }
  for (var j = 0; j < callbackNow.length; ++j) {
    callbackNow[j]();
  }
};

MockTime.prototype.newTimeout = function(interval, handler) {
  var timerId = this.timerIdx++;
  var absTimeout = this.curMs() + interval;
  this.timers.push([timerId, absTimeout, handler]);
  this._reschedule();
};

MockTime.prototype.cancelTimeout = function(timeoutId) {
  for (var i = 0; i < this.timers.length; ++i) {
    if (this.timers[i][0] === timeoutId) {
      this.timers.splice(i, 1);
      this._reschedule();
      return true;
    }
  }
  return false;
};

module.exports = MockTime;
