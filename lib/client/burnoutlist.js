"use strict";

/*
A basic list that causes items to 'burn out' after being polled from the list.
Once an item is burned out, it will not be returned by poll until its burnout
period has expired and it becomes available again.  Polling with no values
available will return a null value.  Items polled from the list are returned
from the front, while items that are unburned will be pushed to the back of
the list, creating a sort of time delayed circular queue.
 */

function BurnoutList(burnTime, initial) {
  this.burnTime = burnTime;
  this.set(initial);
}

BurnoutList.prototype._ts = function() {
  return (new Date()).getTime();
};

BurnoutList.prototype.set = function(values) {
  this.values = [];
  this.burnedValues = [];

  if (values) {
    for (var i = 0; i < values.length; ++i) {
      this.values.push(values[i]);
    }
  }
};

BurnoutList.prototype._checkUnburn = function() {
  var newBurnedValues = [];
  var curTs = this._ts();
  for (var i = 0; i < this.burnedValues.length; ++i) {
    var burnVal = this.burnedValues[i];
    if (burnVal[0] <= curTs) {
      this.values.push(burnVal[1]);
    } else {
      newBurnedValues.push(burnVal);
    }
  }
  this.burnedValues = newBurnedValues;
};

BurnoutList.prototype.poll = function() {
  this._checkUnburn();

  if (this.values.length === 0) {
    return null;
  }

  var value = this.values.shift();
  this.burnedValues.push([
    this._ts() + this.burnTime,
    value
  ]);

  return value;
};

module.exports = BurnoutList;
