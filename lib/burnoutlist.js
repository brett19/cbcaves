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

BurnoutList.prototype.set = function(values) {
  this.values = [];
  this.burnedValues = [];

  if (values) {
    for (var i = 0; i < values.length; ++i) {
      this.values.push(values[i]);
    }
  }
};

BurnoutList.prototype.poll = function() {
  if (this.values.length === 0) {
    return null;
  }

  var value = this.values.shift();
  this.burnedValues.push(value);

  var self = this;
  setTimeout(function() {
    if (self.burnedValues.length > 0) {
      var unburnValue = self.burnedValues.shift();
      self.values.push(unburnValue);
    }
  }, this.burnTime);

  return value;
};

module.exports = BurnoutList;
