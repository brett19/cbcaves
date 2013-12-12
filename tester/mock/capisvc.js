"use strict";

var util = require('util');

var HttpService = require('./httpsvc');

function CapiService(parent) {
  HttpService.call(this, parent);
}
util.inherits(CapiService, HttpService);

CapiService.prototype._setupRoutes = function(app) {

};

module.exports = CapiService;
