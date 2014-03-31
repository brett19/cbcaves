"use strict";

var util = require('util');

var HttpService = require('./httpsvc');

function CapiService(parent, parentNode) {
  HttpService.call(this, parent, parentNode);
}
util.inherits(CapiService, HttpService);

CapiService.prototype._setupRoutes = function(app) {
  app.get('/:bucket/_design/:name', this._handleGetDesignDoc.bind(this));
  app.put('/:bucket/_design/:name', this._handleSetDesignDoc.bind(this));
  app.del('/:bucket/_design/:name', this._handleDelDesignDoc.bind(this));
};

CapiService.prototype._handleGetDesignDoc = function(req, res, next) {
  console.log('GET DESIGN DOC');

  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    return res.send(404);
  }

  res.send(200, {});
};

CapiService.prototype._handleSetDesignDoc = function(req, res, next) {
  console.log('SET DESIGN DOC');

  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    return res.send(404);
  }

  res.send(200, {});
};

CapiService.prototype._handleDelDesignDoc = function(req, res, next) {
  console.log('DELETE DESIGN DOC');

  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    return res.send(404);
  }


  res.send(200, {});
};

module.exports = CapiService;
