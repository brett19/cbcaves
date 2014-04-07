"use strict";

var util = require('util');

var HttpService = require('./httpsvc');
var ViewIndexer = require('./viewidxr');

function CapiService(parent, parentNode) {
  HttpService.call(this, parent, parentNode);
}
util.inherits(CapiService, HttpService);

CapiService.prototype._setupRoutes = function(app) {
  app.get('/:bucket/_design/:ddoc', this._handleGetDesignDoc.bind(this));
  app.put('/:bucket/_design/:ddoc', this._handleSetDesignDoc.bind(this));
  app.del('/:bucket/_design/:ddoc', this._handleDelDesignDoc.bind(this));
  app.get('/:bucket/_design/:ddoc/_view/:view', this._handleView.bind(this));
};

CapiService.prototype._handleGetDesignDoc = function(req, res, next) {
  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    // Bucket not found
    return res.send(404, {
      error: 'not_found',
      reason: 'missing'
    });
  }

  var ddoc = bucket.ddocs[req.params.ddoc];
  if (!ddoc) {
    return res.send(404, {
      error: 'not_found',
      reason: 'Design document _design/' + req.params.ddoc + ' not found'
    });
  }

  //console.log('GET DESIGN DOC', ddoc);

  res.send(ddoc);
};

CapiService.prototype._handleSetDesignDoc = function(req, res, next) {
  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    // Bucket not found
    return res.send(404, {
      error: 'not_found',
      reason: 'missing'
    });
  }

  //console.log('SET DESIGN DOC', req.body);

  bucket.ddocs[req.params.ddoc] = req.body;
  //console.log('SUCCESS');

  res.send(201, {
    ok: true,
    id: '_design/' + req.params.ddoc
  });
};

CapiService.prototype._handleDelDesignDoc = function(req, res, next) {
  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    // Bucket not found
    return res.send(404, {
      error: 'not_found',
      reason: 'missing'
    });
  }

  //console.log('DELETE DESIGN DOC', req.params.ddoc);

  delete bucket.ddocs[req.params.ddoc];

  res.send(200, {
    ok: true,
    id: '_design/' + req.params.ddoc
  });
};

CapiService.prototype._handleView = function(req, res, next) {
  var bucket = this.parent.bucketByName(req.params.bucket);
  if (!bucket) {
    // Bucket not found
    return res.send(404, {
      error: 'not_found',
      reason: 'missing'
    });
  }

  //console.log('RUN VIEW', req.params.ddoc, req.params.view, req.query, req.body);

  try {
    var idxr = new ViewIndexer(bucket);
    idxr.index(req.params.ddoc, req.params.view);
    idxr.execute(req.params.ddoc, req.params.view, req.query, function(err, rows, indexSize) {
      //console.log('VIEW EXECUTED', indexSize, rows);

      res.send(200, {
        total_rows: indexSize,
        rows: rows
      });
    });
  } catch(e) {
    //console.log('VIEW ERROR', e);
    //console.log(e.stack);
    return res.send(500, e);
  }
};

module.exports = CapiService;
