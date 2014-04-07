"use strict";

function ViewIndexer(bucket) {
  this.bucket = bucket;
  this.data = null;
}

function cbCompare(a, b) {
  if (a < b) { return -1; }
  if (a > b) { return +1; }
  return 0;
}

function cbIndexOf(arr, val) {
  for (var i = 0; i < arr.length; ++i) {
    if (cbCompare(arr[i], val) === 0) {
      return i;
    }
  }
  return -1;
}

function cbNormKey(key, groupLevel) {
  if (Array.isArray(key)) {
    if (groupLevel === -1) {
      return key;
    } else if (groupLevel === 0) {
      return null;
    } else {
      return key.slice(0, groupLevel);
    }
  } else {
    return key;
  }
}

function optTryParse(opt) {
  if (opt) {
    return JSON.parse(opt);
  } else {
    return undefined;
  }
}
function optDefault(opt, defaultVal) {
  if (opt) {
    return opt;
  } else {
    return defaultVal;
  }
}

function reduceCount(key, values, rereduce) {
  if (rereduce) {
    var result = 0;
    for (var i = 0; i < values.length; i++) {
      result += values[i];
    }
    return result;
  } else {
    return values.length;
  }
}
function reducerSum(key, values, rereduce) {
  var sum = 0;
  for(var i = 0; i < values.length; i++) {
    sum = sum + values[i];
  }
  return(sum);

}
function reducerStats(key, values, rereduce) {
  return null;
}

var BUILTIN_REDUCERS = {
  '_count': reduceCount,
  '_sum': reducerSum,
  '_stats': reducerStats
};

ViewIndexer.prototype.execute = function(ddoc, name, options, callback) {
  var viewObj = this._getView(ddoc, name);

  //console.log('indexer opts', options);

  var indexSize = this.data.length;

  var startKey = optTryParse(options['startkey']);
  var startKeyDocId = optTryParse(options['startkey_docid']);
  var endKey = optTryParse(options['endkey']);
  var endKeyDocId = optTryParse(options['endkey_docid']);

  var inclusiveStart = true;
  var inclusiveEnd = true;

  if (options['inclusive_end'] !== undefined) {
    if (!options['inclusive_end'] || options['inclusive_end'] === 'false') {
      inclusiveEnd = false;
    }
  }

  if (options['descending']) {
    var _startKey = startKey;
    startKey = endKey;
    endKey = _startKey;
    var _startKeyDocId = startKeyDocId;
    startKeyDocId = endKeyDocId;
    endKeyDocId = _startKeyDocId;
    var _inclusiveStart = inclusiveStart;
    inclusiveStart = inclusiveEnd;
    inclusiveEnd = _inclusiveStart;
  }

  var key = optTryParse(options['key']);
  var keys = optTryParse(options['keys']);

  var results = [];

  for (var i = 0; i < this.data.length; ++i) {
    var item = this.data[i];
    var docKey = item.key;
    var docId = item.id;

    if (key !== undefined) {
      if (cbCompare(docKey, key) !== 0) {
        //console.log('[skip] no key match', docKey, key);
        continue;
      }
    }
    if (keys !== undefined) {
      if (keys.indexOf(docKey) === -1) {
        //console.log('[skip] no keys match', docKey);
        continue;
      }
    }

    if (inclusiveStart) {
      if (startKey && cbCompare(docKey, startKey) < 0) {
        //console.log('[skip] before start key incl', docKey, startKey);
        continue;
      }
      if (startKeyDocId && cbCompare(docId, startKeyDocId) < 0 ) {
        //console.log('[skip] before start id incl', docId, startKeyDocId);
        continue;
      }
    } else {
      if (startKey && cbCompare(docKey, startKey) <= 0) {
        //console.log('[skip] before start key', docKey, startKey);
        continue;
      }
      if (startKeyDocId && cbCompare(docId, startKeyDocId) <= 0) {
        //console.log('[skip] before start id', docId, startKeyDocId);
        continue;
      }
    }

    if (inclusiveEnd) {
      if (endKey && cbCompare(docKey, endKey) > 0) {
        //console.log('[skip] after start key incl', docKey, endKey);
        continue;
      }
      if (endKeyDocId && cbCompare(docId, endKeyDocId) > 0) {
        //console.log('[skip] after start id incl', docId, endKeyDocId);
        continue;
      }
    } else {
      if (endKey && cbCompare(docKey, endKey) >= 0) {
        //console.log('[skip] after start key', docKey, endKey);
        continue;
      }
      if (endKeyDocId && cbCompare(docId, endKeyDocId) >= 0) {
        //console.log('[skip] after start id', docId, endKeyDocId);
        continue;
      }
    }

    /*
{
   "id":"TEST-10968-incrdecr336",
   "key":"TEST-10968-incrdecr336",
   "value":{
      "doc":490,
      "meta":{
         "id":"TEST-10968-incrdecr336",
         "rev":"12-00000447ad27eafd0000000000000000",
         "expiration":0,
         "flags":0,
         "type":"json"
      }
   },
   "doc":{
      "meta":{
         "id":"TEST-10968-incrdecr336",
         "rev":"12-00000447ad27eafd0000000000000000",
         "expiration":0,
         "flags":0
      },
      "json":490
   }
}
     */

    //console.log('key passed', docKey, docId);

    var itemOut = {
      id: item.id,
      key: item.key
    };
    if (item.value !== undefined) {
      itemOut.value = item.value;
    }

    if (options['include_docs']) {
      itemOut.doc = item.doc;
    }

    results.push(itemOut);
  }

  if (options['descending']) {
    results.sort(function(a,b){
      if (a.key > b.key) { return -1; }
      if (a.key < b.key) { return +1; }
      return 0;
    });
  } else {
    results.sort(function(a,b){
      if (b.key > a.key) { return -1; }
      if (b.key < a.key) { return +1; }
      return 0;
    });
  }

  /* REDUCER */
  var doReduce = true;
  if (options['reduce'] !== undefined) {
    if (!options['reduce'] || options['reduce'] === 'false') {
      doReduce = false;
    }
  }

  var groupLevel = optDefault(options['group_level'], 0);
  if (options['group'] === 'true') {
    // `group=true` sets group_level to infinite
    groupLevel = -1;
  }
  //console.log('REDUCER GROUPLEVEL', groupLevel);

  var viewReduceFunc = viewObj.reduce;
  if (doReduce && viewReduceFunc !== undefined) {
    //console.log('VIEW PRE REDUCE', results);

    var reduceOnce = function(key, values, rereduce){};
    if (BUILTIN_REDUCERS[viewReduceFunc]) {
      reduceOnce = BUILTIN_REDUCERS[viewReduceFunc];
    } else {
      eval('reduceOnce = ' + viewReduceFunc);
    }

    var keys = [];
    for (var i = 0; i < results.length; ++i) {
      var keyN = cbNormKey(results[i].key, groupLevel);
      if (cbIndexOf(keys, keyN) < 0) {
        keys.push(keyN);
      }
    }

    var reducedResults = [];

    for (var j = 0; j < keys.length; ++j) {
      var values = [];
      for (var k = 0; k < results.length; ++k) {
        var keyN = cbNormKey(results[k].key, groupLevel);
        if (cbCompare(keyN, keys[j]) === 0) {
          values.push(results[k].value);
        }
      }
      var result = reduceOnce(keys[j], values, false);
      reducedResults.push({
        key: keys[j],
        value: result
      });
    }

    results = reducedResults;
  }

  /* FINALIZE */
  if (options['skip']) {
    results = results.slice(options['skip']);
  }
  if (options['limit']) {
    results = results.slice(0, options['limit']);
  }

  //console.log('VIEW RESULT', results);

  callback(null, results, indexSize);
};

ViewIndexer.prototype._getView = function(ddoc, name) {
  var ddocObj = this.bucket.ddocs[ddoc];
  if (!ddocObj) {
    throw new Error('not_found');
  }
  if (ddocObj.views) {
    ddocObj = ddocObj.views;
  }

  var viewObj = ddocObj[name];
  if (!viewObj) {
    throw new Error('not_found');
  }

  return viewObj;
};

ViewIndexer.prototype.index = function(ddoc, name, callback) {
  var viewObj = this._getView(ddoc, name);
  var viewMapFunc = viewObj.map;

  //console.log(viewObj);

  this.data = [];

  var curdocjson = null;
  var curdockey = null;
  var self = this;
  function emit(key, val) {
    self.data.push({
      key: key,
      id: curdockey,
      value: val,
      doc: {
        json: curdocjson,
        meta: {
          id: curdockey
        }
      }});
  }

  var procOne = function(doc,meta){};
  eval('procOne = ' + viewMapFunc);

  this.bucket.forEachKey(0, function(key, vbId, doc) {
    // Handle possibly non-UTF8 keys here...
    var docKey = new Buffer(key, 'base64').toString('utf8');
    var docString = doc.value.toString('utf8');

    var jsonValue = null;
    if (!jsonValue) {
      try {
        jsonValue = JSON.parse(docString);
      } catch (e) {
      }
    }
    if (!jsonValue) {
      try {
        jsonValue = docString;
      } catch(e) {
      }
    }

    if (!jsonValue) {
      // TODO: This could be wrong, Matt says yes, empirical evidence another.
      // We don't index non-json data...
      return;
    }

    curdockey = docKey;
    curdocjson = jsonValue;
    procOne(jsonValue, {id: docKey});
  });

  if (callback) {
    callback();
  }
};

module.exports = ViewIndexer;
