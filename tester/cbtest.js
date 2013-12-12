"use strict";

var annotations = require('annotations');

var MockCluster = require('./mock/cluster');

function parseTests(path, callback) {
  annotations.get(path, function(err, result) {
    if (err) {
      return callback(new Error('could not parse test annotations'), null);
    }

    var tests = [];
    for (var i in result) {
      if (result.hasOwnProperty(i)) {
        var testSpec = result[i];
        if (!testSpec.test) {
          continue;
        }

        var test = {};
        test.funcName = i;
        test.name = testSpec.test;

        test.needs = [];
        if (testSpec.needs instanceof String) {
          test.needs.push(testSpec.needs);
        } else if (Array.isArray(testSpec.needs)) {
          for (var j = 0; j < testSpec.needs.length; ++j) {
            test.needs.push(testSpec.needs[j]);
          }
        }

        tests.push(test);
      }
    }

    callback(null, tests);
  });
}

function runTest(fn, callback) {
  var mockServer = new MockCluster();
  mockServer.prepare(function() {
    fn(mockServer, callback);
  });
  mockServer.destroy();
}

function runTestFile(path, cli, srv, callback) {
  var filePath = require.resolve(path);

  parseTests(filePath, function(err, tests) {
    if (err) {
      return callback(err);
    }

    delete require.cache[filePath];
    var myMod = require(filePath);

    function executeNextTest() {
      if (tests.length === 0) {
        return callback(null);
      }

      var test = tests.shift();
      runTest(myMod[test.funcName], executeNextTest);
    }
    executeNextTest();
  });
}

// Import testing stuff to global scope
require('./test');

runTestFile('./test/demo.js', null, null, function(err) {
  if (err) {
    throw err;
  }

  console.log('done running ./test/demo.js');
});

