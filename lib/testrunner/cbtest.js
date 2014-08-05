"use strict";

var domain = require('domain');
var annotations = require('annotations');

var MockCluster = require('../mockserver/cluster');
var Harness = require('./harness');

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

function RealCluster() {

}
RealCluster.prototype.prepare = function(callback) {
  process.nextTick(function() {
    callback();
  });
};
RealCluster.prototype.destroy = function(callback) {
  process.nextTick(function() {
    callback();
  });
};
RealCluster.prototype.bootstrapList = function(type) {
  if (type === 'http') {
    return '192.168.7.26:8091';
  } else {
    throw new Error('Unknown bootstrapList type');
  }
};

function runTest(fn, callback) {
  var testServer = null;
  if (0) {
    testServer = new MockCluster();
  } else {
    testServer =  new RealCluster();
  }

  var H = new Harness();
  H.srv = testServer;

  function destroyAndFinish() {
    H.destroy(function() {
      testServer.destroy(function() {
        callback();
      });
    });
  }

  var testDomain = domain.create();
  testDomain.run(function() {
    testServer.prepare(function() {
      fn(H, destroyAndFinish);
    });
  });

  testDomain.on("error", function(error) {
    console.error('TEST ERROR');
    console.error(error.stack);
    destroyAndFinish();
  });
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

      if (!myMod[test.funcName]) {
        return;
      }

      var stime = new Date();
      console.log('Starting test `' + test.name + '`');
      runTest(myMod[test.funcName], function() {
        var etime = new Date();
        console.log('Completed test `' + test.name + '` in ' + (etime-stime) + 'ms');
        executeNextTest();
      });
    }
    executeNextTest();
  });
}

// Import testing stuff to global scope
require('./test');

var TEST_BASE = '../../assets/';
var TESTS = [
  'sdk_tests/demo.js'
];

var testIdx = 0;
function runOneTest() {
  if (testIdx >= TESTS.length) {
    console.log('done running all tests');
    return;
  }
  var testPath = TESTS[testIdx++];

  runTestFile(TEST_BASE + testPath, null, null, function(err) {
    if (err) {
      throw err;
    }

    console.log('done running `' + testPath + '`');

    runOneTest();
  });
}
runOneTest();


