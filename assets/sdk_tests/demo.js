"use strict";

var assert = require('assert');

/**
 * @test basic add tests
 */
exports.basicAdd = function(H, done) {
  var httpHosts = H.srv.bootstrapList('http');
  var cli = H.newClient({
    hosts: httpHosts
  });

  var testKey = H.genKey('add');

  cli.add(testKey, 'bar', H.okCallback(function() {
    done();
  }));
};

/**
 * @test secondary add tests
 */
exports.addWorks = function(H, done) {
  var httpHosts = H.srv.bootstrapList('http');
  var cli = H.newClient({
    hosts: httpHosts
  });

  var testKey = H.genKey('add');

  H.setKey(testKey, 'bar', function() {
    cli.add(testKey, 'baz', function(err) {
      assert(err, 'Should fail to add object second time.');
      done();
    });
  });
};

/**
 * @test binary key add tests
 * @needs binary_key
 */
exports.bkeyAdd = function(H, done) {
  var httpHosts = H.srv.bootstrapList('http');
  var cli = H.newClient({
    hosts: httpHosts
  });

  var testKey = H.genBKey(32);

  H.setKey(testKey, 'bar', function() {
    cli.add(testKey, 'baz', function (err, res) {
      // TODO: This test should check the error return code properly.
      //   It currently fails properly, but with the incorrect error code.
      assert(err, 'Should fail to add object second time.');
      done();
    });
  });
};
