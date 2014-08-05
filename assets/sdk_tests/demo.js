"use strict";

var assert = require('assert');
var Cas = require('../../lib/mockserver/cas');

/**
 * @test Get missing key
 */
exports.getMissing = function(H, done) {
  var cli = H.newClient();

  var testKey = H.genKey('getMissing');
  cli.get(testKey, function(err) {
    assert(err.code === 5);
    done();
  });
};

/**
 * @test Replace missing key
 */
exports.replaceMissing = function(H, done) {
  var cli = H.newClient();

  var testKey = H.genKey('replaceMissing');
  cli.replace(testKey, 'someval', {}, function(err) {
    assert(err.code === 5, 'should fail with key not found error');
    done();
  });
};

/**
 * @test Replace key with bad cas
 */
exports.replaceBadCas = function(H, done) {
  var cli = H.newClient();

  var testKey = H.genKey('replaceBadCas');
  cli.set(testKey, 'some value', {}, function(err, res) {
    cli.replace(testKey, 'something', {cas: [res.cas[0]+1, res.cas[1]]}, function(errs) {
      assert(errs.code === 4, 'should fail with key already exists error');
      done();
    });
  });
};

/**
 * @test Replace missing key with a cas
 */
exports.replaceMissingCas = function(H, done) {
  var cli = H.newClient();

  var testKey = H.genKey('replaceMissingCas');
  cli.replace(testKey, 'something', {cas: [1, 1]}, function(errs) {
    assert(errs.code === 5, 'should fail with key not found error');
    done();
  });
};

/**
 * @test Replace key with matching CAS
 */
exports.replaceWithCas = function(H, done) {
  var cli = H.newClient();

  var testKey = H.genKey('replaceBadCas');
  cli.set(testKey, 'some value', {}, function(err, res) {
    cli.replace(testKey, 'something', {cas: res.cas}, function(errs) {
      assert(!errs, 'should succeed');
      done();
    });
  });
};

/**
 * @test basic add tests
 */
/*
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
*/

/**
 * @test secondary add tests
 */
/*
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
*/

/**
 * @test binary key add tests
 * @needs binary_key
 */
/*
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
*/
