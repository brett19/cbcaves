var CouchbaseClient = require('./lib/couchbase').Connection;

require('buffer').INSPECT_MAX_BYTES = 100;

var tst = new CouchbaseClient({
  uri: ['localhost:8091'],
  bucket: 'default'
});

/*
tst.incr('testkeyi', {initial: 33, offset: 2}, function(err, res) {
  console.log('tst.arithmetic', err, res);

  tst.decr('testkeyj', {initial: 33, offset: 2}, function(err, res) {
    console.log('tst.arithmetic', err, res);

  });
});
//*/

//*
tst.get('testkeya', function(err, res) {
  console.log('tst.get', err, res);

  tst._config.markInvalid();
});
//*/

/*
tst.test('testkeya', {}, function(err, res) {
  console.log('tst.test', err, res);
});
//*/

/*/
tst.set('testkeya', 'franklyn', function(err, res) {
  console.log('tst.set', err, res);

  tst.getMulti(['testkeya', 'testkeyb'], function(err, res) {
    console.log('tst.getMulti:1', err, res);
  });

  tst.getMulti({'testkeya':{}, 'testkeyb': {}}, function(err, res) {
    console.log('tst.getMulti:2', err, res);
  });
});
//*/

/*
tst.set('testkeya', 'franklyn', function(err, res) {
  console.log('tst.set', err, res);

  tst.get('testkeya', function(err, res) {
    console.log('tst.get', err, res);

    tst.remove('testkeya', function(err, res) {
      console.log('tst.remove', err, res);

      tst.get('testkeya', function(err, res) {
        console.log('tst.get', err, res);


      });
    });
  });
});
//*/
