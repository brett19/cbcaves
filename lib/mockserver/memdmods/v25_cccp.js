'use strict';

var memdproto = require('../memdproto');
var MemdCmdModule = require('../memdcmdmodule');

var mod = new MemdCmdModule();

mod.reqOp(memdproto.cmd.GET_CLUSTER_CONFIG, function(socket, req) {
  var bucket = this._getSocketBucket(socket);
  if (!bucket) {
    return socket.writeReply(req, { status: memdproto.status.ETMPFAIL });
  }

  var config = this.parent._generateBucketConfig(bucket);
  socket.writeReply(req, {
    value: new Buffer(JSON.stringify(config), 'utf8')
  });
});

module.exports = mod;
