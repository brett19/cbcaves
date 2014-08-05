'use strict';

var memdproto = require('../memdproto');
var MemdCmdModule = require('./memdcmdmodule');

var mod = new MemdCmdModule();

mod.reqOp(memdproto.cmd.HELLO, function(socket, req) {
  // Lets whine quietly since this should not ever be sent yet...
  console.warn('Received HELLO from non-compliant client');

  socket.writeReply(req, { status: memdproto.status.UNKNOWN_COMMAND });
});

module.exports = mod;
