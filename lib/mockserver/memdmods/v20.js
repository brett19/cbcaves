'use strict';

var MemdCmdModule = require('../memdcmdmodule');

var mod = new MemdCmdModule();

mod.inherit(require('./v20_auth'));
mod.inherit(require('./v20_crud'));

module.exports = mod;
