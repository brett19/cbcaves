'use strict';

var MemdCmdModule = require('../memdcmdmodule');

var mod = new MemdCmdModule();

mod.inherit(require('./v20_auth'));
mod.inherit(require('./v20_crud'));
mod.inherit(require('./v25_cccp'));
mod.inherit(require('./v30_dcp'));
mod.inherit(require('./v30_hello'));

module.exports = mod;
