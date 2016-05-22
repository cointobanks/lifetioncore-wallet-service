var BWS = {};

BWS.Server = require('./lib/server');
BWS.Storage = require('./lib/storage');
BWS.MessageBroker = require('./lib/messagebroker');
BWS.BlockchainExplorer = require('./lib/blockchainexplorer');
BWS.FiatRateService = require('./lib/fiatrateservice');

module.exports = BWS;