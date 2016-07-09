'use strict';

var $ = require('preconditions').singleton();
var _ = require('lodash');
var async = require('async');
var log = require('npmlog');
log.debug = log.verbose;

var Bitcore = require('bitcore-lib-dash');
var BufferUtil = Bitcore.util.buffer;

/* webcoin */
var Chain = require('blockchain-spv');
var params = require('webcoin-bitcoin');
var levelup = require('levelup');
var u = require('bitcoin-util');

/* bitcore-wallet-service */
var BlockchainExplorer = require('./blockchainexplorer');
var MessageBroker = require('./messagebroker');

var util = require('util');
var TransactionStream = require('stream').Transform;
var where = require("lodash.where");


function Blockchain(opts) {
    var self = this;
    opts = opts || {};
    if (!opts.userAgent) throw new Error('Must provide userAgent');
    this.userAgent = opts.userAgent;

    this.db = levelup('dash.chain', { db: require('memdown') });
    this.txStream = new TransactionStream( { objectMode: true } );

    this.syncing = false;
    this.synced = false;
    this.initialized = false;

    this.txStack= [];
}

/***
 * Initializes the blockchain, requires that at least one transaction exist in txStack array.
 *
 * @param opts
 * @param cb
 * @returns {*}
 * @private
 */
Blockchain.prototype._init = function(opts, cb) {
    var self = this;
    opts = opts || {};

    if (this.txStack.length < 1) {
        return cb('err: txStack length', null);
    }

    // identify nearest checkpoint
    var min = Math.min.apply(Math,self.txStack.map(function(o){return o.blockheight;})); // minimum required block height
    params.blockchain.genesisHeader = params.blockchain.checkpoints[0]; // lowest checkpoint

    for (var i = 0, len = params.blockchain.checkpoints.length; i < len; i++) {
        if (params.blockchain.checkpoints[i].height <= min) {
            params.blockchain.genesisHeader = params.blockchain.checkpoints[i]; // identify best checkpoint
        }
    }

    // create chain
    console.log('-initializing chain');
    var chain = self.chain = new Chain(params.blockchain, self.db, { ignoreCheckpoints: true }); // TODO: get rid of checkpoints
    var chainReadStream = self.chainReadStream = chain.createReadStream();
    var chainWriteStream = self.chainWriteStream = chain.createWriteStream();

    this.initialized = true;

    chainReadStream.on('data', function(data) { // sync by new block notification

        if ((data.height % 2000) === 0) {
            console.log(data.height);
        }

        if ((self.txStack[0]) && (data.height == self.txStack[0].blockheight)) { // check txStack to identify if we have a process
            console.log(self.txStack[0]);
            var tx = self.txStack[0];
            self._spv(tx, function(err, txStatus, merkleRootStatus) { // verify transaction
                console.log("-transaction verified from chainReadStream.");
            });
        }
    });

    self.txStream.on('data', function(data) { // sync by
        var self = this;

        if ((data) && (this.chain) && (this.chain.getTip().blockHeight > data.blockheight)) {
            console.log(self.txStack[0]);
            var tx = self.txStack[0];
            self._spv(tx, function(err, txStatus, merkleRootStatus) { // verify transaction
                console.log("-transaction verified from txStream.");
            });
        }
    });

    var syncOpts = {
        start: null,
        stop: null
    };

    this.sync(null); // pass stop, start to sync (optional)

    cb(null, '-initializing sync');
}

/***
 * Parses wallet-service transaction response, adds to txStack
 *
 * @param messages
 * @param opts
 * @param cb
 */
Blockchain.prototype.parse = function(messages, opts, cb) {
    var self = this;
    var messages = messages;
    var opts = opts;

    for (var i = 0, len = messages.length; i < len; i++) {
        this.txStack.push(messages[i]);
    }

    this.txStack.sort(function(a,b) {return (a.blockheight > b.blockheight) ? 1 : ((b.blockheight > a.blockheight) ? -1 : 0);} );
    self.txStream.push(messages); // push sorted txStack to txStream for immediate processing attempt

    if(!this.initialized) {
        this._init(opts, function(err, result) {
            if (err) console.log(err);
            cb(err, result);
        });
    } else {
        cb(null, 'transaction processed.');
    }
}

/***
 * Perform blockchain sync
 *
 * @param {Object} opts
 * @param {string} opts.network -
 * @param {string} opts.start -
 * @param {string} opts.stop -
 */
Blockchain.prototype.sync = function(opts) {
    var self = this;
    var sync = true;
    var chainWriteStream = self.chainWriteStream;

    opts = opts || {};

    this.start = opts.start || BufferUtil.bufferToHex(BufferUtil.reverse(this.chain.getTip().hash));
    this.stop = opts.stop || null; // TODO: get current network height if no value is provided

    this._getHeaders(self.network, this.start, function(err, rpcHeader) {
        if (err) console.log(err);
        var blockHeaders = [];

        var rpcHeader = rpcHeader;
        var blockHeader = rpcHeader.shift();
        var nextHash = blockHeader.nextHash;
        var blockheight = blockHeader.height;

        console.log(' starting sync at block hash ' + self.start);

        async.whilst(
            function() {
                if (sync === false) return false;
                return true;
            },
            function (cb) {
                var status = self.chain.adding;
                if (nextHash) {
                    self._getHeaders(self.network, nextHash, function(err, headers) {

                        var blockHeader = headers[(headers.length-1)];
                        nextHash = blockHeader.nextHash;
                        blockheight = blockheight + headers.length;

                        self._transformHeaders(headers, function(err, result) {
                            if (err) console.log("error");

                            chainWriteStream.write(result); // write block headers to chain
                            cb(null, nextHash);
                        });

                    });
                } else {
                    sync = false; // end of chain
                    cb(null, nextHash);
                }
            },
            function (err) {
                if (err) console.log(err);
                // block headers have finished downloading...

            }
        );

    });

};

/***
 * DSPV Method for transaction verification.
 *
 * @param transaction
 * @param cb
 * @private
 */
Blockchain.prototype._verify = function(transaction, cb) {
    var self = this;

    async.waterfall(
        [
            function(cb) { // get filtered block transaction hashes
                self._getFilteredBlockTxs(self.network, transaction.blockhash, transaction.txid, function(err, blocktxs) {
                    cb(null, blocktxs, transaction.txid);
                });
            },
            function(blocktxs, tx, cb) {
                var txs = [];

                // iterate through blocktxs to find our transaction and push it into txs array
                // j+i and j+i2 are both added to allow for complete construction of Merkle Leaf
                var j = 0;
                for (var size = blocktxs.txs.length; size > 1; size = Math.floor((size + 1) / 2)) {
                    for (var i = 0; i < size; i += 2) {
                        var i2 = Math.min(i + 1, size - 1);

                        if ((i+j < blocktxs.txs.length) && (blocktxs.txs[j+i].txid == tx || blocktxs.txs[j+i2].txid == tx)) {
                            if (blocktxs.txs[j+i]) {
                                txs.push({
                                    index: (j+i),
                                    txid: blocktxs.txs[j+i].txid
                                });
                            }

                            if (blocktxs.txs[j+i2]) {
                                txs.push({
                                    index: (j+i2),
                                    txid: blocktxs.txs[j+i2].txid
                                });
                            }
                        }

                    }
                    j += size;
                }

                // add first item in blocktxs if it doesn't exist
                if (txs[0].txid != blocktxs.txs[0].txid) txs.unshift({
                    index: 0,
                    txid: blocktxs.txs[0].txid
                });

                // add last item in blocktxs if it doesn't exist
                if (txs[(txs.length - 1)].txid != blocktxs.txs[(blocktxs.txs.length - 1)].txid) txs.push({
                    index: (blocktxs.txs.length - 1),
                    txid: blocktxs.txs[(blocktxs.txs.length - 1)].txid
                });

                // extend txs if our merkle tree is extended with a duplicate hash
                if (txs[(txs.length - 1)].index == txs[(txs.length - 2)].index) txs[(txs.length - 1)].index = txs[(txs.length - 1)].index + 1;

                cb(null, blocktxs, txs, tx);
            },
            function(blocktxs, txs, tx, cb) {
                var blocktxs = blocktxs;

                // retrieve txs from insight-api and insert into blocktxs using index
                async.map(txs,
                    function(merkletreetx, cb) {
                        var self = this;
                        this._getRawTx(this.network, merkletreetx.txid, function(err, result) {
                            var indexedResult = {
                                index: merkletreetx.index,
                                txid: Bitcore.Transaction().fromBuffer(new Buffer(result.rawtx, 'hex')).toObject().hash,
                                transaction: Bitcore.Transaction().fromBuffer(new Buffer(result.rawtx, 'hex')),
                                status: 'unverified'
                            }

                            // push transaction back to blocktxs
                            if (indexedResult.index > blocktxs.txs.length-1) {
                                blocktxs.txs.push({
                                    txid: indexedResult.txid
                                });
                            } else {
                                blocktxs.txs[indexedResult.index].txid = indexedResult.txid;
                            }

                            cb(null, indexedResult);
                        });
                    }.bind(self),
                    function (err, indexedResult) {

                        // perform transaction verification here
                        var filtered = where(indexedResult, { txid: transaction.txid });
                        async.each(filtered,
                            function(filteredResult, cb){
                                self._verifyTransaction(transaction, filteredResult, function(err, result) {
                                    filteredResult.status = result;
                                    cb();
                                });
                            },
                            function(err){
                                if (err) return cb(err);

                            }
                        );
                        console.log(filtered);
                        cb(null, blocktxs, filtered);
                    }
                );
            }
        ],
        function (err, blocktxs, filtered) {
            if (err) cb(err); // TODO: error handling

            // build merkle tree
            var tree = [];

            async.each(blocktxs.txs,
                function(tx, cb){
                    tree.push(BufferUtil.reverse(BufferUtil.hexToBuffer(tx.txid)));
                    cb();
                },
                function(err){
                    var merkleRootStatus = 'unverified';

                    var j = 0;
                    for (var size = tree.length; size > 1; size = Math.floor((size + 1) / 2)) {
                        for (var i = 0; i < size; i += 2) {
                            var i2 = Math.min(i + 1, size - 1);
                            var buf = Buffer.concat([tree[j + i], tree[j + i2]]);
                            tree.push(Bitcore.crypto.Hash.sha256sha256(buf));
                        }
                        j += size;
                    }

                    var merkleRoot = BufferUtil.bufferToHex(BufferUtil.reverse(tree[tree.length - 1]));

                    // get block hash for transaction
                    var hash = BufferUtil.reverse(BufferUtil.hexToBuffer(transaction.blockhash));

                    // get block from local blockchain
                    self.chain.getBlock(hash, function(err, result) {
                        if (err) {
                            merkleRootStatus = '(err) BlockHeader not found';

                            cb(null, filtered, merkleRootStatus);
                        }
                        if (result) {
                            var blockHeader = result.header.toObject();
                            if (merkleRoot == blockHeader.merkleRoot) { // compare calculated Merkle Root to BlockHeader Merkle Root
                                merkleRootStatus = 'verified';
                                cb(null, filtered, merkleRootStatus);
                            } else {
                                merkleRootStatus = 'merkleRoot mismatch';
                                cb(null, filtered, merkleRootStatus);
                            }
                        }
                    });
                }
            );
        }
    );

};

/***
 * Wrapper to _verify function for verification broadcasting
 *
 * @param tx
 * @param cb
 * @private
 */
Blockchain.prototype._spv = function(tx, cb) {
    var self = this;

    // verify transaction
    this._verify(tx, function(err, txStatus, merkleRootStatus) {

        // transaction verified
        // if (tx) console.log(tx.outputs);

        for(var i = 0; i < txStatus.length; i++) {
            console.log("transaction: " + txStatus[i].status);
        }
        console.log("merkleRoot: " + merkleRootStatus); // return verification status

        // TODO: signal to frontend or something?

        self.txStack.shift();
        cb(err, txStatus, merkleRootStatus);
    });
}

/***
 * Verifies that transaction reported by wallet-service matches DSPV transaction.
 *
 * @param transactionMessage: transaction reported by wallet-service
 * @param filteredResult: transaction reported by DSPV method
 * @param cb
 * @private
 */
Blockchain.prototype._verifyTransaction = function(transactionMessage, filteredResult, cb) {
    var self = this;
    var tx = transactionMessage;

    var transaction = new Bitcore.Transaction(filteredResult.transaction);
    var status = 'unverified';

    // console.log(transaction.toObject());

    var outputs = transaction.outputs;
    var outputScriptHashes = {};
    var outputLength = outputs.length;

    // Loop through every output in the transaction
    for (var outputIndex = 0; outputIndex < outputLength; outputIndex++) {
        var output = outputs[outputIndex];
        var satoshis = output._satoshis;
        var script = new Bitcore.Script(output.script);

        if(!script) { // TODO: evaluate || !script.isDataOut() inclusion
            console.log('Invalid script');
            continue;
        }

        if(script.isPublicKeyHashOut()) {
            if ((tx.outputs[outputIndex].address == script.toAddress()) && (tx.outputs[outputIndex].amount == satoshis)) {
                var status = 'verified';
            } else {
                var status = 'unverified';
            }
        }

        if(script.isPublicKeyOut()) {
            console.log("script.isPublicKeyOut()");
        }

        if(script.isScriptHashOut()) {
            console.log("script.isScriptHashOut()");
        }

    }

    cb(null, status);
}

Blockchain.prototype._getHeader = function(network, hash, cb) {
    var self = this;

    var bc = this._getBlockchainExplorer(network, self.userAgent);
    bc.getHeader(hash, function(err, header) {
        if (err) return cb(err);
        return cb(null, header);
    });
};

Blockchain.prototype._getHeaders = function(network, hash, cb) {
    var self = this;

    var bc = this._getBlockchainExplorer(network, self.userAgent);
    bc.getHeaders(hash, function(err, headers) {
        if (err) return cb(err);
        return cb(null, headers);
    });
};

Blockchain.prototype._getBlockTxs = function(network, hash, cb) {
    var self = this;

    var bc = this._getBlockchainExplorer(network, self.userAgent);
    bc.getBlockTransactions(hash, function(err, header) {
        if (err) return cb(err);
        return cb(null, header);
    });
};

Blockchain.prototype._getFilteredBlockTxs = function(network, hash, tx, cb) {
    var self = this;

    var bc = this._getBlockchainExplorer(network, self.userAgent);
    bc.getFilteredBlockTransactions(hash, tx, function(err, header) {
        if (err) return cb(err);
        return cb(null, header);
    });
};

Blockchain.prototype._getRawTx = function(network, txid, cb) {
    var self = this;

    var bc = this._getBlockchainExplorer(network, self.userAgent);
    bc.getRawTx(txid, function(err, tx) {
        if (err) return cb(err);
        return cb(null, tx);
    });
};

Blockchain.prototype._getBlockchainExplorer = function(network) {
    var self = this;

    if (!this.blockchainExplorer) {
        var opts = {};
        if (this.blockchainExplorerOpts && this.blockchainExplorerOpts[network]) {
            opts = this.blockchainExplorerOpts[network];
        }
        // TODO: provider should be configurable
        opts.provider = 'insight';
        opts.network = network;
        opts.userAgent = self.userAgent;
        this.blockchainExplorer = new BlockchainExplorer(opts);
    }

    return this.blockchainExplorer;
};

Blockchain.prototype._transformHeader = function(rpcHeader) {
    return new Bitcore.BlockHeader.fromObject({ // TODO: move to bitcoind-rpc ?
        version: rpcHeader.version,
        prevHash: u.toHash(rpcHeader.prevHash),
        merkleRoot: u.toHash(rpcHeader.merkleRoot),
        time: rpcHeader.time,
        bits: parseInt(rpcHeader.bits, 16),
        nonce: rpcHeader.nonce
    });
};

Blockchain.prototype._transformHeaders = function(rpcHeaders, cb) {
    var self = this;
    var headers = [];

    // console.log(rpcHeaders);

    async.each(rpcHeaders,
        function(rpcHeader, cb){
            var blockHeader = new Bitcore.BlockHeader.fromObject({ // TODO: move to bitcoind-rpc ?
                version: rpcHeader.version,
                prevHash: u.toHash(rpcHeader.prevHash),
                merkleRoot: u.toHash(rpcHeader.merkleRoot),
                time: rpcHeader.time,
                bits: parseInt(rpcHeader.bits, 16),
                nonce: rpcHeader.nonce
            });

            headers.push(blockHeader);
            cb();
        },
        function(err){
            if (err) return cb(err);

            // console.log(headers);
            cb(null, headers);

        }
    );

};


Blockchain.prototype.subscribe = function(opts) {

};


// TODO: Move to utility function
TransactionStream.prototype._transform = function (chunk, encoding, done) {
    var data = chunk.toString();

    if (this._lastLineData) data = this._lastLineData + data;

    var lines = data.split('\n');
    this._lastLineData = lines.splice(lines.length-1,1)[0];

    lines.forEach(this.push.bind(this));
    done();
}


module.exports = Blockchain;