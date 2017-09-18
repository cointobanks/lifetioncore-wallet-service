'use strict';

var _ = require('lodash');
var async = require('async');
var $ = require('preconditions').singleton();
var log = require('npmlog');
log.debug = log.verbose;
log.disableColor();
var util = require('util');

var minimongo = require('minimongo');
//var mongodb = require('mongodb');

var Model = require('./model');

var collections = {
  WALLETS: 'wallets',
  TXS: 'txs',
  ADDRESSES: 'addresses',
  NOTIFICATIONS: 'notifications',
  COPAYERS_LOOKUP: 'copayers_lookup',
  PREFERENCES: 'preferences',
  EMAIL_QUEUE: 'email_queue',
  CACHE: 'cache',
  FIAT_RATES: 'fiat_rates',
  TX_NOTES: 'tx_notes',
  SESSIONS: 'sessions',
  PUSH_NOTIFICATION_SUBS: 'push_notification_subs',
  TX_CONFIRMATION_SUBS: 'tx_confirmation_subs',
};

var Storage = function(opts) {
  opts = opts || {};
  this.db = opts.db;
};

Storage.prototype._createIndexes = function() {
  this.db.collection(collections.WALLETS).createIndex({
    id: 1
  });
  this.db.collection(collections.COPAYERS_LOOKUP).createIndex({
    copayerId: 1
  });
  this.db.collection(collections.TXS).createIndex({
    walletId: 1,
    id: 1,
  });
  this.db.collection(collections.TXS).createIndex({
    walletId: 1,
    isPending: 1,
    txid: 1,
  });
  this.db.collection(collections.TXS).createIndex({
    walletId: 1,
    createdOn: -1,
  });
  this.db.collection(collections.NOTIFICATIONS).createIndex({
    walletId: 1,
    id: 1,
  });
  this.db.collection(collections.ADDRESSES).createIndex({
    walletId: 1,
    createdOn: 1,
  });
  this.db.collection(collections.ADDRESSES).createIndex({
    address: 1,
  });
  this.db.collection(collections.EMAIL_QUEUE).createIndex({
    notificationId: 1,
  });
  this.db.collection(collections.CACHE).createIndex({
    walletId: 1,
    type: 1,
    key: 1,
  });
  this.db.collection(collections.TX_NOTES).createIndex({
    walletId: 1,
    txid: 1,
  });
  this.db.collection(collections.PUSH_NOTIFICATION_SUBS).createIndex({
    copayerId: 1,
  });
  this.db.collection(collections.TX_CONFIRMATION_SUBS).createIndex({
    copayerId: 1,
    txid: 1,
  });
  this.db.collection(collections.SESSIONS).createIndex({
    copayerId: 1
  });
};

Storage.prototype.connect = function(opts, cb) {
  var self = this;

  opts = opts || {};

  if (this.db) return cb();

  var config = opts.mongoDb || {};
  mongodb.MongoClient.connect(config.uri, function(err, db) {
    if (err) {
      log.error('Unable to connect to the mongoDB. Check the credentials.');
      return cb(err);
    }
    self.db = db;
    self._createIndexes();
    console.log('Connection established to mongoDB');
    return cb();
  });
};


Storage.prototype.disconnect = function(cb) {
  var self = this;

  self.db = null;
  return cb();

  /*
   this.db.close(true, function(err) {
   if (err) return cb(err);
   self.db = null;
   return cb();
   });
   */
};

Storage.prototype.fetchWallet = function(id, cb) {

  this.db[collections.WALLETS].findOne({
    id: id
  }, {}, function(res) {
    if (!res) return cb();
    return cb(null, Model.Wallet.fromObj(res));
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.WALLETS).findOne({
   id: id
   }, function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();
   return cb(null, Model.Wallet.fromObj(result));
   });
   */
};

Storage.prototype.storeWallet = function(wallet, cb) {

  var self = this;

  var walletObj = wallet.toObject();
  this.db[collections.WALLETS].findOne({
    id: wallet.id
  }, function(res) {
    if(res) walletObj._id = res._id;
    self.db[collections.WALLETS].upsert(walletObj, function(res) {
      cb(null); // success
    }, cb);
  }, cb );

  /*
   this.db.collection(collections.WALLETS).update({
   id: wallet.id
   }, wallet.toObject(), {
   w: 1,
   upsert: true,
   }, cb);
   */

};

Storage.prototype.storeWalletAndUpdateCopayersLookup = function(wallet, cb) {
  var self = this;

  var copayerLookups = _.map(wallet.copayers, function(copayer) {
    $.checkState(copayer.requestPubKeys);
    return {
      copayerId: copayer.id,
      walletId: wallet.id,
      requestPubKeys: copayer.requestPubKeys,
    };
  });

  this.db[collections.COPAYERS_LOOKUP].findOne({
    walletId: wallet.id
  }, {}, function(res) {
    if(res) self.db[collections.COPAYERS_LOOKUP].remove(res._id);
    self.db[collections.COPAYERS_LOOKUP].upsert(copayerLookups, function(res) {
      return self.storeWallet(wallet, cb);
    }, function(err) { return cb(err); } );
  }, function(err) {
    return cb(err);
  });


  /*
   this.db.collection(collections.COPAYERS_LOOKUP).remove({
   walletId: wallet.id
   }, {
   w: 1
   }, function(err) {
   if (err) return cb(err);
   self.db.collection(collections.COPAYERS_LOOKUP).insert(copayerLookups, {
   w: 1
   }, function(err) {
   if (err) return cb(err);
   return self.storeWallet(wallet, cb);
   });
   });
   */
};

Storage.prototype.fetchCopayerLookup = function(copayerId, cb) {

  this.db[collections.COPAYERS_LOOKUP].findOne({
    copayerId: copayerId
  }, {}, function(res) {
    if (!res) return cb();
    if (!res.requestPubKeys) {
      res.requestPubKeys = [{
        key: res.requestPubKey,
        signature: res.signature,
      }];
    }
    return cb(null, res);
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.COPAYERS_LOOKUP).findOne({
   copayerId: copayerId
   }, function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();

   if (!result.requestPubKeys) {
   result.requestPubKeys = [{
   key: result.requestPubKey,
   signature: result.signature,
   }];
   }

   return cb(null, result);
   });
   */
};

Storage.prototype._completeTxData = function(walletId, txs, cb) {
  var self = this;

  self.fetchWallet(walletId, function(err, wallet) {
    if (err) return cb(err);
    _.each([].concat(txs), function(tx) {
      tx.derivationStrategy = wallet.derivationStrategy || 'BIP45';
      tx.creatorName = wallet.getCopayer(tx.creatorId).name;
      _.each(tx.actions, function(action) {
        action.copayerName = wallet.getCopayer(action.copayerId).name;
      });

      if (tx.status == 'accepted')
        tx.raw = tx.getRawTx();

    });
    return cb(null, txs);
  });
};

// TODO: remove walletId from signature
Storage.prototype.fetchTx = function(walletId, txProposalId, cb) {
  var self = this;

  this.db[collections.TXS].findOne({
    id: txProposalId,
    walletId: walletId
  }, {}, function(res) {
    if (!res) return cb();
    return self._completeTxData(walletId, Model.TxProposal.fromObj(res), cb);
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.TXS).findOne({
   id: txProposalId,
   walletId: walletId
   }, function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();
   return self._completeTxData(walletId, Model.TxProposal.fromObj(result), cb);
   });
   */
};

Storage.prototype.fetchTxByHash = function(hash, cb) {
  var self = this;

  this.db[collections.TXS].findOne({
    txid: hash,
  }, {}, function(res) {
    if (!res) return cb();
    return self._completeTxData(res.walletId, Model.TxProposal.fromObj(res), cb);
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.TXS).findOne({
   txid: hash,
   }, function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();

   return self._completeTxData(result.walletId, Model.TxProposal.fromObj(result), cb);
   });
   */
};

Storage.prototype.fetchLastTxs = function(walletId, creatorId, limit, cb) {
  var self = this;

  this.db[collections.TXS].find({
        walletId: walletId,
        creatorId: creatorId,
      }, {
        limit: limit || 5,
        sort: {createdOn: -1}}
  ).fetch(function(res) {
    if (!res) return cb();
    var txs = _.map(res, function(tx) {
      return Model.TxProposal.fromObj(tx);
    });
    return cb(null, txs);
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.TXS).find({
   walletId: walletId,
   creatorId: creatorId,
   }, {
   limit: limit || 5
   }).sort({
   createdOn: -1
   }).toArray(function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();
   var txs = _.map(result, function(tx) {
   return Model.TxProposal.fromObj(tx);
   });
   return cb(null, txs);
   });
   */
};



Storage.prototype.fetchPendingTxs = function(walletId, cb) {
  var self = this;

  this.db[collections.TXS].find({
    walletId: walletId,
    isPending: true,
  }, {sort: {createdOn: -1}}).fetch(function(res) {
    if (!res) return cb();
    var txs = _.map(res, function(tx) {
      return Model.TxProposal.fromObj(tx);
    });
    return self._completeTxData(walletId, txs, cb);
  }, function(err) {
    return cb(err);
  });

  /*
   self.db.collection(collections.TXS).find({
   walletId: walletId,
   isPending: true,
   }).sort({
   createdOn: -1
   }).toArray(function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();
   var txs = _.map(result, function(tx) {
   return Model.TxProposal.fromObj(tx);
   });
   return self._completeTxData(walletId, txs, cb);
   });
   */
};

/**
 * fetchTxs. Times are in UNIX EPOCH (seconds)
 *
 * @param walletId
 * @param opts.minTs
 * @param opts.maxTs
 * @param opts.limit
 */
Storage.prototype.fetchTxs = function(walletId, opts, cb) {
  var self = this;

  opts = opts || {};

  var tsFilter = {};
  if (_.isNumber(opts.minTs)) tsFilter.$gte = opts.minTs;
  if (_.isNumber(opts.maxTs)) tsFilter.$lte = opts.maxTs;

  var filter = {
    walletId: walletId
  };
  if (!_.isEmpty(tsFilter)) filter.createdOn = tsFilter;

  var mods = {
    sort: { createdOn: -1 }
  };

  if (_.isNumber(opts.limit)) mods.limit = opts.limit;

  this.db[collections.TXS].find(filter,mods).fetch(function(res) {
    if (!res) return cb();
    var txs = _.map(res, function(tx) {
      return Model.TxProposal.fromObj(tx);
    });
    return self._completeTxData(walletId, txs, cb);

  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.TXS).find(filter, mods).sort({
   createdOn: -1
   }).toArray(function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();
   var txs = _.map(result, function(tx) {
   return Model.TxProposal.fromObj(tx);
   });
   return self._completeTxData(walletId, txs, cb);
   });
   */
};

/**
 * fetchBroadcastedTxs. Times are in UNIX EPOCH (seconds)
 *
 * @param walletId
 * @param opts.minTs
 * @param opts.maxTs
 * @param opts.limit
 */
Storage.prototype.fetchBroadcastedTxs = function(walletId, opts, cb) {
  var self = this;

  opts = opts || {};

  var tsFilter = {};
  if (_.isNumber(opts.minTs)) tsFilter.$gte = opts.minTs;
  if (_.isNumber(opts.maxTs)) tsFilter.$lte = opts.maxTs;

  var filter = {
    walletId: walletId,
    status: 'broadcasted',
  };
  if (!_.isEmpty(tsFilter)) filter.broadcastedOn = tsFilter;

  var mods = {};
  if (_.isNumber(opts.limit)) mods.limit = opts.limit;

  this.db.collection(collections.TXS).find(filter, mods).sort({
    createdOn: -1
  }).toArray(function(err, result) {
    if (err) return cb(err);
    if (!result) return cb();
    var txs = _.map(result, function(tx) {
      return Model.TxProposal.fromObj(tx);
    });
    return self._completeTxData(walletId, txs, cb);
  });
};

/**
 * Retrieves notifications after a specific id or from a given ts (whichever is more recent).
 *
 * @param {String} notificationId
 * @param {Number} minTs
 * @returns {Notification[]} Notifications
 */
Storage.prototype.fetchNotifications = function(walletId, notificationId, minTs, cb) {
  function makeId(timestamp) {
    return _.padLeft(timestamp, 14, '0') + _.repeat('0', 4);
  };

  var self = this;

  var minId = makeId(minTs);
  if (notificationId) {
    minId = notificationId > minId ? notificationId : minId;
  }

  this.db[collections.NOTIFICATIONS].find({
    walletId: walletId,
    id: {
      $gt: minId,
    },
  }, {sort: {id: 1}}).fetch(function(res) {
    if (!res) return cb();
    var notifications = _.map(res, function(notification) {
      return Model.Notification.fromObj(notification);
    });
    return cb(null, notifications);
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.NOTIFICATIONS)
   .find({
   walletId: walletId,
   id: {
   $gt: minId,
   },
   })
   .sort({
   id: 1
   })
   .toArray(function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();
   var notifications = _.map(result, function(notification) {
   return Model.Notification.fromObj(notification);
   });
   return cb(null, notifications);
   });
   */
};

// TODO: remove walletId from signature
Storage.prototype.storeNotification = function(walletId, notification, cb) {

  this.db[collections.NOTIFICATIONS].upsert(notification, function(res) { cb(null); }, cb);

  /*
   this.db.collection(collections.NOTIFICATIONS).insert(notification, {
   w: 1
   }, cb);
   */
};

// TODO: remove walletId from signature
Storage.prototype.storeTx = function(walletId, txp, cb) {

  var self = this;

  txp.walletId = walletId;

  this.db[collections.TXS].findOne({
    id: txp.id,
    walletId: walletId
  }, function(res) {
    if(res) txp._id = res._id;
    self.db[collections.TXS].upsert(txp.toObject(), function(res) {
      cb(null); // success
    }, cb);
  }, cb );

  /*
   this.db.collection(collections.TXS).update({
   id: txp.id,
   walletId: walletId
   }, txp.toObject(), {
   w: 1,
   upsert: true,
   }, cb);
   */
};

Storage.prototype.removeTx = function(walletId, txProposalId, cb) {

  var self = this;

  this.db[collections.TXS].findOne({
    id: txProposalId,
    walletId: walletId
  }, {}, function(res) {
    self.db[collections.TXS].remove(res._id, cb);
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.TXS).findAndRemove({
   id: txProposalId,
   walletId: walletId
   }, {
   w: 1
   }, cb);
   */

};

Storage.prototype.removeWallet = function(walletId, cb) {
  var self = this;

  async.parallel([

    function(next) {

      self.db[collections.WALLETS].findOne({
        id: walletId
      }, function(res) {
        if(!res) return next();
        self.db[collections.WALLETS].remove(res._id, next);
      }, function(err) {
        return cb(err);
      });

      /*
       self.db.collection(collections.WALLETS).findAndRemove({
       id: walletId
       }, next);
       */
    },
    function(next) {
      var otherCollections = _.without(_.values(collections), collections.WALLETS);
      async.each(otherCollections, function(col, next) {

        self.db[col].find({
          walletId: walletId
        }, {}).fetch( function(res) {
          if(!res[0]) return next();

          for (var i = 0; i < res.length; i++) {
            self.db[col].remove(res[i]._id);
          }

          return next();
        }, function(err) {
          return cb(err);
        });

        /*
         self.db.collection(col).remove({
         walletId: walletId
         }, next);
         */
      }, next);
    },
  ], cb);
};


Storage.prototype.fetchAddresses = function(walletId, cb) {
  var self = this;

  this.db[collections.ADDRESSES].find({
    walletId: walletId,
  }, {sort: {createdOn: 1}}).fetch(function(res) {
    if (!res) return cb();
    var addresses = _.map(res, function(address) {
      return Model.Address.fromObj(address);
    });
    return cb(null, addresses);
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.ADDRESSES).find({
   walletId: walletId,
   }).sort({
   createdOn: 1
   }).toArray(function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();
   var addresses = _.map(result, function(address) {
   return Model.Address.fromObj(address);
   });
   return cb(null, addresses);
   });
   */
};

Storage.prototype.countAddresses = function(walletId, cb) {

  this.db[collections.ADDRESSES].find({
    walletId: walletId,
  }, { }).fetch(function(res) {
    if (!res) return cb(null, null);
    cb(null, res.length);
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.ADDRESSES).find({
   walletId: walletId,
   }).count(cb);
   */
};

Storage.prototype.storeAddress = function(address, cb) {
  var self = this;

  self.db[collections.ADDRESSES].findOne({
    address: address,
  }, {}, function(res) {
    if(res) address._id = res._id;
    self.db[collections.ADDRESSES].upsert(address, function(res) {
      cb(null);
    }, function(err) {
      return cb();
    } );
  }, function(err) {
    return cb();
  });


  /*
   self.db.collection(collections.ADDRESSES).update({
   address: address.address
   }, address, {
   w: 1,
   upsert: false,
   }, cb);
   */
};

Storage.prototype.storeAddressAndWallet = function(wallet, addresses, cb) {
  var self = this;

  function saveAddresses(addresses, cb) {
    if (_.isEmpty(addresses)) return cb();
    self.db.collection(collections.ADDRESSES).insert(addresses, {
      w: 1
    }, cb);
  };

  var addresses = [].concat(addresses);
  if (addresses.length == 0) return cb();

  async.filter(addresses, function(address, next) {

    self.db[collections.ADDRESSES].findOne({
      address: address.address,
    }, { }, function(res) {
      if(!res) return next(true);
      if (res.walletId != wallet.id) {
        log.warn('Address ' + address.address + ' exists in more than one wallet.');
        return next(true);
      }
      // Ignore if address was already in wallet
      return next(false);
    }, function(err) {
      return cb(err);
    });

    /*
     self.db.collection(collections.ADDRESSES).findOne({
     address: address.address,
     }, {
     walletId: true,
     }, function(err, result) {
     if (err || !result) return next(true);
     if (result.walletId != wallet.id) {
     log.warn('Address ' + address.address + ' exists in more than one wallet.');
     return next(true);
     }
     // Ignore if address was already in wallet
     return next(false);
     });
     */

  }, function(newAddresses) {
    if (newAddresses.length < addresses.length) {
      log.warn('Attempted to store already existing addresses on wallet ' + wallet.id);
    }
    saveAddresses(newAddresses, function(err) {
      if (err) return cb(err);
      self.storeWallet(wallet, cb);
    });


    /*
     self.db[collections.ADDRESSES].upsert(newAddresses, function(res) {
     self.storeWallet(wallet, cb);
     }, function(err) { cb(err) });
     */

    /*
     self.db.collection(collections.ADDRESSES).insert(newAddresses, {
     w: 1
     }, function(err) {
     if (err) return cb(err);
     self.storeWallet(wallet, cb);
     });
     */

  });
};

Storage.prototype.fetchAddress = function(address, cb) {
  var self = this;

  // TODO: not tested
  // not sure where test coverage is for this

  self.db[collection.ADDRESSES].findOne({
    address: address,
  }, {}, function(res) {
    if (!res) return cb();
    return cb(null, Model.Address.fromObj(result));
  }, function(err) {
    return cb(err);
  });


  /*
   this.db.collection(collections.ADDRESSES).findOne({
   address: address,
   }, function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();

   return cb(null, Model.Address.fromObj(result));
   });
   */
};

Storage.prototype.fetchPreferences = function(walletId, copayerId, cb) {

  this.db[collections.PREFERENCES].find({
    walletId: walletId,
  }).fetch(function(res) {
    if (!res) return cb();

    if (copayerId) {
      res = _.find(res, {
        copayerId: copayerId
      });
    }
    if (!res) return cb();

    var preferences = _.map([].concat(res), function(r) {
      return Model.Preferences.fromObj(r);
    });
    if (copayerId) {
      preferences = preferences[0];
    }
    return cb(null, preferences);
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.PREFERENCES).find({
   walletId: walletId,
   }).toArray(function(err, result) {
   if (err) return cb(err);

   if (copayerId) {
   result = _.find(result, {
   copayerId: copayerId
   });
   }
   if (!result) return cb();

   var preferences = _.map([].concat(result), function(r) {
   return Model.Preferences.fromObj(r);
   });
   if (copayerId) {
   preferences = preferences[0];
   }
   return cb(null, preferences);
   });
   */
};

Storage.prototype.storePreferences = function(preferences, cb) {
  var self = this;

  this.db[collections.PREFERENCES].findOne({
    walletId: preferences.walletId,
    copayerId: preferences.copayerId
  }, function(res) {
    if(res) preferences._id = res._id;
    self.db[collections.PREFERENCES].upsert(preferences, function(res) { cb(null); }, cb);
  }, cb );

  /*
   this.db.collection(collections.PREFERENCES).update({
   walletId: preferences.walletId,
   copayerId: preferences.copayerId,
   }, preferences, {
   w: 1,
   upsert: true,
   }, cb);
   */
};

Storage.prototype.storeEmail = function(email, cb) {

  var self = this;

  this.db[collections.EMAIL_QUEUE].findOne({
    id: email.id,
  }, function(res) {
    if(res) email._id = res._id;
    self.db[collections.EMAIL_QUEUE].upsert(email, function(res) { cb(null); }, cb);
  }, cb );

  /*
   this.db.collection(collections.EMAIL_QUEUE).update({
   id: email.id,
   }, email, {
   w: 1,
   upsert: true,
   }, cb);
   */
};

Storage.prototype.fetchUnsentEmails = function(cb) {

  this.db[collections.EMAIL_QUEUE].find({
    status: 'pending',
  }).fetch(function(res) {
    if (!res || _.isEmpty(res)) return cb(null, []);
    return cb(null, Model.Email.fromObj(res));
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.EMAIL_QUEUE).find({
   status: 'pending',
   }).toArray(function(err, result) {
   if (err) return cb(err);
   if (!result || _.isEmpty(result)) return cb(null, []);
   return cb(null, Model.Email.fromObj(result));
   });
   */
};

Storage.prototype.fetchEmailByNotification = function(notificationId, cb) {

  this.db[collections.EMAIL_QUEUE].findOne({
    notificationId: notificationId,
  }, {}, function(res) {
    if (!res) return cb();
    return cb(null, Model.Email.fromObj(res));
  }, function(err) {
    return cb(err);
  });

  /*
   this.db.collection(collections.EMAIL_QUEUE).findOne({
   notificationId: notificationId,
   }, function(err, result) {
   if (err) return cb(err);
   if (!result) return cb();

   return cb(null, Model.Email.fromObj(result));
   });
   */
};

Storage.prototype.cleanActiveAddresses = function(walletId, cb) {
  var self = this;

  async.series([

    function(next) {

      self.db[collections.CACHE].findOne({
        walletId: walletId,
        type: 'activeAddresses',
      }, {}, function(res) {
        if(res){self.db[collections.CACHE].remove(res._id, next);}
        else{next();}
      }, function(err) {
        return cb(err);
      });

      /*
       self.db.collection(collections.CACHE).remove({
       walletId: walletId,
       type: 'activeAddresses',
       }, {
       w: 1
       }, next);
       */

    },
    function(next) {

      self.db[collections.CACHE].upsert({
        walletId: walletId,
        type: 'activeAddresses',
        key: null
      }, function(res) {
        next();
      }, function(err) {

      });

      /*
       self.db.collection(collections.CACHE).insert({
       walletId: walletId,
       type: 'activeAddresses',
       key: null
       }, {
       w: 1
       }, next);
       */

    },
  ], cb);
};

Storage.prototype.storeActiveAddresses = function(walletId, addresses, cb) {
  var self = this;

  async.each(addresses, function(address, next) {
    var record = {
      walletId: walletId,
      type: 'activeAddresses',
      key: address,
    };

    self.db[collections.CACHE].findOne(record, {}, function(res) {
      if(res) record._id = res._id;
      self.db[collections.CACHE].upsert(record, function(res) {
        next();
      }, function(err) {
        cb(err);
      })

    }, function(err) {
      cb(err);
    });


    /*
     self.db.collection(collections.CACHE).update({
     walletId: record.walletId,
     type: record.type,
     key: record.key,
     }, record, {
     w: 1,
     upsert: true,
     }, next);
     */

  }, cb);
};

// --------         ---------------------------  Total
//           > Time >                  
//                       ^to     <=  ^from
//                       ^fwdIndex  =>  ^end
Storage.prototype.getTxHistoryCache = function(walletId, from, to, cb) {
  var self = this;
  $.checkArgument(from >= 0);
  $.checkArgument(from <= to);

  self.db.collection(collections.CACHE).findOne({
    walletId: walletId,
    type: 'historyCacheStatus',
    key: null
  }, function(err, result) {
    if (err) return cb(err);
    if (!result) return cb();
    if (!result.isUpdated) return cb();

    // Reverse indexes
    var fwdIndex = result.totalItems - to;

    if (fwdIndex < 0) {
      fwdIndex = 0;
    }

    var end = result.totalItems - from;

    // nothing to return
    if (end <= 0) return cb(null, []);

    // Cache is OK.
    self.db.collection(collections.CACHE).find({
      walletId: walletId,
      type: 'historyCache',
      key: {
        $gte: fwdIndex,
        $lt: end
      },
    }).sort({
      key: -1,
    }).toArray(function(err, result) {
      if (err) return cb(err);

      if (!result) return cb();

      if (result.length < end - fwdIndex) {
        // some items are not yet defined.
        return cb();
      }

      var txs = _.pluck(result, 'tx');
      return cb(null, txs);
    });
  })
};

Storage.prototype.softResetAllTxHistoryCache = function(cb) {
  this.db.collection(collections.CACHE).update({
    type: 'historyCacheStatus',
  }, {
    isUpdated: false,
  }, {
    multi: true,
  }, cb);
};

Storage.prototype.softResetTxHistoryCache = function(walletId, cb) {
  this.db.collection(collections.CACHE).update({
    walletId: walletId,
    type: 'historyCacheStatus',
    key: null
  }, {
    isUpdated: false,
  }, {
    w: 1,
    upsert: true,
  }, cb);
};

Storage.prototype.clearTxHistoryCache = function(walletId, cb) {
  var self = this;
  self.db.collection(collections.CACHE).remove({
    walletId: walletId,
    type: 'historyCache',
  }, {
    multi: 1
  }, function(err) {
    if (err) return cb(err);
    self.db.collection(collections.CACHE).remove({
      walletId: walletId,
      type: 'historyCacheStatus',
      key: null
    }, {
      w: 1
    }, cb);
  });
};

// items should be in CHRONOLOGICAL order
Storage.prototype.storeTxHistoryCache = function(walletId, totalItems, firstPosition, items, cb) {
  $.shouldBeNumber(firstPosition);
  $.checkArgument(firstPosition >= 0);
  $.shouldBeNumber(totalItems);
  $.checkArgument(totalItems >= 0);

  var self = this;

  _.each(items, function(item, i) {
    item.position = firstPosition + i;
  });
  var cacheIsComplete = (firstPosition == 0);

  // TODO: check txid uniqness?
  async.each(items, function(item, next) {
    var pos = item.position;
    delete item.position;
    self.db.collection(collections.CACHE).update({
      walletId: walletId,
      type: 'historyCache',
      key: pos,
    }, {
      walletId: walletId,
      type: 'historyCache',
      key: pos,
      tx: item,
    }, {
      w: 1,
      upsert: true,
    }, next);
  }, function(err) {
    if (err) return cb(err);

    self.db.collection(collections.CACHE).update({
      walletId: walletId,
      type: 'historyCacheStatus',
      key: null
    }, {
      walletId: walletId,
      type: 'historyCacheStatus',
      key: null,
      totalItems: totalItems,
      updatedOn: Date.now(),
      isComplete: cacheIsComplete,
      isUpdated: true,
    }, {
      w: 1,
      upsert: true,
    }, cb);
  });
};





Storage.prototype.fetchActiveAddresses = function(walletId, cb) {
  var self = this;

  this.db[collections.CACHE].find({
    walletId: walletId,
    type: 'activeAddresses',
  }).fetch(function(res) {
    console.log(res);
    if (_.isEmpty(res)) return cb();
    return cb(null, _.compact(_.pluck(res, 'key')));
  }, function(err) {
    return cb(err);
  });

  /*
   self.db.collection(collections.CACHE).find({
   walletId: walletId,
   type: 'activeAddresses',
   }).toArray(function(err, result) {
   if (err) return cb(err);
   if (_.isEmpty(result)) return cb();

   return cb(null, _.compact(_.pluck(result, 'key')));
   });
   */
};

Storage.prototype.storeFiatRate = function(providerName, rates, cb) {
  var self = this;

  var now = Date.now();
  async.each(rates, function(rate, next) {

    self.db[collections.FIAT_RATES].upsert({
      provider: providerName,
      ts: now,
      code: rate.code,
      value: rate.value,
    }, function(res) {
      next();
    }, function(err) {
      cb(err);
    });

    /*
     self.db.collection(collections.FIAT_RATES).insert({
     provider: providerName,
     ts: now,
     code: rate.code,
     value: rate.value,
     }, {
     w: 1
     }, next);
     */

  }, cb);
};

Storage.prototype.fetchFiatRate = function(providerName, code, ts, cb) {
  var self = this;

  this.db[collections.FIAT_RATES].find({
        provider: providerName,
        code: code,
        ts: {
          $lte: ts
        },
      }, {
        limit: 1,
        sort: {ts: -1}}
  ).fetch(function(res) {
    if (!res || _.isEmpty(res)) return cb(null);
    return cb(null, res[0]);
  }, function(err) {
    return cb(err);
  });

  /*
   self.db.collection(collections.FIAT_RATES).find({
   provider: providerName,
   code: code,
   ts: {
   $lte: ts
   },
   }).sort({
   ts: -1
   }).limit(1).toArray(function(err, result) {
   if (err || _.isEmpty(result)) return cb(err);
   return cb(null, result[0]);
   });
   */
};

Storage.prototype.fetchTxNote = function(walletId, txid, cb) {
  var self = this;

  self.db.collection(collections.TX_NOTES).findOne({
    walletId: walletId,
    txid: txid,
  }, function(err, result) {
    if (err) return cb(err);
    if (!result) return cb();
    return self._completeTxNotesData(walletId, Model.TxNote.fromObj(result), cb);
  });
};

// TODO: should be done client-side
Storage.prototype._completeTxNotesData = function(walletId, notes, cb) {
  var self = this;

  self.fetchWallet(walletId, function(err, wallet) {
    if (err) return cb(err);
    _.each([].concat(notes), function(note) {
      note.editedByName = wallet.getCopayer(note.editedBy).name;
    });
    return cb(null, notes);
  });
};

/**
 * fetchTxNotes. Times are in UNIX EPOCH (seconds)
 *
 * @param walletId
 * @param opts.minTs
 */
Storage.prototype.fetchTxNotes = function(walletId, opts, cb) {
  var self = this;

  var filter = {
    walletId: walletId,
  };
  if (_.isNumber(opts.minTs)) filter.editedOn = {
    $gte: opts.minTs
  };
  this.db.collection(collections.TX_NOTES).find(filter).toArray(function(err, result) {
    if (err) return cb(err);
    var notes = _.compact(_.map(result, function(note) {
      return Model.TxNote.fromObj(note);
    }));
    return self._completeTxNotesData(walletId, notes, cb);
  });
};

Storage.prototype.storeTxNote = function(txNote, cb) {
  this.db.collection(collections.TX_NOTES).update({
    txid: txNote.txid,
    walletId: txNote.walletId
  }, txNote.toObject(), {
    w: 1,
    upsert: true,
  }, cb);
};

Storage.prototype.getSession = function(copayerId, cb) {
  var self = this;

  self.db.collection(collections.SESSIONS).findOne({
      copayerId: copayerId,
    },
    function(err, result) {
      if (err || !result) return cb(err);
      return cb(null, Model.Session.fromObj(result));
    });
};

Storage.prototype.storeSession = function(session, cb) {
  this.db.collection(collections.SESSIONS).update({
    copayerId: session.copayerId,
  }, session.toObject(), {
    w: 1,
    upsert: true,
  }, cb);
};

Storage.prototype.fetchPushNotificationSubs = function(copayerId, cb) {
  this.db.collection(collections.PUSH_NOTIFICATION_SUBS).find({
    copayerId: copayerId,
  }).toArray(function(err, result) {
    if (err) return cb(err);

    if (!result) return cb();

    var tokens = _.map([].concat(result), function(r) {
      return Model.PushNotificationSub.fromObj(r);
    });
    return cb(null, tokens);
  });
};

Storage.prototype.storePushNotificationSub = function(pushNotificationSub, cb) {
  this.db.collection(collections.PUSH_NOTIFICATION_SUBS).update({
    copayerId: pushNotificationSub.copayerId,
    token: pushNotificationSub.token,
  }, pushNotificationSub, {
    w: 1,
    upsert: true,
  }, cb);
};

Storage.prototype.removePushNotificationSub = function(copayerId, token, cb) {
  this.db.collection(collections.PUSH_NOTIFICATION_SUBS).remove({
    copayerId: copayerId,
    token: token,
  }, {
    w: 1
  }, cb);
};

Storage.prototype.fetchActiveTxConfirmationSubs = function(copayerId, cb) {
  var filter = {
    isActive: true
  };
  if (copayerId) filter.copayerId = copayerId;

  this.db.collection(collections.TX_CONFIRMATION_SUBS).find(filter)
    .toArray(function(err, result) {
      if (err) return cb(err);

      if (!result) return cb();

      var subs = _.map([].concat(result), function(r) {
        return Model.TxConfirmationSub.fromObj(r);
      });
      return cb(null, subs);
    });
};

Storage.prototype.storeTxConfirmationSub = function(txConfirmationSub, cb) {
  this.db.collection(collections.TX_CONFIRMATION_SUBS).update({
    copayerId: txConfirmationSub.copayerId,
    txid: txConfirmationSub.txid,
  }, txConfirmationSub, {
    w: 1,
    upsert: true,
  }, cb);
};

Storage.prototype.removeTxConfirmationSub = function(copayerId, txid, cb) {
  this.db.collection(collections.TX_CONFIRMATION_SUBS).remove({
    copayerId: copayerId,
    txid: txid,
  }, {
    w: 1
  }, cb);
};


Storage.prototype._dump = function(cb, fn) {
  fn = fn || console.log;
  cb = cb || function() {};

  // TODO: can't locate test coverage, unconverted mongo below

  var self = this;
  this.db.collections(function(err, collections) {
    if (err) return cb(err);
    async.eachSeries(collections, function(col, next) {
      col.find().toArray(function(err, items) {
        fn('--------', col.s.name);
        fn(items);
        fn('------------------------------------------------------------------\n\n');
        next(err);
      });
    }, cb);
  });
};

Storage.collections = collections;
module.exports = Storage;
