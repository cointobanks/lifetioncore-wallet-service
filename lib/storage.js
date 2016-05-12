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
};

var Storage = function(opts) {
  opts = opts || {};
  this.db = opts.db;
  this.db.addCollection("wallets");
  this.db.addCollection("txs");
  this.db.addCollection("addresses");
  this.db.addCollection("notifications");
  this.db.addCollection("copayers_lookup");
  this.db.addCollection("preferences");
  this.db.addCollection("email_queue");
  this.db.addCollection("cache");
  this.db.addCollection("fiat_rates");
};

Storage.prototype._createIndexes = function() {

  // TODO: replace with collections reference
  this.db.addCollection("wallets");
  this.db.addCollection("txs");
  this.db.addCollection("addresses");
  this.db.addCollection("notifications");
  this.db.addCollection("copayers_lookup");
  this.db.addCollection("preferences");
  this.db.addCollection("email_queue");
  this.db.addCollection("cache");
  this.db.addCollection("fiat_rates");

/* not supported by minimongo
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
  this.db.collection(collections.ADDRESSES).dropIndex({
    walletId: 1
  });
  */
};

Storage.prototype.connect = function(opts, cb) {
  var self = this;

  opts = opts || {};

  if (this.db) return cb();

  var config = opts.mongoDb || {};

  var localDb = minimongo.MemoryDb;
  db = new LocalDb();

  self.db = db;
  self._createIndexes();
  return cb();

  /*
  mongodb.MongoClient.connect(config.uri, function(err, db) {
    if (err) {
      log.error('Unable to connect to the mongoDB server on ', config.uri);
      return cb(err);
    }
    self.db = db;
    self._createIndexes();
    console.log('Connection established to ', config.uri);
    return cb();
  });
  */
};


Storage.prototype.disconnect = function(cb) {
  var self = this;
  this.db.close(true, function(err) {
    if (err) return cb(err);
    self.db = null;
    return cb();
  });
};

Storage.prototype.fetchWallet = function(id, cb) {

  this.db.wallets.findOne({
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
  this.db.wallets.findOne({
    id: wallet.id
  }, function(res) {
    if(res) walletObj._id = res._id;
    self.db.wallets.upsert(walletObj, function(res) {
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


  this.db.copayers_lookup.findOne({
    walletId: wallet.id
  }, {}, function(res) {
    if(res) self.db.copayers_lookup.remove(res._id);
    self.db.copayers_lookup.upsert(copayerLookups, function(res) {
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

  this.db.copayers_lookup.findOne({
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

// TODO: should be done client-side
Storage.prototype._completeTxData = function(walletId, txs, cb) {
  var txList = [].concat(txs);
  this.fetchWallet(walletId, function(err, wallet) {
    if (err) return cb(err);
    _.each(txList, function(tx) {
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

  this.db.txs.findOne({
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

  this.db.txs.findOne({
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

  this.db.txs.find({
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

  this.db.txs.find({
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

  var mods = {};
  if (_.isNumber(opts.limit)) mods.limit = opts.limit;

  this.db.txs.find(filter,mods).fetch(function(res) {
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

  this.db.notifications.find({
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

  this.db.notifications.upsert(notification, function(res) { cb(null); }, cb);

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
  if (txp.status == 'pending') txp.isPending = true; // TODO - figure out why this has to be included

  this.db.txs.findOne({
    id: txp.id,
    walletId: walletId
  }, function(res) {
    if(res) txp._id = res._id;
    self.db.txs.upsert(txp, function(res) {
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

  this.db.txs.findOne({
    id: txProposalId,
    walletId: walletId
  }, {}, function(res) {
    self.db.txs.remove(res._id, cb);
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

  // TODO: needs refactor

  async.parallel([

    function(next) {
      self.db.collection(collections.WALLETS).findAndRemove({
        id: walletId
      }, next);
    },
    function(next) {
      var otherCollections = _.without(_.values(collections), collections.WALLETS);
      async.each(otherCollections, function(col, next) {
        self.db.collection(col).remove({
          walletId: walletId
        }, next);
      }, next);
    },
  ], cb);
};


Storage.prototype.fetchAddresses = function(walletId, cb) {
  var self = this;

  this.db.addresses.find({
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

  // TODO: needs refactor

  this.db.collection(collections.ADDRESSES).find({
    walletId: walletId,
  }).count(cb);
};

Storage.prototype.storeAddress = function(address, cb) {
  var self = this;

  // fix

  self.db.addresses.upsert({
    address: address.address
  }, function(res) { cb(null); }, cb);

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

  var addresses = [].concat(addresses);
  if (addresses.length == 0) return cb();

  async.filter(addresses, function(address, next) {

    self.db.addresses.findOne({
      address: address.address,
      }, {
      walletId: true,
      }, function(res) {
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
    if (newAddresses.length == 0) return cb();

    self.db.addresses.upsert(newAddresses, function(res) {
      self.storeWallet(wallet, cb);
    }, function(err) { cb(err) });

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

  // TODO: needs refactor

  this.db.collection(collections.ADDRESSES).findOne({
    address: address,
  }, function(err, result) {
    if (err) return cb(err);
    if (!result) return cb();

    return cb(null, Model.Address.fromObj(result));
  });
};

Storage.prototype.fetchPreferences = function(walletId, copayerId, cb) {

  this.db.preferences.find({
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

  this.db.preferences.findOne({
    walletId: preferences.walletId,
    copayerId: preferences.copayerId
  }, function(res) {
    if(res) preferences._id = res._id;
    self.db.preferences.upsert(preferences, function(res) { cb(null); }, cb);
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

  // TODO: needs refactor

  this.db.collection(collections.EMAIL_QUEUE).update({
    id: email.id,
  }, email, {
    w: 1,
    upsert: true,
  }, cb);
};

Storage.prototype.fetchUnsentEmails = function(cb) {

  // TODO: needs refactor

  this.db.collection(collections.EMAIL_QUEUE).find({
    status: 'pending',
  }).toArray(function(err, result) {
    if (err) return cb(err);
    if (!result || _.isEmpty(result)) return cb(null, []);
    return cb(null, Model.Email.fromObj(result));
  });
};

Storage.prototype.fetchEmailByNotification = function(notificationId, cb) {

  // TODO: needs refactor

  this.db.collection(collections.EMAIL_QUEUE).findOne({
    notificationId: notificationId,
  }, function(err, result) {
    if (err) return cb(err);
    if (!result) return cb();

    return cb(null, Model.Email.fromObj(result));
  });
};

Storage.prototype.cleanActiveAddresses = function(walletId, cb) {
  var self = this;

  async.series([

    function(next) {

      self.db.cache.findOne({
        walletId: walletId,
        type: 'activeAddresses',
      }, {}, function(res) {
        if(res) self.db.cache.remove(res._id, next);
        next();
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

      self.db.cache.upsert({
        walletId: walletId,
        type: 'activeAddresses',
        key: null
      }, next);

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

  // TODO: test coverage?

  async.each(addresses, function(address, next) {
    var record = {
      walletId: walletId,
      type: 'activeAddresses',
      key: address,
    };

    this.db.cache.findOne({
      walletId: record.walletId,
      type: record.type,
      key: record.key,
    }, function(res) {
      if(res) record._id = res._id;
      self.db.cache.upsert(record, next, cb);
    }, cb );

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

Storage.prototype.fetchActiveAddresses = function(walletId, cb) {
  var self = this;

  // TODO: needs refactor ??

  this.db.addresses.find({
    walletId: walletId,
    type: 'activeAddresses',
  }, { }).fetch(function(res) {
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

  // TODO: needs refactor

  var now = Date.now();
  async.each(rates, function(rate, next) {
    self.db.collection(collections.FIAT_RATES).insert({
      provider: providerName,
      ts: now,
      code: rate.code,
      value: rate.value,
    }, {
      w: 1
    }, next);
  }, cb);
};

Storage.prototype.fetchFiatRate = function(providerName, code, ts, cb) {
  var self = this;

  // TODO: needs refactor

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
};

Storage.prototype._dump = function(cb, fn) {
  fn = fn || console.log;
  cb = cb || function() {};

  // TODO: needs refactor

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
