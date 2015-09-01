var Queue = require('./queue');
var Worker = require('./worker');

var MongoClient = require('mongodb').MongoClient;
var ObjectID = require('mongodb').ObjectID;

module.exports = Connection;

function Connection(url, options) {
    this.url = url;
    this.options = options || {};
}

Connection.prototype.init = function (callback) {
    this.getDb(callback)
}

Connection.prototype.getDb = function (callback) {
    var that = this;
    if (that.db) {
        callback(null, that.db);
    } else {
        MongoClient.connect(that.url, function(err, db) {
            if (err == null) {
                console.log("MonQ. Successfully connected to MongoDB");
            } else {
                console.log("ERROR. MonQ. Failed to Connect to MongoDB");
            }

            that.db = db;
            if (callback) {
                callback(err, that.db);
            }
        });
    }
}

Connection.prototype.worker = function (queues, options) {
    var self = this;

    var queues = queues.map(function (queue) {
        if (typeof queue === 'string') {
            queue = self.queue(queue, options);
        }
        return queue;
    });
    return new Worker(queues, options);
};

Connection.prototype.queue = function (name, options) {
    return new Queue(this, name, options);
};

Connection.prototype.close = function () {
    this.db.close();
};

Connection.prototype.archive = function (job) {
    var collection = this.options.collection || 'workers';

    var collectionWorkers = this.db.collection(collection);
    var collectionWorkersArchive = this.db.collection(collection + '_archives');

    collectionWorkers.remove({_id: new ObjectID(job._id)}, function (err) {
        collectionWorkersArchive.insert(job, function(error){
            "use strict";
            if (error != null) {
                console.error(error);
            }
        });
    });
};