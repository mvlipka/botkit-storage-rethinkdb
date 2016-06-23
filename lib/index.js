var rethinkdb = require("rethinkdb");

module.exports = function (config, callback) {
    if (!config || !config.host || !config.port) throw new Error('Please provide RethinkDB configuration');

    var connection = null;
    var storage = {};
    rethinkdb.connect({ host: config.host, port: config.port }, function (err, conn) {
        if (err) throw err;
        connection = conn;
        initRethink(conn, function () {
            ['teams', 'channels', 'users'].forEach(function (zone) {
                storage[zone] = getStorage(connection, zone);
            });
            callback();
        });
    });
    return storage;
};

function initRethink(conn, cb) {
    createDatabase(conn, cb);
}

function createDatabase(conn, cb) {
    rethinkdb.dbCreate('botkit_storage').run(conn, function () {
        createTables(conn, cb);
    });
};

function createTables(conn, cb) {
    rethinkdb.db('botkit_storage').tableCreate('teams').run(conn, function (err, res) {
        rethinkdb.db('botkit_storage').tableCreate('channels').run(conn, function (err, res) {
            rethinkdb.db('botkit_storage').tableCreate('users').run(conn, function (err, res) {
                cb();
            });
        });
    });
};

function getStorage(conn, zone) {
    var table = rethinkdb.db('botkit_storage').table(zone);

    return {
        get: function (id, cb) {
            table.get(id).run(conn, cb);
        },
        save: function (data, cb) {
            table.get(data.id).replace(data).run(conn, cb);
        },
        all: function (cb) {
            table.run(conn, function (err, cursor) {
                cursor.toArray(function (err, result) {
                    cb(err, result);
                })
            });
        }
    }
}