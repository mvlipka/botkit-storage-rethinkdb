var rethinkdb = require("rethinkdb");

module.exports = function (config, cb) {
    if (!config || !config.host || !config.port) throw new Error('Please provide RethinkDB configuration');
    
    var connection = null;
    var storage = {};   
    rethinkdb.connect({ host: config.host, port: config.port }, function (err, conn) {
        if (err) throw err;
        connection = conn;
        console.log("HI");
        cb();
    });

    ['teams', 'channels', 'users'].forEach(function (zone) {
        storage[zone] = getStorage(connection, zone);
    });

    return storage;
}

function getStorage(conn, zone) {
    var table = rethinkdb.table(zone);

    return {
        get: function (id, cb) {
            table.filter(r.row('id').eq(id)).run(conn, cb);
        },
        save: function (data, cb) {
            table.insert(data).run(conn, cb);
        },
        all: function (cb) {
            table.run(conn, cb);
        }
    }
}