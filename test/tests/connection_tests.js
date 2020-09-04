exports.beforeTests = function(configuration, callback) {
  var Proxy = require('../../lib/proxy')
    , MongoClient = require('mongodb').MongoClient;

  // URI
  var mongodburi = "mongodb://localhost:27017/test";
  // Create a new proxy and start it
  var proxy = new Proxy({
      port: 51000, uri: mongodburi, bind_to: '127.0.0.1',
      debug:true, log_debug:true, log_level:'debug', rw:false, "auth-sslValidate":false,
      socketTimeout: 10000,
      tls: true
  });

  MongoClient.connect(mongodburi, {
    poolSize: 1,
    useUnifiedTopology: true,
  }, function(err, client) {
    client.db("test").dropDatabase(() => {
      // Start the proxy
      proxy.start(callback);
    });
  });
}

exports['Should correctly connect to proxy'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('mongodb').MongoClient;

    // Url for connection to proxy
    //var url = 'mongodb://localhost:51000/test';
    const url = 'mongodb://root:password123@localhost:51000/test?authMechanism=SCRAM-SHA-1&authSource=admin';

    // Connect to mongodb
    MongoClient
      .connect(url, {poolSize: 1, useUnifiedTopology: true})
      .then(async db => {
          try {
              const collection = db.db().collection('t1');
              // Perform an inserts
              const insertResult = await collection.insertMany([{a: 1}, {b: 1}, {c: 1}, {d: 1}])
              test.equal(1, insertResult.result.ok);
              test.equal(4, insertResult.result.n);
              const docs = await collection.find({}).batchSize(2).toArray();
              test.notEqual(0, docs.length);
              db.close();
              test.done();
          } catch (e) {
              console.log("Simple test failed",e)
              db.close();
              test.done();
          }
      }).catch(err => {
        console.log(err);
        test.equal(null, err)
       });
  }
}

exports['Concurrent cursors'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('mongodb').MongoClient;

    //var url = 'mongodb://localhost:51000/test';
    const url = 'mongodb://root:password123@localhost:51000/test?authMechanism=SCRAM-SHA-1&authSource=admin';


    // Connect to mongodb
    MongoClient
      .connect(url, {poolSize: 1, useUnifiedTopology: true})
      .then(async db => {
          const collection = db.db().collection('t2');
          const insertResult = await collection.insertMany([{a:1}, {b:1}, {c:1}, {d:1}])
          test.equal(1, insertResult.result.ok);
          test.equal(4, insertResult.result.n);

          const total = 10;
          let numberLeft = total;
          for(let i = 0; i < total; i++) {
            collection.find({}).batchSize(2).toArray((err, docs) => {
              test.equal(null, err);
              test.notEqual(0, docs.length);
              numberLeft--;

              if(numberLeft === 0) {
                db.close();
                test.done();
              }
            });
          }
      }).catch(err => test.equal(null, err));
  }
}

exports['Should correctly connect to proxy and use readPreference secondary'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('mongodb').MongoClient;

    // Url for connection to proxy
    //const url = 'mongodb://localhost:51000/test?readPreference=secondary';
    const url = 'mongodb://root:password123@localhost:51000/test?authMechanism=SCRAM-SHA-1&authSource=admin&readPreference=secondary';

    // Connect to mongodb
    MongoClient
        .connect(url, {poolSize: 1, useUnifiedTopology: true})
        .then(async db => {
          const collection = db.db().collection('t3');
          // Perform an inserts
          const insertResult = await collection.insertMany([{a:1}, {b:1}, {c:1}, {d:1}])
          test.equal(1, insertResult.result.ok);
          test.equal(4, insertResult.result.n);
          const docs = await collection.find({}).batchSize(2).toArray();
          test.notEqual(0, docs.length);
          db.close();
          test.done();
        }).catch(err => test.equal(null, err));
  }
}

exports['Concurrent cursors against secondary'] = {
  metadata: { requires: { } },

  // The actual test we wish to run
  test: function(configuration, test) {
    var MongoClient = require('mongodb').MongoClient;

    // Url for connection to proxy
    //const url = 'mongodb://localhost:51000/test?readPreference=secondary';
    const url = 'mongodb://root:password123@localhost:51000/test?authMechanism=SCRAM-SHA-1&authSource=admin&readPreference=secondary';

    // Connect to mongodb
    MongoClient
        .connect(url, {poolSize: 1, useUnifiedTopology: true})
        .then(async db => {
          const collection = db.db().collection('t4');
          const insertResult = await collection.insertMany([{a:1}, {b:1}, {c:1}, {d:1}])
          test.equal(1, insertResult.result.ok);
          test.equal(4, insertResult.result.n);

          const total = 10;
          let numberLeft = total;
          for(let i = 0; i < total; i++) {
            collection.find({}).batchSize(2).toArray((err, docs) => {
              test.equal(null, err);
              test.notEqual(0, docs.length);
              numberLeft--;

              if(numberLeft === 0) {
                db.close();
                test.done();
              }
            });
          }
        }).catch(err => test.equal(null, err));
  }
}
