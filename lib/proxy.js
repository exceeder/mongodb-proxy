const MongoClient = require('mongodb').MongoClient
  , net = require('net')
  , f = require('util').format
  , Logger = require('./logger')
  , Connection = require('./connection');

class Proxy {

  constructor(options) {
    this.options = options;
    this.debug = options.debug;

    // Create log file based logger or stdio
    if (options.log_file) {
      this.logger = Logger.createFileLogger(options.log_file, options.log_level);
    } else {
      this.logger = Logger.createStdioLogger(options.log_level);
    }
    console.log("Debug level:" + options.log_level);
    console.log("R/W allowed:" + options.rw);
    console.log("Proxy port:" + options.port);
    console.log("Proxy tls:" + options.tls);
    console.log("Proxy socket timeout:" + options.socketTimeout);
  }

  start(callback) {
    // Create a new tcp server
    this.server = net.createServer( (conn) => {

      if (this.logger.isInfo() || this.logger.isDebug())
        this.logger.info(f('client connected from %s:%s', conn.remoteAddress, conn.remotePort));

      // Create connection object
      conn.setNoDelay(true)
      conn.setTimeout(this.options.socketTimeout, () => {
        this.logger.info("Client socket timed out! "+conn.remoteAddress+':'+conn.remotePort);
      })
      this.lastConnection = new Connection(this, conn);
    });

    // Listen to server
    this.server.listen(this.options.port, this.options.bind_to, callback);
    console.log("server started")
  }
}

module.exports = Proxy;