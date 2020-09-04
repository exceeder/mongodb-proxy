const f = require('util').format
  , fs = require('fs');

class Logger {
  constructor(logger, level) {
    this.logger = logger;
    this.level = level;
  }

  isError() {
    return this.level === 'error' || this.level === 'info' || this.level === 'debug';
  }

  isInfo() {
    return this.level === 'info' || this.level === 'debug';
  }

  isDebug() {
    return this.level === 'debug';
  }

  error(message) {
    this.logger.log(f('[ERROR] %s %s', new Date(), message));
  }

  info(message) {
    this.logger.log(f('[INFO] %s %s', new Date(), message));
  }

  debug(message) {
    this.logger.log(f('[DEBUG] %s %s', new Date(), message));
  }

  static createFileLogger(file, level) {
    return new Logger(new FileLogger(file), level);
  }

  static createStdioLogger(level) {
    return new Logger(new StdioLogger(), level);
  }
}

/*
 * File logger
 */
var FileLogger = function(file) {
  this.file = file;
};

FileLogger.prototype.log = function(message) {
  fs.appendFileSync(this.file, f("%s\n", message));
};

/*
 * StdioLogger
 */
var StdioLogger = function() {  
};

StdioLogger.prototype.log = function(message) {
  console.log(message);
};

module.exports = Logger;