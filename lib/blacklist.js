/**
 * See https://docs.mongodb.com/manual/reference/command/#internal-commands for commands that go in here
 * @type {[string,string]}
 */
var blacklist = [
  'insert', 'update', 'delete', 'findAndModify', 'eval', '$eval'
];

module.exports = blacklist;