/**
 * See https://docs.mongodb.com/manual/reference/command/#internal-commands for commands that go in here
 * @type {[string,string]}
 */
const blacklist = [
    'insert', 'insertOne', 'insertMany',
    'update', 'updateOne', 'updateMany',
    'delete', 'deleteOne', 'deleteMany',
    'findAndModify',
    'eval', '$eval',
    'create', 'reIndex', 'drop',
    'renameCollection'
];

module.exports = blacklist;