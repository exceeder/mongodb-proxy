const WireMessage = require('./wire_message')
    , ReplyMessage = require('./reply_message')
    , ReadPreference = require('mongodb').ReadPreference
    , MongoClient = require('mongodb').MongoClient
    , EventEmitter = require('events')
    , f = require('util').format
    , m = require('mongodb')
    , blacklist = require('./blacklist');


// Checks
const ismaster = Buffer.from('ismaster');
const isMaster = Buffer.from('isMaster');
const readPreference = Buffer.from('$readPreference');

/*
 * Buffer indexOf
 */
const bufferIndexOf = function(buf,search,offset){
    offset = offset||0;

    let m = 0;
    let s = -1;
    for(let i=offset; i<buf.length; ++i) {
        if(buf[i] === search[m]) {
            if(s === -1) s = i;
            ++m;
            if(m === search.length) break;
        } else {
            s = -1;
            m = 0;
        }
    }

    if (s > -1 && buf.length - s < search.length) return -1;
    return s;
};

/*
 * Connection wrapper
 */
class Connection extends EventEmitter {

    constructor(proxy, connection) {
        super();

        // The actual connection
        this.connection = connection;

        // Proxy server
        this.proxy = proxy;

        // Store logger
        this.logger = proxy.logger;

        // Get server config
        this.db = null; // MongoClient

        this.maxBsonMessageSize = 48000000;

        // Connections by cursorId
        this.connections = {};

        // Create a MongoClient
        MongoClient.connect(proxy.options.uri, {
            poolSize: 1,
            useUnifiedTopology: true,
        }, (err, db) => this.onConnectionEstablished(err, db, connection));
    }

    onConnectionEstablished(err, db, connection) { //db is a client here
        const self = this;
        // Connection details
        const remoteAddress = this.connection.remoteAddress;
        const remotePort = this.connection.remotePort;

        if (err) {
            if (self.logger.isError()) self.logger.error(f('failed to connect to MongoDB topology for client connection %s:%s'
                , remoteAddress
                , remotePort));
            return connection.destroy();
        }

        // Save the db reference
        self.db = db;

        // Log info about mongodb connection
        if (self.logger.isInfo()) self.logger.info(f('correctly connected to MongoDB topology for client connection %s:%s'
            , remoteAddress
            , remotePort));

        // Unpack the mongodb-core
        if (self.db.topology instanceof m.Server) {
            self.topology = self.db.topology
        } else if (self.db.topology instanceof m.ReplSet) {
            self.topology = self.db.topology;
        } else if (self.db.topology instanceof m.Mongos) {
            self.topology = self.db.topology;
        } else {
            //Native topology
            self.topology = self.db.topology;
        }

        // Connection closed by peer
        connection.on('end', function () {
            if (self.logger.isInfo()) self.logger.info(f('connection closed from %s:%s'
                , remoteAddress
                , remotePort));
            // Shut down db connection
            db.close();
        });

        // Data handler
        connection.on('data', self.dataHandler(self));

        // Data handler
        connection.on('parseError', function (err) {
            if (self.logger.isError())
                self.logger.error(f('connection closed from from %s:%s due to parseError %s',
                    this.remoteAddress, this.remotePort, err));
            connection.destroy();
        });
    }

    replyWithMaster(requestId, responseTo) {
        const self = this;
        if (self.logger.isDebug())
            self.logger.debug(f('client sent ismaster command'));

        // Create the response document
        const ismasterResponse = {
            "ismaster": true,
            "msg": "isdbgrid",
            "maxBsonObjectSize": 16777216,
            "maxMessageSizeBytes": 48000000,
            "maxWriteBatchSize": 1000,
            "localTime": new Date(),
            "maxWireVersion": 3,
            "minWireVersion": 0,
            "ok": 1
        };

        // Create a new Message Response and reply to the ismaster
        const reply = new ReplyMessage(self.topology.bson, requestId, responseTo, [ismasterResponse]);
        // Write it to the connection
        try {
            self.connection.write(reply.toBin());
        } catch (err) {
            if (self.logger.isError())
                self.logger.error(f('failed to write to client connection %s:%s'
                    , self.connection.remoteAddress, self.connection.remotePort));

        }
    }

    replyWithError(requestId, responseTo, message) {
        const self = this;
        if (self.logger.isDebug())
            self.logger.debug(f('replying error'));

        // Create the response document
        const errorResponse = {
            "ok": 1,
            "n": 0,
            "writeErrors": [
                {
                    "index": 0,
                    "code": 20,
                    "errmsg": message
                }
            ]
        };

        // Create a new Message Response and reply to the ismaster
        const reply = new ReplyMessage(self.topology.bson, requestId, responseTo, [errorResponse]);
        // Write it to the connection
        try {
            self.connection.write(reply.toBin());
        } catch (err) {
            if (self.logger.isError())
                self.logger.error(f('failed to write to client connection %s:%s'
                    , self.connection.remoteAddress, self.connection.remotePort));

        }
    }

    blacklisted(query) {
        for (let j = 0; j < blacklist.length; j++) {
            if (query[blacklist[j]]) {
                return true;
            }
        }
        return false;
    }

    messageHandler(data) {
        const self = this;
        if (self.logger.isDebug())
            self.logger.debug(f('client message decoded: [%s]', data.toString('hex')));

        // Get the request Id
        const message = new WireMessage(data, self.topology.bson);

        // We need this to build a response message
        const requestId = message.requestID();
        const responseTo = message.responseTo();
        const opCode = message.opCode();

        //
        // Return the handler
        const getRespHandler = (connection, _server, clientMessage, _clientConnection) => {
            return (data) => {
                // Extract WireProtocol information
                const responseMessage = new WireMessage(data.raw, self.topology.s.bson);
                // if (responseMessage.responseTo() !== clientMessage.requestID()) {
                //     console.log("reply ID mismatch: "+responseMessage.responseTo() + " != "+clientMessage.requestID());
                //     return;
                // }
                console.log("<===:" + responseMessage + "\n");
                // Extract the cursor
                const cursorID = responseMessage.responseCursorID();
                //If we have a zero cursorId delete any pinned connections
                if (cursorID.isZero()
                    && (clientMessage.opCode() === 2005)) {
                    const connId = clientMessage.getMoreCursorID();
                    delete self.connections[connId];
                } else if ((!self.connections[cursorID])
                    && (clientMessage.opCode() === 2004 || clientMessage.opCode() === 2005)) {
                    self.connections[cursorID] = {
                        server: _server
                    }
                }

                // Return the result
                try {
                    _clientConnection.write(data.raw);
                } catch (err) {
                    if (self.logger.isError())
                        self.logger.error(f('failed to write to client connection %s:%s'
                            , _clientConnection.remoteAddress, _clientConnection.remotePort));

                }
            }
        };

        // Check if we have an ismaster command
        if (bufferIndexOf(data, isMaster, 0) !== -1 || bufferIndexOf(data, ismaster, 0) !== -1) {
            self.replyWithMaster(requestId, responseTo);
        } else {
            // No read preference
            let preference = null;
            // Read preference index
            let rIndex = bufferIndexOf(data, readPreference, 0);

            // Check if we have a $readpreference
            if (rIndex !== -1) {
                // We need to snip out the bson object and decode it to know the routing
                // of the query, locate the length part of the doc
                rIndex = rIndex + '$readPreference'.length + 1;
                // Decode the read preference doc length
                const readPreferenceDocLength = data[rIndex] | data[rIndex + 1] << 8 | data[rIndex + 2] << 16 | data[rIndex + 3] << 24;
                // Deserialize bson of read preference doc
                const doc = self.topology.bson.deserialize(data.slice(rIndex, rIndex + readPreferenceDocLength));
                // Create the read Preference
                preference = new ReadPreference(doc.mode, doc.tags);
            }

            // Client message
            const clientMessage = message; //new WireMessage(data);
            console.log("===>:" + clientMessage);
            let server = null;

            //We have a OP_GETMORE pick the right server callback pool
            if (clientMessage.opCode() === 2005) {
                const cname = clientMessage.getMoreCursorID().toString();
                if (cname && self.connections[cname]) {
                    server = self.connections[cname].server;
                } else {
                    this.topology.selectServer({readPreference:preference}, (err, srv) => {
                        console.log("2005 Single Server selected " + srv.name);
                        server = srv;
                        self.connections[cname] = { server: server };
                    });
                }

            } else if (!self.proxy.options.rw && (opCode === 2001 || opCode === 2002 || opCode === 2006)) {
                //nop
                //insert, delete and update won't work
                console.log("!!!!! PROTECTED COMMAND !!!!! will skip opCode =>" + opCode);
                self.replyWithError(requestId, responseTo, "Cannot execute protected opCode " + opCode);
                return;
            } else {
                if (opCode === 2004) {
                    const q = clientMessage.getQuery();
                    if (self.proxy.options.rw && self.blacklisted(q.query)) {
                        console.log("!!!!! PROTECTED COMMAND !!!!! will skip =>" + blacklist[j]);
                        self.replyWithError(requestId, responseTo, "Cannot execute protected command " + blacklist[j]);
                        return;
                    }
                }
                try {

                    self.topology.selectServer({readPreference:preference}, (err, srv) => {
                        console.log("Server selected " + srv.name);
                        server = srv;
                    });
                } catch (err) {
                    if (self.logger.isError())
                        self.logger.error(f('routing OP_CODE=%s with readPreference [%s] to a server failed with error = [%s]'
                            , clientMessage.opCode(), JSON.stringify(preference), err));
                    return;
                }
            }

            if (self.logger.isDebug())
                self.logger.debug(f('routing OP_CODE=%s with readPreference [%s] to server %s'
                    , clientMessage.opCode(), JSON.stringify(preference), (server ? server.name : '?')));

            // No server able to service the result
            if (server === null) {
                if (self.logger.isError() || self.logger.isDebug())
                    self.logger.error(f('routing OP_CODE=%s with readPreference [%s] to a server failed due to no server found for readPreference'
                        , clientMessage.opCode(), JSON.stringify(preference)));
                return;
            }

            // Associate responses with specfic connections
            const callbackFunction = function (_server, _clientMessage, _clientConnection) {
                // Store a new connection if needed
                let connection = null;

                // Client message
                const clientMessage = new WireMessage(_clientMessage);
                // If we have a getmore
                if (clientMessage.opCode() === 2005) {
                    // Unpack the cursor Id
                    const curs = clientMessage.getMoreCursorID();
                    // Use the pinned connection
                    try {
                        const srv = self.connections[curs.toString()];
                        srv.server.s.pool.withConnection((err, conn, cb) => {
                            if (err) {
                                console.log(err);
                                return;
                            }
                            //console.log("querying "+JSON.stringify(conn.description))
                            connection = conn;
                            connection.once('message', getRespHandler(conn, _server, clientMessage, _clientConnection));
                            connection.stream.write(_clientMessage);
                            cb();
                        })
                    } catch (err) {

                        if (self.logger.isError())
                            self.logger.error(f('failed to write to client connection %s:%s'
                                , self.connections[curs.toString()]
                                , err));
                        return;
                    }
                } else if (clientMessage.opCode() === 2004) {
                    // Get the connection
                    // Write the data to the connection
                    server.s.pool.withConnection((err, conn, cb) => {
                        if (err) {
                            console.log(err);
                            return;
                        }
                        //console.log("querying "+JSON.stringify(conn.description))
                        connection = conn;
                        connection.once('message', getRespHandler(conn, _server, clientMessage, _clientConnection));
                        connection.stream.write(_clientMessage);
                        cb();
                    })
                }


            }

            //todo enable retries
            callbackFunction(server, data, this.connection);
        }
    }

    /*
     * Read wire protocol message off the sockets
     */
    dataHandler(self) {
        return (data) => {
            // Parse until we are done with the data
            while (data.length > 0) {
                // If we still have bytes to read on the current message
                if (self.bytesRead > 0 && self.sizeOfMessage > 0) {
                    data = self.readRemaining(self, data)
                } else  if (self.stubBuffer && self.stubBuffer.length > 0) {
                    // Stub buffer is kept in case we don't get enough bytes to determine the
                    // size of the message (< 4 bytes)
                    data = self.readIntoStubBuffer(self, data)
                } else if (data.length > 4) {
                    // Retrieve the message size
                    // const sizeOfMessage = data.readUInt32LE(0);
                    const sizeOfMessage = data[0] | data[1] << 8 | data[2] << 16 | data[3] << 24;
                    // If we have a negative sizeOfMessage emit error and return
                    if (sizeOfMessage < 0 || sizeOfMessage > self.maxBsonMessageSize) {
                        const errorObject = {
                            err: "socketHandler", trace: '', bin: self.buffer, parseState: {
                                sizeOfMessage: sizeOfMessage,
                                bytesRead: self.bytesRead,
                                stubBuffer: self.stubBuffer
                            }
                        };
                        // We got a parse Error fire it off then keep going
                        self.connection.emit("parseError", errorObject, self);
                        return;
                    }

                    // Ensure that the size of message is larger than 0 and less than the max allowed
                    if (sizeOfMessage > data.length && sizeOfMessage > 4 && sizeOfMessage < self.maxBsonMessageSize) {
                        data = self.readMessage(self, sizeOfMessage, data)
                    } else if (sizeOfMessage === data.length && sizeOfMessage > 4 && sizeOfMessage < self.maxBsonMessageSize) {
                        data = self.readMessageExactLength(data, self)
                    } else if (sizeOfMessage <= 4 || sizeOfMessage > self.maxBsonMessageSize) {
                        data = self.errorMessageSize(data, sizeOfMessage, self)
                    } else {
                        data = self.readMessageSlice(data, sizeOfMessage, self)
                    }
                } else {
                    // Create a buffer that contains the space for the non-complete message
                    self.stubBuffer = new Buffer(data.length);
                    // Copy the data to the stub buffer
                    data.copy(self.stubBuffer, 0);
                    // Exit parsing loop
                    data = new Buffer(0);
                }
            }
        }
    }

    readMessageSlice(data, sizeOfMessage, self) {
        const emitBuffer = data.slice(0, sizeOfMessage);
        // Reset state of buffer
        self.buffer = null;
        self.sizeOfMessage = 0;
        self.bytesRead = 0;
        self.stubBuffer = null;
        // Copy rest of message
        data = data.slice(sizeOfMessage);
        // Emit the message
        self.messageHandler(emitBuffer, self);
        return data
    }

    errorMessageSize(data, sizeOfMessage, self) {
        const errorObject = {
            err: "socketHandler", trace: null, bin: data, parseState: {
                sizeOfMessage: sizeOfMessage,
                bytesRead: 0,
                buffer: null,
                stubBuffer: null
            }
        };
        // We got a parse Error fire it off then keep going
        self.connection.emit("parseError", errorObject, self);

        // Clear out the state of the parser
        self.resetBufferState();
        // Exit parsing loop
        data = new Buffer(0);
        return data
    }

    readMessageExactLength(data, self) {
        try {
            const emitBuffer = data;
            self.resetBufferState();
            // Exit parsing loop
            data = Buffer.alloc(0);
            // Emit the message
            self.messageHandler(emitBuffer, self);
        } catch (err) {
            const errorObject = {
                err: "socketHandler", trace: err, bin: self.buffer, parseState: {
                    sizeOfMessage: self.sizeOfMessage,
                    bytesRead: self.bytesRead,
                    stubBuffer: self.stubBuffer
                }
            };
            // We got a parse Error fire it off then keep going
            self.connection.emit("parseError", errorObject, self);
        }
        return data
    }

    readMessage(self, sizeOfMessage, data) {
        self.buffer = Buffer.alloc(sizeOfMessage);
        // Copy all the data into the buffer
        data.copy(self.buffer, 0);
        // Update bytes read
        self.bytesRead = data.length;
        // Update sizeOfMessage
        self.sizeOfMessage = sizeOfMessage;
        // Ensure stub buffer is null
        self.stubBuffer = null;
        // Exit parsing loop
        data = Buffer.alloc(0);
        return data
    }

    readIntoStubBuffer(self, data) {
        // If we have enough bytes to determine the message size let's do it
        if (self.stubBuffer.length + data.length > 4) {
            // Pre-pad the data
            const newData = Buffer.alloc(self.stubBuffer.length + data.length);
            self.stubBuffer.copy(newData, 0);
            data.copy(newData, self.stubBuffer.length);
            // Reassign for parsing
            data = newData;
            self.resetBufferState()
        } else {
            // Add the the bytes to the stub buffer
            const newStubBuffer = Buffer.alloc(self.stubBuffer.length + data.length);
            // Copy existing stub buffer
            self.stubBuffer.copy(newStubBuffer, 0);
            // Copy missing part of the data
            data.copy(newStubBuffer, self.stubBuffer.length);
            // Exit parsing loop
            data = Buffer.alloc(0);
        }
        return data
    }

    readRemaining(self, data) {
        // Calculate the amount of remaining bytes
        const remainingBytesToRead = self.sizeOfMessage - self.bytesRead;
        // Check if the current chunk contains the rest of the message
        if (remainingBytesToRead > data.length) {
            // Copy the new data into the exiting buffer (should have been allocated when we know the message size)
            data.copy(self.buffer, self.bytesRead);
            // Adjust the number of bytes read so it point to the correct index in the buffer
            self.bytesRead += data.length;
            // Reset state of buffer
            data = Buffer.alloc(0);
        } else {
            // Copy the missing part of the data into our current buffer
            data.copy(self.buffer, self.bytesRead, 0, remainingBytesToRead);
            // Slice the overflow into a new buffer that we will then re-parse
            data = data.slice(remainingBytesToRead);

            // Emit current complete message
            try {
                const emitBuffer = self.buffer;
                self.resetBufferState()
                // Emit the buffer
                self.messageHandler(emitBuffer, self);
            } catch (err) {
                const errorObject = {
                    err: "socketHandler", trace: err, bin: self.buffer, parseState: {
                        sizeOfMessage: self.sizeOfMessage,
                        bytesRead: self.bytesRead,
                        stubBuffer: self.stubBuffer
                    }
                };
                // We got a parse Error fire it off then keep going
                self.connection.emit("parseError", errorObject, self);
            }
        }
        return data
    }

    resetBufferState() {
        this.buffer = null;
        this.sizeOfMessage = 0;
        this.bytesRead = 0;
        this.stubBuffer = null;
    }
}

module.exports = Connection;