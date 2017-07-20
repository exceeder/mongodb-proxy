var Long = require('mongodb').Long;

var WireMessage = function (data, bson) {
    this.data = data;
    this.bson = bson;
};

WireMessage.prototype.messageLength = function () {
    var index = 0;
    // Return the messageLength
    return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
};

WireMessage.prototype.requestID = function () {
    var index = 4;
    // Return the messageLength
    return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
};

WireMessage.prototype.responseTo = function () {
    var index = 8;
    // Return the messageLength
    return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
};

WireMessage.prototype.opCode = function () {
    var index = 12;
    // Return the messageLength
    return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
};

WireMessage.prototype.getInt = function(idx) {
    var index = idx;
    // Return the messageLength
    return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
};

WireMessage.prototype.getLong = function(idx) {
    var index = idx;
    // Unpack the cursor
    var lowBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    index = index + 4;
    var highBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;

    // Create long object
    return new Long(lowBits, highBits);
};


WireMessage.prototype.getStr = function(idx) {
    var start = idx;
    var end = idx;
    while (end<this.data.length && this.data[end] !== 0) {
        end++;
    }
    // Return the messageLength
    return this.data.toString('utf8',start,end);
};

WireMessage.prototype.responseResponseFlags = function () {
    var index = 16;
    // Return the messageLength
    return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
};

WireMessage.prototype.responseCursorID = function () {
    var index = 20;

    // Unpack the cursor
    var lowBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    index = index + 4;
    var highBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    //index = index + 4;

    // Create long object
    return new Long(lowBits, highBits);
};

WireMessage.prototype.getMoreCursorID = function () {
    var index = this.data.length - 8;
    // Unpack the cursor
    var lowBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    index = index + 4;
    var highBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    //index = index + 4;

    // Create long object
    return new Long(lowBits, highBits);
};

WireMessage.prototype.getQuery = function() {
    var ret = {opCode:this.opCode()};
    ret.flags = (this.getInt(16) >>> 0).toString(16);
    var str = this.getStr(20);
    ret.db = str;
    ret.numberToSkip = this.getInt(20+str.length+1);
    ret.numberToReturn = this.getInt(24+str.length+1);
    if (this.bson) {
        ret.query = this.bson.deserialize(this.data.slice(28+str.length+1));
    }
    return ret;
};

WireMessage.prototype.toString = function () {
    var opc = this.opCode();
    var ret = {
        len: this.messageLength(),
        opCode: opc,
        requestId: this.requestID(),
        responseTo: this.responseTo()
    };
    if (opc === 2005) {
        ret.ZERO = this.getInt(16);               // 0 - reserved for future use
        var str = this.getStr(20);
        ret.db = str;
        ret.numberToReturn = this.getInt(20+str.length+1);
        ret.getMoreCursorId =  this.getLong(24+str.length+1);
    } else if (opc === 1) {
        ret.flags = (this.getInt(16) >>> 0).toString(16);
        ret.responseCursorId = this.responseCursorID();
        ret.startingFrom = this.getInt(28);
        ret.numberReturned = this.getInt(32);
        if (this.bson) {
            ret.response = JSON.stringify(this.bson.deserialize(this.data.slice(36)));
        }
        //ret.data = String(this.data);
    } else if (opc === 2004) {
        ret.flags = (this.getInt(16) >>> 0).toString(16);
        var str = this.getStr(20);
        ret.db = str;
        ret.numberToSkip = this.getInt(20+str.length+1);
        ret.numberToReturn = this.getInt(24+str.length+1);
        if (this.bson) {
            ret.query = JSON.stringify(this.bson.deserialize(this.data.slice(28+str.length+1)));
        }
    }
    return JSON.stringify(ret)
};

module.exports = WireMessage;