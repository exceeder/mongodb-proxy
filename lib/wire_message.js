const Long = require('mongodb').Long;

class WireMessage {

    constructor(data, bson) {
        this.data = data;
        const len = this.messageLength();
        if (len !== data.length) {
            console.log("ERROR in wire msg! Received:" + data.length + " expected:" + len + ", opCode:" + this.opCode());
        }
        this.bson = bson;
    }

    messageLength() {
        const index = 0;
        // Return the messageLength
        return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    }

    requestID() {
        const index = 4;
        // Return the messageLength
        return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    }

    responseTo() {
        const index = 8;
        // Return the messageLength
        return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    }

    opCode() {
        const index = 12;
        // Return the messageLength
        return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    }

    getInt(idx) {
        const index = idx;
        // Return the messageLength
        return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    }

    getLong(idx) {
        let index = idx;
        // Unpack the cursor
        const lowBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
        index = index + 4;
        const highBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;

        // Create long object
        return new Long(lowBits, highBits);
    }

    getStr(idx) {
        const start = idx;
        let end = idx;
        while (end < this.data.length && this.data[end] !== 0) {
            end++;
        }
        // Return the messageLength
        return this.data.toString('utf8', start, end);
    }

    responseResponseFlags() {
        const index = 16;
        // Return the messageLength
        return this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
    }

    responseCursorID() {
        let index = 20;

        // Unpack the cursor
        const lowBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
        index = index + 4;
        const highBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
        //index = index + 4;

        // Create long object
        return new Long(lowBits, highBits);
    }

    getMoreCursorID() {
        let index = this.data.length - 8;
        // Unpack the cursor
        const lowBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
        index = index + 4;
        const highBits = this.data[index] | this.data[index + 1] << 8 | this.data[index + 2] << 16 | this.data[index + 3] << 24;
        //index = index + 4;

        // Create long object
        return new Long(lowBits, highBits);
    }

    unbson(pos) {
        if (pos >= this.data.length) return {error: "bad length:" + this.data.length};
        try {
            return this.bson.deserialize(this.data.slice(pos));
        } catch (e) {
            return {error: e};
        }
    }

    getQuery() {
        const ret = {opCode: this.opCode()};
        let next = 16;
        ret.flags = "0x" + (this.getInt(next) >>> 0).toString(16);
        next += 4;
        const str = this.getStr(next);
        ret.db = str;
        next += str.length + 1;
        ret.numberToSkip = this.getInt(next);
        next += 4;
        ret.numberToReturn = this.getInt(next);
        next += 4;
        if (this.bson) {
            ret.query = this.unbson(next);
        }
        return ret;
    }

    toString() {
        const opc = this.opCode();
        let dbname;
        const ret = {
            len: this.messageLength(),
            opCode: opc,
            requestId: this.requestID(),
            responseTo: this.responseTo()
        };
        if (opc === 2005) {
            ret.ZERO = this.getInt(16);               // 0 - reserved for future use
            dbname = this.getStr(20);
            ret.db = dbname;
            ret.numberToReturn = this.getInt(20 + dbname.length + 1);
            ret.getMoreCursorId = this.getLong(24 + dbname.length + 1);
        } else if (opc === 1) {
            ret.flags = (this.getInt(16) >>> 0).toString(16);
            ret.responseCursorId = this.responseCursorID();
            ret.startingFrom = this.getInt(28);
            ret.numberReturned = this.getInt(32);
            if (this.bson && ret.numberReturned > 0) {
                ret.response = this.unbson(36);
            }
            //ret.data = String(this.data);
        } else if (opc === 2004) {
            ret.flags = (this.getInt(16) >>> 0).toString(16);
            dbname = this.getStr(20);
            ret.db = dbname;
            ret.numberToSkip = this.getInt(20 + dbname.length + 1);
            ret.numberToReturn = this.getInt(24 + dbname.length + 1);
            if (this.bson) {
                ret.query = this.unbson(28 + dbname.length + 1);
            }
        }
        return JSON.stringify(ret)
    }
}


module.exports = WireMessage;