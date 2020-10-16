//base class for reply and wire message
class Message {
    constructor() {
        this.bytes = Buffer.alloc(0);
    }
    length() {
        return this.bytes.length;
    }
}