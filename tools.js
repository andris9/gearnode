
function packInt(nr, bytelen){
    bytelen = Number(bytelen) || 4;
    bytes = Buffer(bytelen);
    for(var i=bytelen-1; i>=0; i--){
        bytes[i] = nr & (255);
        nr = nr >> 8;
    }
    return bytes;
}

function unpackInt(bytes){
    var nr = 0;
    for(var i=bytes.length-1; i >= 0; i--){
        nr += (Math.pow(256, bytes.length - i - 1)) * bytes[i];
    }
    return nr;
}

module.exports.packInt = packInt;
module.exports.unpackInt = unpackInt;