
function packInt(nr, bytelen){
    if(!bytelen){
        bytelen = Math.floor(Math.log(nr) / Math.floor(255)) + 1;
    }
    
    bytes = Buffer(Number(bytelen) || 4);
    
    for(var i=bytelen-1; i>=0; i--){
        bytes[i] = nr & (255);
        nr = nr >> 8;
    }
    
    return new Buffer(bytes);
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