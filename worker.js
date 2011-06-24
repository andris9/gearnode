var Gearman = require("./gearman");

String.prototype.reverse = function(){
    splitext = this.split("");
    revertext = splitext.reverse();
    reversed = revertext.join("");
    return reversed;
}

worker= new Gearman();
worker.addServer("localhost", 7003);
worker.setWorkerId("testkast");

worker.addFunction("reverse", function(payload, callback){
    var str = payload.toString("utf-8"),
        reversed = str.reverse();
    
    if(callback){
        setTimeout(function(){
            callback(null, "andmepakett", "data");
            setTimeout(function(){
                callback(null, "probla tekkis!", "warning");
                setTimeout(function(){
                    callback(null, reversed);
                },500);
            },500);
        },500);
        
    }else{
        return str.reverse(reversed);
    }
});

worker.addFunction("reverse2", function(payload){
    var str = payload.toString("utf-8");
    return str.reverse();
});