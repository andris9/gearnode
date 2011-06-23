var Gearman = require("./gearman");

String.prototype.reverse = function(){
    splitext = this.split("");
    revertext = splitext.reverse();
    reversed = revertext.join("");
    return reversed;
}

worker= new Gearman();
worker.addServer("localhost", 7003);
worker.addFunction("reverse", function(payload){
    var str = payload.toString("utf-8");
    return str.reverse();
});

worker.addFunction("reverse2", function(payload){
    var str = payload.toString("utf-8");
    return str.reverse();
});