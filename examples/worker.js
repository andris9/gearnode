var Gearman = require("../lib/gearnode");

String.prototype.reverse = function(){
    splitext = this.split("");
    revertext = splitext.reverse();
    reversed = revertext.join("");
    return reversed;
};

worker= new Gearman();
worker.addServer("localhost", 7003);
worker.setWorkerId("testkast");

worker.addFunction("reverse", "utf-8", function(payload, job){
    var str = payload,
        reversed = str.reverse();
    
    setTimeout(function(){
        job.data("data part");
        
        setTimeout(function(){
            job.warning("something strange happened!");
            
            setTimeout(function(){
                job.setStatus(50, 100);
            
                setTimeout(function(){
                    
                    job.complete(reversed);
                
                },500);
                
            },500);
        },500);
    },500);
});

worker.addFunction("sqr", "number", function(payload, job){
    if(payload < 0){
        job.warning("Used number is smaller than zero!");
    }
    job.complete(payload * payload);
});