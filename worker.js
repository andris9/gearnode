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

worker.addFunction("reverse", function(payload, job){
    var str = payload.toString("utf-8"),
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

worker.addFunction("reverse2", function(payload){
    var str = payload.toString("utf-8");
    return str.reverse();
});