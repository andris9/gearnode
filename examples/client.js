var Gearman = require("../lib/gearnode");

client = new Gearman();
client.addServer("localhost", 7003);

client.getExceptions(function(err, success){
    console.log(success && "Registered for exceptions" || "No exceptions");
});

var job = client.submitJob("sqr", -25, {encoding:"number"});    

job.on("created", function(handle){
    console.log("Job created as '"+handle+"'");
});

job.on("complete", function(response){
    console.log("Job ready: '"+response+"'");
    client.end();
});

job.on("fail", function(){
    console.log("Job failed");
    client.end();
});

job.on("exception", function(message){
    console.log("Exception '"+message+"'");
});

job.on("warning", function(message){
    console.log("Warning '"+message+"'");
});

job.on("data", function(message){
    console.log("Data '"+message+"'");
});

job.on("status", function(nu, de){
    console.log("Status "+nu+" / "+de);
});

