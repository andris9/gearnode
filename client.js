var Gearman = require("./gearman");

client = new Gearman();
client.addServer("localhost", 7003);

client.submitJob("reverse", "Hello world!", {encoding:"base64"});    

client.on("created", function(handle){
    console.log("Job created as '"+handle+"'");
});

client.on("complete", function(handle, response){
    console.log("Job '"+handle+"' ready: '"+response+"'");
    client.end();
});

client.on("fail", function(handle){
    console.log("Job '"+handle+"' failed");
    client.end();
});

client.on("exception", function(handle, message){
    console.log("Exception with '"+handle+"': '"+message+"'");
    client.end();
});