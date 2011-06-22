var Gearman = require("./gearman");


client = new Gearman();
client.addServer("localhost", 7003);

client.submitJob("reverse", "test");


client.on("created", function(handle){
    console.log("Job created as '"+handle+"'");
});

client.on("complete", function(handle, response){
    console.log("Job '"+handle+"' ready: '"+(response && response.toString("utf-8") || "")+"'");
    client.end();
});

client.on("fail", function(handle){
    console.log("Job '"+handle+"' failed");
});

client.on("exception", function(handle, message){
    console.log("Exception with '"+handle+"': '"+message+"'");
});