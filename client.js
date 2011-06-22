var Gearman = require("./gearman");


client = new Gearman();
client.addServer("localhost", 7003);
setInterval(function(){
    client.submitJob("reverse", "test");
}, 1000)
