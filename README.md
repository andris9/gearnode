# Gearman-Worker

**Gearman-Worker** is a Node.JS client/worker module for Gearman.

## Usage

### Worker

    var Gearman = require("./gearman");

    worker= new Gearman();
    worker.addServer("localhost", 7003);
    worker.addFunction("upper", function(payload){
        return payload.toString("utf-8").toUpperCase();
    });
    
### Client

    var Gearman = require("./gearman");

    client = new Gearman();
    client.addServer("localhost", 7003);

    client.submitJob("upper", "hello world!");

    client.on("complete", function(handle, response){
        console.log(response.toString("utf-8"));
        client.end();
    });
