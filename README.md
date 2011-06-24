# Gearman-Worker

**Gearman-Worker** is a Node.JS client/worker module for Gearman.

## Usage

### Worker

    var Gearman = require("./gearman");

    worker= new Gearman();
    worker.addServer("localhost", 7003);
    worker.addFunction("upper", function(payload, callback){
        var response =  payload.toString("utf-8").toUpperCase(),
            error = null;
            
        callback(error, response);
    });
    
### Client

    var Gearman = require("./gearman");

    client = new Gearman();
    client.addServer("localhost", 7003);

    var job = client.submitJob("upper", "hello world!", {encoding:'utf-8'});

    job.on("complete", function(handle, response){
        console.log(response); // HELLO WORLD!
        client.end();
    });
