# NB !!ABANDONWARE!!

I'm not maintaining this repo anymore. Sorry.

# Gearnode

**Gearnode** is a Node.JS client/worker module for Gearman.

**NB!** check out another Gearman library of mine [node-gearman](https://github.com/andris9/node-gearman) which is somewhat more simple than this one.

## Installation

    npm install gearnode
    
## Tests

Tests are run with *nodeunit*

    ./run_tests.sh

Tests expect a Gearman daemon running on port 7003

## Usage

### Worker

    var Gearnode = require("gearnode");

    worker = new Gearnode();
    worker.addServer(); // use localhost
    
    worker.addFunction("upper", "utf-8", function(payload, job){
        var response =  payload.toUpperCase();
        job.complete(response);
    });
    
### Client

    var Gearnode = require("gearnode");

    client = new Gearnode();
    client.addServer();

    var job = client.submitJob("upper", "hello world!", {encoding:'utf-8'});

    job.on("complete", function(handle, response){
        console.log(response); // HELLO WORLD!
        client.end();
    });

## API

### Require Gearman library

    var Gearnode = require("gearnode");

### Create a new Gearnode worker/client

    var gearnode = new Gearnode();
    
### Add a server

    gearnode.addServer([host][, port])

Where

  * **host** is the hostname of the Gearman server (defaults to localhost);
  * **port** is the Gearman port (default is 4730)

Example

    gearnode.addServer(); // use default values
    gearnode.addServer("gearman.lan", 7003);
    
### Register for exceptions

Exceptions are not sent to the client by default. To turn these on use the following command. 

    gearnode.getExceptions([callback])
    
Where

  * **callback** is an optional callback function with params *error* if an error occured and *success* which is true if the command succeeded

Example

    client = new Gearnode();
    client.addServer(); // use default values
    client.getExceptions();
    
    job = client.submitJob("reverse", "Hello world!");
    
    job.on("error", function(exception){
        console.log(exception);
    });
    
### Assign an ID for the Worker

Worker ID's identify unique workers for monitoring Gearman. 

    gearnode.setWorkerId(id)
    
Where

  * **id** is a string that will act as the name for the worker

Example

    worker = new Gearnode();
    worker.addServer(); // use default values
    worker.setWorkerId("my_worker");

### Submit a job

    gearnode.submitJob(func, payload[, options])
    
Where

  * **func** is the function name
  * **payload** is either a String or a Buffer value
  * **options** is an optional options object (see below)
  
Possible option values

  * **encoding** - indicates the encoding for the job response (default is Buffer). Can be "utf-8", "ascii", "base64", "number" or "buffer"
  * **background** - if set to true, detach the job from the client (complete and error events will not be sent to the client)
  * **priority** - indicates the priority of the job. Possible values "low", "normal" (default) and "high"
  
Returns a Client Job object with the following events

  * **created** - when the function is queued by the server (params: handle value) 
  * **complete** - when the function returns (params: response data in encoding specified by the options value)
  * **fail** - when the function fails (params: none)
  * **error** - when an exception is thrown (params: error string)
  * **warning** - when a warning is sent (params: warning string)
  * **status** - when the status of a long running function is updated (params: numerator numbber, denominator number)
  * **data** - when partial data is available (params: response data in encoding specified by the options value)
  
Example

    client = new Gearnode();
    client.addServer(); // use default values
    worker.getExceptions();
    
    job = client.submitJob("reverse", "Hello world!", {encoding:"utf-8"});
    
    job.on("complete", function(response){
        console.log(response); // !dlrow olleH
    });
    
    job.on("fail", function(){
        console.log("Job failed :S");
    });
    
### Create a worker function

    gearnode.addFunction(func_name[, encoding], worker_func)
    
Where

  * **func_name** is the name of the function to be created
  * **endocing** is the input encoding (default is buffer)
  * **worker_func** is the actual worker function

#### Worker function

    worker_func = function(payload, job)
    
Where

  * **payload** is the data sent by the client and in the encoding specified with *addFunction*
  * **job** is a Gearman Job object that can be used to send data back
  
#### Worker Job object

Worker Job object has the following methods

  * **complete(response)** - send the result of the function back to the client
  * **error(error)** - throw an exception (and end the job with *failed* status)
  * **fail()** - end the function without response data when the function failed
  * **warning(warning)** - send a warning message to the client
  * **data(response)** - send a partial response data to the client
  * **setStatus(numerator, denominator)** - send a progress event to the client
  
#### Example

    var Gearnode = require("gearnode");
    
    var worker = new Gearnode();
    worker.addServer();
    
    worker.addFunction("sqr", "number", function(payload, job){
        if(payload < 0){
            job.warning("Used number is smaller than zero!");
        }
        job.complete(payload * payload);
    });

### Detect connection errors

When the connection is lost a "disconnect" event is emitted to the client/worker

    worker.addServer("gearman.lan");
    worker.on("disconnect", function(server){
        console.log("Connection lost from "+server_name);
    });

## License

MIT
