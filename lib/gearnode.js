var GearmanConnection = require("./gearman-connection"),
    EventEmitter = require('events').EventEmitter,
    utillib = require("util");

/**
 * new Gearnode()
 * 
 * Creates a Gearnode object which can be used as a Gearman worker
 * or a client. This is an event emitter.
 **/
function Gearnode(){
    EventEmitter.call(this);
    
    this.server_names = [];
    this.servers = {};
    
    this.function_names = [];
    this.functions = {};
    
    this.workerId = null;
}
utillib.inherits(Gearnode, EventEmitter);

/**
 * Gearnode#addServer([server_name][, server_port]]) -> undefined
 * - server_name (String): hostname of the server, defaults to localhost
 * - server_port (Number): port for the Gearman server, defaults to 4730
 * 
 * Adds a Gearman server to the list. If this instance is used as a worker,
 * then all connected servers can assign jobs. If this instance is a client,
 * only the last one from the list is used.
 **/
Gearnode.prototype.addServer = function(server_name, server_port){
    server_name = (server_name || "127.0.0.1").toLowerCase().trim();
    server_port = server_port || 4730;
    
    if(this.servers[server_name]){
        return;
    }
    
    this.server_names.push(server_name);
    this.servers[server_name] = {
        name: server_name,
        port: server_port,
        connection: new GearmanConnection(server_name, server_port),
        functions: []
    };
    
    this.setupServerListeners(server_name);
    
    this.update(server_name);
};

/**
 * Gearnode#removeServer(server_name) -> undefined
 * - server_name (String): hostname of the Gearman server
 * 
 * Removes a server from the list of servers.
 **/
Gearnode.prototype.removeServer = function(server_name){
    var connection, pos;
    
    server_name = (server_name || "127.0.0.1").toLowerCase().trim();
    
    if(!this.servers[server_name]){
        return false;
    }
    
    connection = this.servers[server_name].connection;
    connection.closeConnection();
    
    if((pos = this.server_names.indexOf(server_name))>=0){
        this.server_names.splice(pos, 1);
    }
    
    delete this.servers[server_name];
    
    return true;
};

/**
 * Gearnode#end() -> undefined
 * 
 * Removes all listed servers
 **/
Gearnode.prototype.end = function(){
    for(var i=this.server_names.length-1; i>=0; i--){
        this.removeServer(this.server_names[i]);
    }
};

/**
 * Gearnode#setupServerListeners(server_name) -> undefined
 * - server_name (String): hostname of the Gearman server
 * 
 * This function sets up a set of listener for different events related
 * with the Gearman-Connection server instance.
 **/
Gearnode.prototype.setupServerListeners = function(server_name){
    
    this.servers[server_name].connection.on("error", function(err){
        console.log("Error with "+server_name);
        console.log(err.stack);
    });
    
    this.servers[server_name].connection.on("job", this.runJob.bind(this, server_name));
    
    this.servers[server_name].connection.on("created", (function(handle, options){
        if(options && options.job){
            options.job.emit("created", handle);
        }
    }).bind(this));
    
    this.servers[server_name].connection.on("complete", (function(handle, response, options){
        if(options && options.job){
            options.job.emit("complete", response);
        }
    }).bind(this));
    
    this.servers[server_name].connection.on("exception", (function(handle, error, options){
        if(options && options.job){
            options.job.emit("exception", error);
        }
    }).bind(this));
    
    this.servers[server_name].connection.on("warning", (function(handle, error, options){
        if(options && options.job){
            options.job.emit("warning", error);
        }
    }).bind(this));
    
    this.servers[server_name].connection.on("data", (function(handle, error, options){
        if(options && options.job){
            options.job.emit("data", error);
        }
    }).bind(this));
    
    this.servers[server_name].connection.on("status", (function(handle, numerator, denominator, options){
        if(options && options.job){
            options.job.emit("status", numerator, denominator);
        }
    }).bind(this));
    
    this.servers[server_name].connection.on("fail", (function(handle, options){
        if(options && options.job){
            options.job.emit("fail");
        }
    }).bind(this));
    
    this.servers[server_name].connection.on("disconnect", (function(){
        this.emit("disconnect", server_name);
    }).bind(this));
};

/**
 * Gearnode#update(server_name) -> undefined
 * - server_name (String): hostname of the Gearman server
 * 
 * Makes sure that a newly added server gets all the functions that
 * are supported by this worker instance
 **/
Gearnode.prototype.update = function(server_name){
    if(!server_name){
        return;
    }

    this.function_names.forEach((function(func_name){
        this.register(func_name, server_name);
    }).bind(this));
    
    if(this.workerId){
        this.setWorkerId(server_name, this.workerId);
    }
};

/**
 * Gearnode#register(func_name[, server_name]) -> undefined
 * - func_name (String): function name to be listed with the server
 * - server_name (String): hostname of the Gearman server
 * 
 * If server_name is set, adds a function to this server. If not set
 * adds the function to all servers
 **/
Gearnode.prototype.register = function(func_name, server_name){
    if(this.servers[server_name]){
        if(this.servers[server_name].functions.indexOf(func_name)<0){
            this.servers[server_name].connection.addFunction(func_name);
            this.servers[server_name].functions.push(func_name);
        }
    }else{
        this.server_names.forEach((function(server_name){
            if(server_name){
                this.register(func_name, server_name);
            }
        }).bind(this));
    }
};

/**
 * Gearnode#unregister(func_name[, server_name]) -> undefined
 * - func_name (String): function name to be removed from the server
 * - server_name (String): hostname of the Gearman server
 * 
 * If server_name is set, removes support for a function from this server.
 * If not set, removes the function from all servers
 **/
Gearnode.prototype.unregister = function(func_name, server_name){
    var pos;
    if(this.servers[server_name]){
        if((pos = this.servers[server_name].functions.indexOf(func_name))>=0){
            this.servers[server_name].connection.removeFunction(func_name);
            this.servers[server_name].functions.splice(pos, 1);
        }
    }else{
        this.server_names.forEach((function(server_name){
            if(server_name){
                this.unregister(func_name, server_name);
            }
        }).bind(this));
    }
};

// WORKER FUNCTIONS

/**
 * Gearnode#runJob(server_name, handle, func_name, payload[, uid]) -> undefined
 * - server_name (String): hostname of the Gearman server
 * - handle (String): unique handle ID for the job
 * - func_name (String): name of the function to be run
 * - payload: (Buffer | String): data sent by the client
 * - uid (String): unique id set by the client 
 * 
 * Initiated by "job" event from the Gearman-Connection. Executes the
 * worker function with payload and a GearmanWorker object. The latter is
 * used to report back when the job is completed (the job migh be async)
 **/
Gearnode.prototype.runJob = function(server_name, handle, func_name, payload, uid){
    uid = uid || null;
    if(this.functions[func_name]){
        
        var encoding = this.functions[func_name].encoding.toLowerCase() || "buffer";
        
        switch(encoding){
            case "utf-8":
            case "ascii":
            case "base64":
                payload = payload && payload.toString(encoding) || "";
                break;
            case "number":
                payload = Number(payload && payload.toString("ascii") || "") || 0;
                break;
            //case "buffer":
            default:
                // keep buffer
        }
        
        var job = new Gearnode.GearmanWorker(handle, server_name, this);
        this.functions[func_name].func(payload, job);
    }else{
        this.servers[server_name].connection.jobError(handle, "Function "+func_name+" not found");
    }
};

/**
 * Gearnode#setWorkerId([server_name], id) -> undeifned
 * - server_name (String): hostname of the Gearman server
 * - id (String): identifier for this worker instance
 * 
 * Registers an ID for this worker instance, useful when monitoring Gearman.
 * If server_name is set, set the Id for this server, otherwise set it for all.
 **/
Gearnode.prototype.setWorkerId = function(server_name, id){
    var pos;
    
    if(arguments.length<2){
        id = server_name;
        server_name = null;
    }
    if(!id){
        return false;
    }
    
    if(server_name){
        if(this.servers[server_name]){
            this.servers[server_name].connection.setWorkerId(id);
        }
    }else{
        this.workerId = id;
        this.server_names.forEach((function(server_name){
            if(server_name){
                this.setWorkerId(server_name, id);
            }
        }).bind(this));
    }
};

/**
 * Gearnode#addFunction(name[, encoding], func) -> undefined
 * - name (String): name of the function
 * - encoding (String): encoding of the payload when run as a job
 *                      defaults to "buffer"
 * - func (Function): the actual function to be run
 * 
 * Sets up a worker function. If a function with the same name already
 * exists, it is overwritten.
 **/
Gearnode.prototype.addFunction = function(name, encoding, func){
    if(!name){
        return false;
    }
    
    if(!func && typeof encoding=="function"){
        func = encoding;
        encoding = null;
    }else if(typeof func != "function"){
        return;
    }
    
    if(!(name in this.functions)){
        this.functions[name] = {
            func: func,
            encoding: encoding || "buffer"
        };
        this.function_names.push(name);
        this.register(name);
    }else{
        this.functions[name] = {
            func: func,
            encoding: encoding || "buffer"
        };
    }
};

/**
 * Gearnode#removeFunction(name) -> undefined
 * - name (String): name of the function to be removed
 * 
 * Removes a function from the available functions list
 **/
Gearnode.prototype.removeFunction = function(name){
    var pos;
    
    if(!name){
        return false;
    }
    
    if((pos = this.function_names.indexOf(name))>=0){
        delete this.functions[name];
        this.function_names.splice(pos, 1);
        this.unregister(name);
    }
};

// CLIENT FUNCTIONS

/**
 * Gearnode#getExceptions([server_name], callback) -> undefined
 * - server_name (String): hostname of the Gearman server
 * - callback (Function): function to be run when done
 * 
 * Notifies the server that worker exceptions should be delivered to
 * the client (not delivered by default). If server_name is specified
 * notifies only this server, otherwise notifies all.
 **/
Gearnode.prototype.getExceptions = function(server_name, callback){
    var pos;
    
    if(!callback && typeof server_name =="function"){
        callback = server_name;
        server_name = null;
    }
    
    if(server_name){
        if(this.servers[server_name]){

            this.servers[server_name].connection.getExceptions((function(err, success){
                if(callback){
                    return callback(err, success);
                }
                if(err){
                    console.log("Server "+server_name+" responded with error: "+(err.message || err));
                }else{
                    console.log("Exceptions are followed from "+server_name);
                }
            }).bind(this));
        }
    }else{
        this.server_names.forEach((function(server_name){
            if(server_name){
                this.getExceptions(server_name, callback);
            }
        }).bind(this));
    }
};

/**
 * Gearnode#submitJob(func_name, payload[, options]) -> Object
 * - func_name (String): name of the function to run
 * - payload (String | Buffer): data to be sent as the payload
 * - options (Object): options param
 * 
 * Initiates a job to be run by a worker. Returns GearmanJob object.
 **/
Gearnode.prototype.submitJob = function(func_name, payload, options){
    if(!func_name){
        return false;
    }

    if(!this.server_names.length){
        throw new Error("No Gearman servers specified");
    }
    
    var server = this.servers[this.server_names[this.server_names.length-1]];

    return new Gearnode.GearmanJob(func_name, payload, options, server);
};


// CLIENT JOB
/**
 * new Gearnode.GearmanJob(func_name, payload, options, server)
 * - func_name (String): name of the function to run
 * - payload (String | Buffer): data to be sent as the payload
 * - options (Object): options param
 * - server (Objet): server object from the servers list
 * 
 * Creates an event emitter object and submits the job to the server
 **/
Gearnode.GearmanJob = function(func_name, payload, options, server){
    EventEmitter.call(this);
    
    options = options || {};
    options.job = this;
    
    server.connection.submitJob(func_name, payload, options);
};
utillib.inherits(Gearnode.GearmanJob, EventEmitter);

// WORKER JOB

Gearnode.GearmanWorker = function(handle, server_name, gm){
    this.handle = handle;
    this.server_name = server_name;
    this.gm = gm;
};

Gearnode.GearmanWorker.prototype.complete = function(response){
    this.gm.servers[this.server_name].connection.jobComplete(this.handle, response);
};

Gearnode.GearmanWorker.prototype.data = function(data){
    this.gm.servers[this.server_name].connection.jobData(this.handle, data);
};

Gearnode.GearmanWorker.prototype.warning = function(warning){
    this.gm.servers[this.server_name].connection.jobWarning(this.handle, warning);
};

Gearnode.GearmanWorker.prototype.fail = function(){
    this.gm.servers[this.server_name].connection.jobFail(this.handle);
};

Gearnode.GearmanWorker.prototype.error = function(error){
    this.gm.servers[this.server_name].connection.jobError(this.handle, error);
};

Gearnode.GearmanWorker.prototype.setStatus = function(numerator, denominator){
    numerator = parseInt(numerator, 10) || 0;
    denominator = parseInt(denominator, 10) || 0;
    this.gm.servers[this.server_name].connection.jobStatus(this.handle, numerator, denominator);
};

module.exports = Gearnode;



