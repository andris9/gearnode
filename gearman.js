var GearmanConnection = require("./gearman-connection"),
    EventEmitter = require('events').EventEmitter,
    utillib = require("util");

function Gearman(){
    EventEmitter.call(this);
    
    this.server_names = [];
    this.servers = {};
    
    this.function_names = [];
    this.functions = {};
}
utillib.inherits(Gearman, EventEmitter);

Gearman.prototype.addServer = function(server_name, server_port){
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
    
    this.servers[server_name].connection.on("error", function(err){
        console.log("Error with "+server_name);
        //console.log(err.message);
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
    
    this.servers[server_name].connection.on("fail", (function(handle, options){
        if(options && options.job){
            options.job.emit("fail");
        }
    }).bind(this));
    
    this.update(server_name);
}

Gearman.prototype.runJob = function(server_name, handle, func_name, payload, uid){
    uid = uid || null;
    if(this.functions[func_name]){
        this.functions[func_name](payload, (function(err, response, type){
            if(err){
                this.servers[server_name].connection.jobError(handle, err.message || err);
                return;
            }
            switch(type){
                case "warning":
                    this.servers[server_name].connection.jobWarning(handle, response);
                    break;
                case "data":
                    this.servers[server_name].connection.jobData(handle, response);
                    break;
                case "normal":
                default:
                    this.servers[server_name].connection.jobComplete(handle, response);
            }
            
        }).bind(this));
    }else{
        this.servers[server_name].connection.jobError(handle, "Function "+func_name+" not found");
    }
}

Gearman.prototype.removeServer = function(server_name){
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
}


Gearman.prototype.update = function(server_name){
    if(!server_name){
        return;
    }

    this.function_names.forEach((function(func_name){
        this.register(func_name, server_name);
    }).bind(this));
}

Gearman.prototype.register = function(func_name, server_name){
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
        }).bind(this))
    }
}

Gearman.prototype.unregister = function(func_name, server_name){
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
        }).bind(this))
    }
}


Gearman.prototype.getExceptions = function(server_name, callback){
    var pos;
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

    }else{
        this.server_names.forEach((function(server_name){
            if(server_name){
                this.getExceptions(server_name, callback);
            }
        }).bind(this))
    }
}

Gearman.prototype.addFunction = function(name, func){
    if(!name){
        return false;
    }
    
    if(!(name in this.functions)){
        this.functions[name] = func;
        this.function_names.push(name);
        this.register(name);
    }else{
        this.functions[name] = func;
        this.function_names.push(name);
    }
    
}

Gearman.prototype.removeFunction = function(name){
    var pos;
    
    if(!name){
        return false;
    }
    
    if((pos = this.function_names.indexOf(name))>=0){
        delete this.functions[name];
        this.function_names.splice(pos, 1);
        this.unregister(name);
    }
}

Gearman.prototype.end = function(){
    for(var i=this.server_names.length-1; i>=0; i--){
        this.removeServer(this.server_names[i]);
    }
}

Gearman.prototype.submitJob = function(func_name, payload, options){
    if(!func_name){
        return false;
    }

    var server = this.servers[this.server_names[this.server_names.length-1]];

    return new Gearman.GearmanJob(func_name, payload, options, server);
}

Gearman.GearmanJob = function(func_name, payload, options, server){
    EventEmitter.call(this);
    
    options = options || {};
    options.job = this;
    
    server.connection.submitJob(func_name, payload, options);
}
utillib.inherits(Gearman.GearmanJob, EventEmitter);

module.exports = Gearman;



