var netlib = require("net"),
    tools = require("./tools"),
    EventEmitter = require('events').EventEmitter,
    utillib = require("util");

module.exports = GearmanConnection;

function GearmanConnection(server, port){
    EventEmitter.call(this);
    
    this.server = server;
    this.port = port;
    
    this.command_queue = [];
    this.queue_pipe = [];
    
    this.queued_jobs = {};
    
    this.workerId = null;
    
    this.connected = false;
    this.processing = false;
    this.failed = false;
    
    this.remainder = false;
    
    this.debug = false;
}
utillib.inherits(GearmanConnection, EventEmitter);

GearmanConnection.packet_types = {
    CAN_DO: 1,
    CANT_DO: 2,
    RESET_ABILITIES: 3,
    PRE_SLEEP: 4,
    NOOP: 6,
    SUBMIT_JOB: 7,
    JOB_CREATED: 8,
    GRAB_JOB: 9,
    NO_JOB: 10,
    JOB_ASSIGN: 11,
    WORK_STATUS: 12,
    WORK_COMPLETE: 13,
    WORK_FAIL: 14,
    GET_STATUS: 15,
    ECHO_REQ: 16,
    ECHO_RES: 17,
    SUBMIT_JOB_BG: 18,
    ERROR: 19,
    STATUS_RES: 20,
    SUBMIT_JOB_HIGH: 21,
    SET_CLIENT_ID: 22,
    CAN_DO_TIMEOUT: 23,
    ALL_YOURS: 24,
    WORK_EXCEPTION: 25,
    OPTION_REQ: 26,
    OPTION_RES: 27,
    WORK_DATA: 28,
    WORK_WARNING: 29,
    GRAB_JOB_UNIQ: 30,
    JOB_ASSIGN_UNIQ: 31,
    SUBMIT_JOB_HIGH_BG: 32,
    SUBMIT_JOB_LOW: 33,
    SUBMIT_JOB_LOW_BG: 34,
    SUBMIT_JOB_SCHED: 35,
    SUBMIT_JOB_EPOCH: 36
};

GearmanConnection.packet_types_reversed = {
    "1": "CAN_DO",
    "2": "CANT_DO",
    "3": "RESET_ABILITIES",
    "4": "PRE_SLEEP",
    "6": "NOOP",
    "7": "SUBMIT_JOB",
    "8": "JOB_CREATED",
    "9": "GRAB_JOB",
    "10": "NO_JOB",
    "11": "JOB_ASSIGN",
    "12": "WORK_STATUS",
    "13": "WORK_COMPLETE",
    "14": "WORK_FAIL",
    "15": "GET_STATUS",
    "16": "ECHO_REQ",
    "17": "ECHO_RES",
    "18": "SUBMIT_JOB_BG",
    "19": "ERROR",
    "20": "STATUS_RES",
    "21": "SUBMIT_JOB_HIGH",
    "22": "SET_CLIENT_ID",
    "23": "CAN_DO_TIMEOUT",
    "24": "ALL_YOURS",
    "25": "WORK_EXCEPTION",
    "26": "OPTION_REQ",
    "27": "OPTION_RES",
    "28": "WORK_DATA",
    "29": "WORK_WARNING",
    "30": "GRAB_JOB_UNIQ",
    "31": "JOB_ASSIGN_UNIQ",
    "32": "SUBMIT_JOB_HIGH_BG",
    "33": "SUBMIT_JOB_LOW",
    "34": "SUBMIT_JOB_LOW_BG",
    "35": "SUBMIT_JOB_SCHED",
    "36": "SUBMIT_JOB_EPOCH"
};

GearmanConnection.param_count = {
    ERROR: ["string","string"],
    JOB_ASSIGN: ["string","string", "buffer"],
    JOB_ASSIGN_UNIQ: ["string","string", "string", "buffer"],
    JOB_CREATED: ["string", "string"],
    WORK_COMPLETE: ["string", "buffer"],
    WORK_EXCEPTION: ["string", "string"],
    WORK_WARNING: ["string", "string"],
    WORK_DATA: ["string", "buffer"],
    WORK_FAIL: ["string"],
    WORK_STATUS: ["string", "number", "number"]
};

GearmanConnection.prototype.sendCommand = function(command){
    if(!command){
        return false;
    }
    
    if(typeof command == "string"){
        command = {
            type: command
        };
    }
    
    if(!command.params){
        command.params = [];
    }
    this.command_queue.push(command);
    this.processQueue();
};


GearmanConnection.prototype.processQueue = function(){
    var command;
    
    // if no connection yet, open one
    if(!this.connected){
        return this.connect();
    }
    
    // get commands as FIFO
    if(this.command_queue.length){
        this.processing = true;
        command = this.command_queue.shift();
        process.nextTick(this.runCommand.bind(this, command));
    }else{
        this.processing = false;
    }
};

GearmanConnection.prototype.runCommand = function(command){
    if(!command || !command.type || !GearmanConnection.packet_types[command.type]){
        return;
    }
    this.send(command);
};

GearmanConnection.prototype.send = function(command){
    var magicREQ = new Buffer([0, 82, 69, 81]), //\0REQ
        type = tools.packInt(GearmanConnection.packet_types[command.type], 4),
        param, params = [], paramlen = 0, size = 0, buf, pos,
        i, len;
    
    // teeme parameetritest ükshaaval Buffer objektid ning loeme pikkused kokku
    for(i=0, len=command.params.length; i<len; i++){
        if(command.params[i] instanceof Buffer){
            params.push(command.params[i]);
            size += command.params[i].length;
        }else{
            param = new Buffer(String(command.params[i] || ""),"utf-8");
            params.push(param);
            size += param.length;
        }
    }
    
    // add the length for separator \0 bytes
    if(params.length>1){
        size += params.length - 1;
    }
    
    paramlen = tools.packInt(size, 4);
    
    // add the length for \0REQ 4B + type 4B + paramsize 4B
    size += 12;
    
    // loome objekti mis saata serverile
    buf = new Buffer(size);
    
    // kopeerime Magick baidid
    magicREQ.copy(buf, 0, 0, 4);
    
    // kopeerime käsu koodi
    type.copy(buf, 4, 0);
    
    // kopeerime parameetrite pikkuse 4B
    paramlen.copy(buf, 8, 0);
    
    // parameetrite jaoks on stardipositsioon 12s bait
    pos = 12;
    
    // kopeerime ükshaaval parameetrid
    for(i=0, len=params.length; i<len; i++){
        params[i].copy(buf, pos, 0);
        pos += params[i].length;
        // juhul kui tegu pole viimase elemendiga, lisa ka \0
        if(i<params.length-1){
            buf[pos]=0;
            pos++;
        }
    }
    
    // salvesta sisendkäsklus PIPELINE'i
    if(command.pipe){
        this.queue_pipe.push(command);
    }

    if(this.debug){    
        console.log("--> outgoing");
        console.log(command);
        console.log(buf);
    }

    // saada teele
    process.nextTick((function(){
        this.socket.write(buf, (function(){
            // kui saadetud, käivita järgmine
            // TODO: selle võiks ehk välja tõsta, käsud saab korraga saata
            if(this.debug){
                console.log("--> data sent");
            }
            this.processQueue();
        }).bind(this));    
    }).bind(this));
};

// CONNECTION COMMANDS

GearmanConnection.prototype.connect = function(){
    
    if(this.connected || this.connecting){
        // juhul kui ühendus on juba olemas käivita protsessimine
        if(this.connected && !this.processing){
            this.processQueue();
        }
        return false;
    }
    
    this.connecting = true;

    if(this.debug){
        console.log("connecting...");
    }
    this.socket = netlib.createConnection(this.port, this.server);
        
    this.socket.on("connect", (function(){
        this.connecting = false;
        this.connected = true;
    
        if(this.debug){
            console.log("connected!");
        }
        
        this.processQueue();
    }).bind(this));

    this.socket.on("end", this.close.bind(this));
    this.socket.on("error", this.close.bind(this));
    this.socket.on("close", this.close.bind(this));
    this.socket.on("timeout", this.close.bind(this));
    
    this.socket.on("data", this.receive.bind(this));
};

GearmanConnection.prototype.closeConnection = function(){
    if(this.connected){
        this.socket.end();
    }
};

GearmanConnection.prototype.close = function(){
    if(this.connected){
        if(this.socket){
            try{
                this.socket.end();
            }catch(E){}
        }
        this.connected = false;
        this.connecting = false;

        // kill all pending jobs
        var handles = Object.keys(this.queued_jobs), original;

        for(var i=0, len = handles.length; i<len; i++){
            original = this.queued_jobs[handles[i]] || {};
            delete this.queued_jobs[handles[i]];
            this.emit("fail", handles[i], original.options);
        }
        
        this.emit("disconnect");
    }
};

// WORKER COMMANDS

GearmanConnection.prototype.addFunction = function(func_name){
    this.sendCommand({
        type: "CAN_DO",
        params: [func_name]
    });
    
    if(this.debug){
        console.log("Registered for '"+func_name+"'");
    }
    
    this.sendCommand({
        type: "GRAB_JOB",
        pipe: true
    });
};

GearmanConnection.prototype.removeFunction = function(func_name){
    this.sendCommand({
        type: "CANT_DO",
        params: [func_name]
    });
    
    if(this.debug){
        console.log("Unregistered for '"+func_name+"'");
    }
};

GearmanConnection.prototype.removeAllFunction = function(){
    this.sendCommand("RESET_ABILITIES");
};

GearmanConnection.prototype.jobComplete = function(handle, payload){
    this.sendCommand({
        type: "WORK_COMPLETE",
        params: [handle, payload]
    });
    
    this.sendCommand("GRAB_JOB");    
};

GearmanConnection.prototype.jobFail = function(handle){
    this.sendCommand({
        type: "WORK_FAIL",
        params: [handle]
    });
    
    this.sendCommand("GRAB_JOB");
};

GearmanConnection.prototype.jobError = function(handle, message){
    this.sendCommand({
        type: "WORK_EXCEPTION",
        params: [handle, message]
    });
    
    this.jobFail(handle);
};

GearmanConnection.prototype.jobWarning = function(handle, data){
    this.sendCommand({
        type: "WORK_WARNING",
        params: [handle, data]
    });
};

GearmanConnection.prototype.jobData = function(handle, data){
    this.sendCommand({
        type: "WORK_DATA",
        params: [handle, data]
    });
};

GearmanConnection.prototype.jobStatus = function(handle, numerator, denominator){
    this.sendCommand({
        type: "WORK_STATUS",
        params: [handle, tools.packInt(numerator), tools.packInt(denominator)]
    });
};

// CLIENT COMMANDS

GearmanConnection.prototype.submitJob = function(func_name, payload, options){
    
    var command = ["SUBMIT_JOB"];
    
    switch(options.priority){
        case "low":
            command.push("LOW");
            break;
        case "high":
            command.push("HIGH");
            break;
    }
    
    if(options.background){
        command.push("BG");
    }
    
    this.sendCommand({
        type: command.join("_"),
        params: [func_name, options.uid || '', payload],
        options: options,
        pipe: true
    });
};

GearmanConnection.prototype.getExceptions = function(callback){
    this.sendCommand({
        type: "OPTION_REQ",
        params: ["exceptions"],
        callback: callback,
        pipe: true
    });
};

GearmanConnection.prototype.setWorkerId = function(id){
    this.workerId = id;
    this.sendCommand({
        type: "SET_CLIENT_ID",
        params: [id]
    });
};


// RECEIVER

GearmanConnection.prototype.receive = function(chunk){
    var buf = new Buffer((chunk && chunk.length || 0) + (this.remainder && this.remainder.length || 0)),
        magicRES = new Buffer([0, 82, 69, 83]), type = new Buffer(4), paramlen = new Buffer(4),
        action, piped, params;
    
    // nothing to do here
    if(!buf.length){
        return;
    }
    
    // if theres a remainder value, tie it together with the incoming chunk
    if(this.remainder){
        this.remainder.copy(buf, 0, 0);
        if(chunk){
            chunk.copy(buf, this.remainder.length, 0);
        }
    }else{
        if(chunk){
            chunk.copy(buf, 0, 0);
        }
    }
    
    // response needs to be at least 12 bytes
    // otherwise keep the current chunk as remainder
    if(buf.length<12){
        this.remainder = buf;
        return;
    }
    
    // check if the magic bytes are set (byte 0-3)
    // TODO: here should be some kind of mechanism to recover sync
    for(var i=0; i<4; i++){
        if(magicRES[i] != buf[i]){
            console.log("ERROR: out of sync!");
            this.close();
            return;
        }
    }
    
    // read the type of the command (bytes 4-7)
    buf.copy(type, 0, 4, 8);
    type = tools.unpackInt(type);
    type = GearmanConnection.packet_types_reversed[String(type)];
    
    // loe parameetrite pikkus
    buf.copy(paramlen, 0, 8, 12);
    
    paramlen = tools.unpackInt(paramlen);
    
    // not enough info
    if(buf.length<12 + paramlen){
        this.remainder = buf;
        return;
    }
    
    if(type && type!="NOOP"){
        piped = this.queue_pipe.shift();
    }
    
    params = new Buffer(paramlen);
    buf.copy(params, 0, 12, 12+paramlen);
    
    if(buf.length > 12+paramlen){
        this.remainder = new Buffer(buf.length - (12+paramlen));
        buf.copy(this.remainder, 0, 12+paramlen);
        process.nextTick(this.receive.bind(this));
    }else{
        this.remainder = false;
    }
    
    if(this.debug){
        console.log("<-- incoming");
        console.log(type, params);
    }
    
    this.handleCommand(type, params, piped);
};

GearmanConnection.prototype.handleCommand = function(type, paramsBuffer, command){
    
    var params = [], hint, positions = [], curpos=0, curparam, i, len;
    
    // check if there are expected params and if so, break 
    // the buffer into individual pieces
    if((hint = GearmanConnection.param_count[type]) && hint.length){
        
        // find \0 positions for individual params
        for(i=0, len = paramsBuffer.length; i<len; i++){
            if(paramsBuffer[i]===0){
                positions.push(i);
                if(positions.length >= hint.length-1){
                    break;
                }
            }
        }

        for(i=0, len = positions.length + 1; i<len; i++){
            curparam = new Buffer((positions[i] || paramsBuffer.length) - curpos);
            // there is no positions[i] for the last i, undefined is used instead
            paramsBuffer.copy(curparam, 0, curpos, positions[i]);
            curpos = positions[i]+1;
            if(hint[i]=="string"){
                params.push(curparam.toString("utf-8"));
            }else if(hint[i]=="number"){
                params.push(tools.unpackInt(curparam));
            }else{
                params.push(curparam);
            }
        }
    }
    
    // run the command handler (if there is one)
    if(this["handler_"+type]){
        this["handler_"+type].apply(this,[command].concat(params));
    }
};

// INCOMING COMMANDS

// UNIVERSAL

GearmanConnection.prototype.handler_ERROR = function(command, code, message){
    if(command && command.callback){
        command.callback(new Error(message));
        return;
    }
    this.emit("error", new Error(message));
};

// WORKER

GearmanConnection.prototype.handler_NO_JOB = function(command){
    this.sendCommand("PRE_SLEEP");
};

GearmanConnection.prototype.handler_NOOP = function(command){
    // probably some jobs available
    this.sendCommand("GRAB_JOB");
};

GearmanConnection.prototype.handler_JOB_ASSIGN = function(command, handle, func_name, payload){
    this.emit("job", handle, func_name, payload);
};

GearmanConnection.prototype.handler_JOB_ASSIGN_UNIQ = function(command, handle, func_name, uid, payload){
    this.emit("job", handle, func_name, payload, uid);
};

// CLIENT

GearmanConnection.prototype.handler_OPTION_RES = function(command){
    if(command && command.callback){
        command.callback(null, true);
    }
};

GearmanConnection.prototype.handler_JOB_CREATED = function(command, handle){
    var original = command || {};
    this.queued_jobs[handle] = command;
    this.emit("created", handle, original.options);
};

GearmanConnection.prototype.handler_WORK_COMPLETE = function(command, handle, response){
    var original = this.queued_jobs[handle] || {},
        encoding = original.options && original.options.encoding || "buffer";
    delete this.queued_jobs[handle];
    
    switch(encoding.toLowerCase()){
        case "utf-8":
        case "ascii":
        case "base64":
            response = response && response.toString(encoding) || "";
            break;
        case "number":
            response = Number(response && response.toString("ascii") || "") || 0;
            break;
        //case "buffer":
        default:
            // keep buffer
    }

    this.emit("complete", handle, response, original.options);
};

GearmanConnection.prototype.handler_WORK_EXCEPTION = function(command, handle, error){
    console.log(arguments)
    var original = this.queued_jobs[handle] || {};
    this.emit("exception", handle, error, original.options);
};

GearmanConnection.prototype.handler_WORK_WARNING = function(command, handle, error){
    var original = this.queued_jobs[handle] || {};
    this.emit("warning", handle, error, original.options);
};

GearmanConnection.prototype.handler_WORK_DATA = function(command, handle, payload){
    var original = this.queued_jobs[handle] || {},
        encoding = original.options && original.options.encoding || "buffer";
    
    switch(encoding.toLowerCase()){
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
    
    this.emit("data", handle, payload, original.options);
};

GearmanConnection.prototype.handler_WORK_FAIL = function(command, handle){
    var original = this.queued_jobs[handle] || {};
    delete this.queued_jobs[handle];
    this.emit("fail", handle, original.options);
};

GearmanConnection.prototype.handler_WORK_STATUS = function(command, handle, numerator, denominator){
    var original = this.queued_jobs[handle] || {};
    this.emit("status", handle, numerator, denominator, original.options);
};
