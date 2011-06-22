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
    
    this.connected = false;
    this.processing = false;
    this.failed = false;
    
    this.retries = 0;
    this.remainder = false;
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
}

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
}

GearmanConnection.param_count = {
    ERROR: ["string","string"],
    JOB_ASSIGN: ["string","string", "buffer"],
    JOB_ASSIGN: ["string","string", "string", "buffer"],
    JOB_CREATED: ["string", "string"],
    WORK_COMPLETE: ["string", "buffer"],
    WORK_EXCEPTION: ["string", "string"],
    WORK_FAIL: ["string"]
}

GearmanConnection.prototype.sendCommand = function(command){
    if(!command){
        return false;
    }
    
    if(typeof command == "string"){
        command = {
            type: command
        }
    }
    
    if(!command.params){
        command.params = [];
    }
    this.command_queue.push(command);
    this.processQueue();
}

GearmanConnection.prototype.addFunction = function(func_name){
    this.sendCommand({
        type: "CAN_DO",
        params: [func_name]
    });
    
    this.sendCommand({
        type: "GRAB_JOB",
        pipe: true
    });
}

GearmanConnection.prototype.removeFunction = function(func_name){
    this.sendCommand({
        type: "CANT_DO",
        params: [func_name]
    });
}

GearmanConnection.prototype.removeAllFunction = function(){
    this.sendCommand("RESET_ABILITIES");
}

GearmanConnection.prototype.processQueue = function(){
    var command;
    
    // kui ühendust veel pole, telli selle avamine
    if(!this.connected){
        if(this.retries<5){
            this.connect();
        }else{
            console.log("failed")
            this.failed = true;
        }
        return false;
    }
    
    // lae järjekorrast FIFO käsklus
    if(this.command_queue.length){
        this.processing = true;
        command = this.command_queue.shift();
        process.nextTick(this.runCommand.bind(this, command));
    }else{
        this.processing = false;
    }
}

GearmanConnection.prototype.runCommand = function(command){
    var req  = [00, 82, 69, 81], //\0REQ
        type = tools.nrpad(GearmanConnection.packet_types[command.type], 4),
        param, params = [], paramlen = 0, size, buf, pos;
    
    // teeme parameetritest ükshaaval Buffer objektid ning loeme pikkused kokku
    for(var i=0; i<command.params.length; i++){
        if(command.params[i] instanceof Buffer){
            params.push(command.params[i]);
            paramlen += command.params[i].length;
        }else{
            param = new Buffer(String(command.params[i] || ""),"utf-8");
            params.push(param);
            paramlen += param.length;
        }
    }
    
    // lisame pikkusele parameetrite vahele minevate \0 arvu
    paramlen += command.params.length-1;
    paramlen = paramlen>0?paramlen:0;
    
    // pikkus koosneb \0REQ 4B + tyyp 4B + parameetrite pikkus 4B + parameetrid
    size = 4 + 4 + 4 + paramlen;
    
    // konverteerime 4B Buffer objektiks
    paramlen = tools.nrpad(paramlen, 4);
    
    // loome objekti mis saata serverile
    buf = new Buffer(size);
    
    // kopeerime Magick baidid
    // TODO: see peaks olema Buffer mitte array
    for(var i=0; i<req.length; i++){
        buf[i] = req[i];
    }
    
    // kopeerime käsu koodi
    type.copy(buf, 4, 0);
    
    // kopeerime parameetrite pikkuse 4B
    paramlen.copy(buf, 8, 0);
    
    // parameetrite jaoks on stardipositsioon 12s bait
    pos = 12;
    
    // kopeerime ükshaaval parameetrid
    for(var i=0; i<params.length; i++){
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
}

GearmanConnection.prototype.connect = function(){
    
    if(this.connected || this.connecting){
        // juhul kui ühendus on juba olemas käivita protsessimine
        if(this.connected && !this.processing){
            this.processQueue();
        }
        return false;
    }
    
    this.connecting = true;

    console.log("connecting...");
    this.socket = netlib.createConnection(this.port, this.server);
        
    this.socket.on("connect", (function(){
        this.connecting = false;
        this.connected = true;
        this.retries = 0;
    
        console.log("connected!");        
        this.processQueue();
            
    }).bind(this));
    

    this.socket.on("end", this.close.bind(this));
    this.socket.on("error", this.close.bind(this));
    this.socket.on("close", this.close.bind(this));
    this.socket.on("timeout", this.close.bind(this));
    
    this.socket.on("data", this.receive.bind(this));
}

GearmanConnection.prototype.closeConnection = function(){
    if(this.connected){
        this.socket.end();
    }
}

GearmanConnection.prototype.close = function(){
    if(this.connected){
        if(this.socket){
            try{
                this.socket.end();
            }catch(E){}
        }
        this.connected = false;
        this.connecting = false;
        this.retries++;
    }
}


GearmanConnection.prototype.receive = function(chunk){
    var buf = new Buffer((chunk && chunk.length || 0) + (this.remainder && this.remainder.length || 0)),
        pos = 0, res = [00, 82, 69, 83], type = new Buffer(4), paramlen = new Buffer(4),
        action, piped, params;
    
    if(this.remainder){
        this.remainder.copy(buf, 0, 0);
        pos = this.remainder.length;
    }
    chunk && chunk.copy(buf, pos, 0);
    
    // vastus peab olema vähemalt 12 baiti pikk
    if(buf.length<12){
        this.remainder = buf;
        return;
    }
    
    // kontrolli magickut
    for(var i=0; i<4; i++){
        if(res[i] != buf[i]){
            console.log("WARNING: out of sync!");
            break;
        }
    }
    
    // loe töö tüüp
    buf.copy(type, 0, 4, 8);
    type = tools.nrunpad(type);
    type = GearmanConnection.packet_types_reversed[String(type)];
    
    // loe parameetrite pikkus
    buf.copy(paramlen, 0, 8, 12);
    paramlen = tools.nrunpad(paramlen);
    
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
        process.nextTick(this.receive.bind(this))
    }else{
        this.remainder = false;
    }
    
    if(this.debug){
        console.log("<-- incoming");
        console.log(type, params);
    }
    
    this.handleCommand(type, params, piped);
}

GearmanConnection.prototype.handleCommand = function(type, params, command){
    var data = [], hint, positions = [], curpos=0, curparam;
    
    if(hint = GearmanConnection.param_count[type]){
        if(hint.length){
            
            for(var i=0, len = params.length; i<len; i++){
                if(params[i]===0){
                    positions.push(i);
                    if(positions.length >= hint.length-1){
                        break;
                    }
                }
            }
            
            for(var i=0, len = positions.length; i<len; i++){
                curparam = new Buffer(positions[i] - curpos);
                params.copy(curparam, 0, curpos, positions[i]);
                curpos = positions[i]+1;
                if(hint[i]=="string"){
                    data.push(curparam.toString("utf-8"));
                }else{
                    data.push(curparam);
                }
            }
            curparam = new Buffer(params.length - curpos);
            params.copy(curparam, 0, curpos);
            if(hint[hint.length-1] == "string"){
                data.push(curparam.toString("utf-8"));
            }else{
                data.push(curparam);
            }
        }
    }
    
    if(this["handler_"+type]){
        this["handler_"+type].apply(this,[command].concat(data));
    }
}

GearmanConnection.prototype.jobComplete = function(handle, payload){
    this.sendCommand({
        type: "WORK_COMPLETE",
        params: [handle, payload]
    });
    
    this.sendCommand("GRAB_JOB");
    
}

GearmanConnection.prototype.jobFail = function(handle){
    this.sendCommand({
        type: "WORK_FAIL",
        params: [handle]
    });
    
    this.sendCommand("GRAB_JOB");
}

GearmanConnection.prototype.jobError = function(handle, message){
    this.sendCommand({
        type: "WORK_EXCEPTION",
        params: [handle, message]
    });
    
    this.sendCommand("GRAB_JOB");
}

GearmanConnection.prototype.jobData = function(handle, data){
    this.sendCommand({
        type: "WORK_DATA",
        params: [handle, data]
    });
}

GearmanConnection.prototype.handler_ERROR = function(command, code, message){
    this.emit("error", new Error(message));
}

GearmanConnection.prototype.handler_NO_JOB = function(command){
    this.sendCommand("PRE_SLEEP");
}

GearmanConnection.prototype.handler_NOOP = function(command){
    this.sendCommand("GRAB_JOB");
}

GearmanConnection.prototype.handler_JOB_ASSIGN = function(command, handle, func_name, payload){
    this.emit("job", handle, func_name, payload);
}

GearmanConnection.prototype.handler_JOB_ASSIGN_UNIQ = function(command, handle, func_name, uid, payload){
    this.emit("job", handle, func_name, payload, uid);
}


GearmanConnection.prototype.handler_JOB_CREATED = function(command, handle){
    this.emit("created", handle);
}

GearmanConnection.prototype.handler_WORK_COMPLETE = function(command, handle, response){
    this.emit("complete", handle, response);
}

GearmanConnection.prototype.handler_WORK_EXCEPTION = function(command, handle, error){
    this.emit("exception", handle, error);
}

GearmanConnection.prototype.handler_WORK_FAIL = function(command, handle){
    this.emit("fail", handle);
}


GearmanConnection.prototype.submitJob = function(func_name, uid, data){
    this.sendCommand({
        type: "SUBMIT_JOB",
        params: [func_name, uid, data],
        pipe: true
    });
}
