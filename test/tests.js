var Gearnode = require("../lib/gearnode"),
    GearmanConnection = require("../lib/gearman-connection"),
    testCase = require('nodeunit').testCase;


exports["gearnode instance"] = function(test){
    var gearman = new Gearnode();
    test.expect(1);
    test.ok(gearman instanceof Gearnode, "Worker is a Gearnode instance");
    test.done();
};

// ADD SERVER

exports["test server"] = {
    
    "add one server": function(test){
        var gearman = new Gearnode();
        gearman.addServer();
        
        test.expect(2);
        test.equal(gearman.server_names.length, 1, "One item in server_names array");
        test.equal(Object.keys(gearman.servers).length, 1, "One item in gearman.servers object");
        test.done();
    },
    
    "add one server multiple times": function(test){
        var gearman = new Gearnode();
        gearman.addServer("localhost");
        gearman.addServer("localhost");
        
        test.expect(2);
        test.equal(gearman.server_names.length, 1, "One item in server_names array");
        test.equal(Object.keys(gearman.servers).length, 1, "One item in gearman.servers object");
        test.done();
    },
    
    "add multiple servers": function(test){
        var gearman = new Gearnode();
        gearman.addServer("localhost.local");
        gearman.addServer("localhost.lan");
        
        test.expect(2);
        test.equal(gearman.server_names.length, 2, "Two items in server_names array");
        test.equal(Object.keys(gearman.servers).length, 2, "Two items in gearman.servers object");
        test.done();
    },
    
    "server instance": function(test){
        var gearman = new Gearnode();
        gearman.addServer();
        
        test.expect(1);
        test.ok(gearman.servers[gearman.server_names[0]].connection instanceof GearmanConnection, "Connection instance");
        test.done();
    }
};

// ADD FUNCTIONS

exports["test functions"] = {
    
    "add one function": function(test){
        var gearman = new Gearnode();
        
        gearman.addFunction("foo", function(){});
        
        test.expect(2);
        test.equal(gearman.function_names.length, 1, "One item in function_names array");
        test.equal(Object.keys(gearman.functions).length, 1, "One item in gearman.functions object");
        test.done();
    },
    
    "add one function multiple times": function(test){
        var gearman = new Gearnode();
        
        gearman.addFunction("foo", function(){});
        gearman.addFunction("foo", function(){});
        
        test.expect(2);
        test.equal(gearman.function_names.length, 1, "One item in function_names array");
        test.equal(Object.keys(gearman.functions).length, 1, "One item in gearman.functions object");
        test.done();
    },
    
    "add multiple functions": function(test){
        var gearman = new Gearnode();
        
        gearman.addFunction("foo", function(){});
        gearman.addFunction("bar", function(){});
        
        test.expect(2);
        test.equal(gearman.function_names.length, 2, "Two items in function_names array");
        test.equal(Object.keys(gearman.functions).length, 2, "Two items in gearman.functions object");
        test.done();
    },
    
    "function properties": function(test){
        var gearman = new Gearnode();
        
        gearman.addFunction("foo", function(){});
        gearman.addFunction("bar", "string", function(){});
        
        test.expect(6);
        test.equal(gearman.function_names[0], "foo", "Function name for foo");
        test.equal(gearman.function_names[1], "bar", "Function name for bar");
        test.equal(typeof gearman.functions[gearman.function_names[0]].func, "function", "Function instance for foo");
        test.equal(typeof gearman.functions[gearman.function_names[1]].func, "function", "Function instance for bar");
        test.equal(gearman.functions[gearman.function_names[0]].encoding, "buffer", "Function encoding for foo");
        test.equal(gearman.functions[gearman.function_names[1]].encoding, "string", "Function encoding for bar");
        test.done();
    }
};

// FUNCTIONS AND SERVES

exports["functions + servers"] = {
    
    "add function to existing server": function(test){
        var gearman = new Gearnode();
        
        gearman.addServer("foo");
        gearman.addFunction("bar", function(){});
        
        test.expect(1);
        test.equal(gearman.servers.foo.functions.length, 1, "One item in server functions array");
        test.done();
    },
    
    "add function before server": function(test){
        var gearman = new Gearnode();
    
        gearman.addFunction("bar", function(){});
        gearman.addServer("foo");
        
        test.expect(1);
        test.equal(gearman.servers.foo.functions.length, 1, "One item in server functions array");
        test.done();
    }
};

// WORKER ID

exports["worker id"] = {
    
    "set worker id": function(test){
        var gearman = new Gearnode();
        
        gearman.setWorkerId("bar");
        
        test.expect(1);
        test.equal(gearman.workerId, "bar", "Worker ID");
        test.done();
    },
    
    "set worker id to servers": function(test){
        var gearman = new Gearnode();
        
        gearman.addServer("foo");
        gearman.addServer("bar");
        
        gearman.setWorkerId("baz");
        
        test.expect(2);
        test.equal(gearman.servers.foo.connection.workerId, "baz", "Worker ID");
        test.equal(gearman.servers.bar.connection.workerId, "baz", "Worker ID");
        test.done();
    },
    
    "set worker id before server": function(test){
        var gearman = new Gearnode();
        
        gearman.addServer("foo");
        gearman.setWorkerId("baz");
        gearman.addServer("bar");
        
        test.expect(2);
        test.equal(gearman.servers.foo.connection.workerId, "baz", "Worker ID");
        test.equal(gearman.servers.bar.connection.workerId, "baz", "Worker ID");
        test.done();
    }
};

module.exports["worker behavior"] = testCase({
    setUp: function (callback) {
        this.worker = new Gearnode();
        this.worker.addServer("localhost",7003);
        
        this.client = new Gearnode();
        this.client.addServer("localhost",7003);
        
        this.worker.addFunction("testjob_upper", function(payload, job){
            job.complete(payload.toString("utf-8").toUpperCase());
        });
        
        this.worker.addFunction("testjob_upper_utf8","utf-8", function(payload, job){
            job.complete(payload.toUpperCase());
        });
        
        this.worker.addFunction("testjob_upper_base64","base64", function(payload, job){
            job.complete(new Buffer(payload, "base64").toString("utf-8").toUpperCase());
        });
        
        this.worker.addFunction("testjob_getexception",function(payload, job){
            job.error(new Error("Error happened"));
        });
        
        this.worker.addFunction("testjob_partial",function(payload, job){
            for(var i=0; i<4; i++){
                job.data("data" + i);
            }
            job.complete("ready");
        });
        
        this.worker.addFunction("testjob_getwarning",function(payload, job){
            job.warning("foo");
            job.complete("bar");
        });
        
        this.worker.addFunction("testjob_getfail",function(payload, job){
            job.fail();
        });
        
        this.worker.addFunction("testjob_disconnect",function(payload, job){
            setTimeout(function(){
                job.complete("bar");
            },1000);
        });
        
        callback();
    },
    
    tearDown: function (callback) {
        // clean up
        callback();
    },
    
    "upper": function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("testjob_upper","test");
        
        job.on("complete", function(data){
            test.equal(data.toString("utf-8"), "TEST", "Function success");
            test.done();
        });
        
        job.on("fail", function(){
            test.ok(false, "Function failed");
            test.done();
        });
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
    },
    
    "upper utf8": function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("testjob_upper_utf8","test");
        
        job.on("complete", function(data){
            test.equal(data.toString("utf-8"), "TEST", "Function success");
            test.done();
        });
        
        job.on("fail", function(){
            test.ok(false, "Function failed");
            test.done();
        });
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
    },
    
    "upper base64": function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("testjob_upper_base64","test");
        
        job.on("complete", function(data){
            test.equal(data.toString("utf-8"), "TEST", "Function success");
            test.done();
        });
        
        job.on("fail", function(){
            test.ok(false, "Function failed");
            test.done();
        });
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
    },
    
    "upper expect utf8": function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("testjob_upper","test", {encoding:"utf-8"});
        
        job.on("complete", function(data){
            test.equal(data, "TEST", "Function success");
            test.done();
        });
        
        job.on("fail", function(){
            test.ok(false, "Function failed");
            test.done();
        });
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
    },
    
    "upper expect base64": function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("testjob_upper","test", {encoding:"base64"});
        
        job.on("complete", function(data){
            test.equal(data, new Buffer("TEST","utf-8").toString("base64"), "Function success");
            test.done();
        });
        
        job.on("fail", function(){
            test.ok(false, "Function failed");
            test.done();
        });
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
    },
    
    "get exceptions": function(test){
        test.expect(1);
        this.client.getExceptions((function(err, success){
            test.ok(success,"Listening for exceptions");
            test.done();
        }).bind(this));
    },
    
    "fail": function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("testjob_getfail","test", {encoding:"utf-8"});
        
        job.on("complete", function(data){
            test.ok(false, "Should not complete");
            test.done();
        });
        
        job.on("fail", function(){
            test.ok(true, "Function failed");
            test.done();
        });
        
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
    },
    
    "partial data": function(test){
        test.expect(5);
        
        var job = this.client.submitJob("testjob_partial", "test", {encoding:"utf-8"}),
            i = 0;
        
        job.on("complete", function(data){
            test.equal(data, "ready", "Function success");
            test.done();
        });
        
        job.on("fail", function(){
            test.ok(false, "Function failed");
            test.done();
        });
        
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
        
        job.on("data", function(data){
            test.equal("data" + (i++), data, "Function part OK");
        });
    },
    
    "warning": function (test) {
        
        test.expect(2);
        
        var job = this.client.submitJob("testjob_getwarning","test", {encoding:"utf-8"});
        
        job.on("complete", function(data){
            test.equal(data, "bar", "Completed");
            test.done();
        });
        
        job.on("warning", function(data){
            test.equal(data, "foo", "Function warning");
        });
        
        job.on("fail", function(){
            test.ok(false, "Function failed");
            test.done();
        });
        
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
    },
    
    "disconnect server": function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("testjob_disconnect","test");
        
        job.on("complete", function(data){
            test.ok(false, "Should not complete")
            test.done();
        });
                
        job.on("fail", function(){
            test.ok(true, "Function failed");
            test.done();
        });
        
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
        
        setTimeout((function(){
            this.client.servers[this.client.server_names[this.client.server_names.length-1]].connection.close();
        }).bind(this), 100);
    },
    
    "disconnect event": function (test) {
        
        test.expect(2);
        
        var job = this.client.submitJob("testjob_disconnect","test");
        
        this.client.on("disconnect", function(server_name){
            test.equal(server_name, "localhost", "Server disconnected");
            test.done();
        });
        
        job.on("complete", function(data){
            test.ok(false, "Should not complete")
            test.done();
        });
                
        job.on("fail", function(){
            test.ok(true, "Function failed");
            //test.done();
        });
        
        job.on("error", function(){
            test.ok(false, "Function failed with error");
            test.done();
        });
        
        setTimeout((function(){
            this.client.servers[this.client.server_names[this.client.server_names.length-1]].connection.close();
        }).bind(this), 100);
    }
});
