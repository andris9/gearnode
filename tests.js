var Gearnode = require("./gearnode"),
    GearmanConnection = require("./gearman-connection"),
    testCase = require('nodeunit').testCase;


exports.gearnode_instance = function(test){
    var gearman = new Gearnode();
    test.expect(1);
    test.ok(gearman instanceof Gearnode, "Worker is a Gearnode instance");
    test.done();
}

// ADD SERVER

exports.server = {
    
    add_one_server: function(test){
        var gearman = new Gearnode();
        gearman.addServer();
        
        test.expect(2);
        test.equal(gearman.server_names.length, 1, "One item in server_names array");
        test.equal(Object.keys(gearman.servers).length, 1, "One item in gearman.servers object");
        test.done();
    },
    
    add_one_server_multiple_times: function(test){
        var gearman = new Gearnode();
        gearman.addServer("localhost");
        gearman.addServer("localhost");
        
        test.expect(2);
        test.equal(gearman.server_names.length, 1, "One item in server_names array");
        test.equal(Object.keys(gearman.servers).length, 1, "One item in gearman.servers object");
        test.done();
    },
    
    add_multiple_servers: function(test){
        var gearman = new Gearnode();
        gearman.addServer("localhost.local");
        gearman.addServer("localhost.lan");
        
        test.expect(2);
        test.equal(gearman.server_names.length, 2, "Two items in server_names array");
        test.equal(Object.keys(gearman.servers).length, 2, "Two items in gearman.servers object");
        test.done();
    },
    
    server_instance: function(test){
        var gearman = new Gearnode();
        gearman.addServer();
        
        test.expect(1);
        test.ok(gearman.servers[gearman.server_names[0]].connection instanceof GearmanConnection, "Connection instance")
        test.done();
    }
}

// ADD FUNCTIONS

exports.functions = {
    
    add_one_function: function(test){
        var gearman = new Gearnode();
        
        gearman.addFunction("foo", function(){});
        
        test.expect(2);
        test.equal(gearman.function_names.length, 1, "One item in function_names array");
        test.equal(Object.keys(gearman.functions).length, 1, "One item in gearman.functions object");
        test.done();
    },
    
    add_one_function_multiple_times: function(test){
        var gearman = new Gearnode();
        
        gearman.addFunction("foo", function(){});
        gearman.addFunction("foo", function(){});
        
        test.expect(2);
        test.equal(gearman.function_names.length, 1, "One item in function_names array");
        test.equal(Object.keys(gearman.functions).length, 1, "One item in gearman.functions object");
        test.done();
    },
    
    add_multiple_functions: function(test){
        var gearman = new Gearnode();
        
        gearman.addFunction("foo", function(){});
        gearman.addFunction("bar", function(){});
        
        test.expect(2);
        test.equal(gearman.function_names.length, 2, "Two items in function_names array");
        test.equal(Object.keys(gearman.functions).length, 2, "Two items in gearman.functions object");
        test.done();
    },
    
    function_properties: function(test){
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
}

// FUNCTIONS AND SERVES

exports.functions_servers = {
    
    add_function_to_existing_server: function(test){
        var gearman = new Gearnode();
        
        gearman.addServer("foo");
        gearman.addFunction("bar", function(){});
        
        test.expect(1);
        test.equal(gearman.servers["foo"].functions.length, 1, "One item in server functions array");
        test.done();
    },
    
    add_function_before_server: function(test){
        var gearman = new Gearnode();
    
        gearman.addFunction("bar", function(){});
        gearman.addServer("foo");
        
        test.expect(1);
        test.equal(gearman.servers["foo"].functions.length, 1, "One item in server functions array");
        test.done();
    }
}

// WORKER ID

exports.worker_id = {
    
    set_worker_id: function(test){
        var gearman = new Gearnode();
        
        gearman.setWorkerId("bar");
        
        test.expect(1);
        test.equal(gearman.workerId, "bar", "Worker ID");
        test.done();
    },
    
    set_worker_id_to_servers: function(test){
        var gearman = new Gearnode();
        
        gearman.addServer("foo");
        gearman.addServer("bar");
        
        gearman.setWorkerId("baz");
        
        test.expect(2);
        test.equal(gearman.servers["foo"].connection.workerId, "baz", "Worker ID");
        test.equal(gearman.servers["bar"].connection.workerId, "baz", "Worker ID");
        test.done();
    },
    
    set_worker_id_before_server: function(test){
        var gearman = new Gearnode();
        
        gearman.addServer("foo");
        gearman.setWorkerId("baz");
        gearman.addServer("bar");
        
        test.expect(2);
        test.equal(gearman.servers["foo"].connection.workerId, "baz", "Worker ID");
        test.equal(gearman.servers["bar"].connection.workerId, "baz", "Worker ID");
        test.done();
    }
}

module.exports.worker = testCase({
    setUp: function (callback) {
        this.worker = new Gearnode();
        this.worker.addServer("localhost",7003);
        
        this.client = new Gearnode();
        this.client.addServer("localhost",7003);
        
        this.worker.addFunction("upper", function(payload, job){
            job.complete(payload.toString("utf-8").toUpperCase());
        });
        
        this.worker.addFunction("upper_utf8","utf-8", function(payload, job){
            job.complete(payload.toUpperCase());
        });
        
        this.worker.addFunction("upper_base64","base64", function(payload, job){
            job.complete(new Buffer(payload, "base64").toString("utf-8").toUpperCase());
        });
        
        this.worker.addFunction("getexception",function(payload, job){
            job.error(new Error("Error happened"));
        });
        
        this.worker.addFunction("partial",function(payload, job){
            var i=0;
            job.data(i++);
            job.data(i++);
            job.data(i++);
            job.complete("ready");
        });
        
        this.worker.addFunction("getwarning",function(payload, job){
            job.warning("foo");
            job.complete("bar");
        });
        
        this.worker.addFunction("getfail",function(payload, job){
            job.fail();
        });
        
        callback();
    },
    
    tearDown: function (callback) {
        // clean up
        callback();
    },
    
    test_upper: function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("upper","test");
        
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
    
    test_upper_utf8: function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("upper_utf8","test");
        
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
    
    test_upper_base64: function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("upper_base64","test");
        
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
    
    test_upper_expect_utf8: function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("upper","test", {encoding:"utf-8"});
        
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
    
    test_upper_expect_base64: function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("upper","test", {encoding:"base64"});
        
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
    
    getExceptions: function(test){
        test.expect(2);
        this.client.getExceptions((function(err, success){
            test.ok(success,"Listening for exceptions");
            
            var job = this.client.submitJob("getexception","test");
            
            job.on("complete", function(data){
                test.ok("false","No exceptions");
                test.done();
            });
            
            job.on("fail", function(){
                test.ok("false","No exceptions");
                test.done();
            });
            
            job.on("error", function(){
                test.ok(true, "Function failed with error");
                test.done();
            });

        }).bind(this));
    },
    
    test_partial_data: function(test){
        test.expect(4);
        
        var job = this.client.submitJob("partial", "test", {encoding:"number"}),
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
            test.equal(i++, data, "Function part OK");
            test.done();
        });
    },
    
    test_warning: function (test) {
        
        test.expect(2);
        
        var job = this.client.submitJob("getwarning","test", {encoding:"utf-8"});
        
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
    
    test_fail: function (test) {
        
        test.expect(1);
        
        var job = this.client.submitJob("getfail","test", {encoding:"utf-8"});
        
        job.on("complete", function(data){
            test.ok(false, "SHould not complete");
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
    }
});



