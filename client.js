var gearman = require("gearman"),
    client = gearman.createClient(7003, "localhost");

console.log("Sending job...");
var job = client.submitJob("reverse", "test", { encoding: "utf8" });
job.on("complete", function (data) {
    console.log(data);
    client.end();
});

var handle;

job.on("create", function(h){
    console.log(h)
    handle = h;
});