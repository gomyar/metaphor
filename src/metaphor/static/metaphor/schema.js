
var schema = {}


$(document).ready(function() {
    console.log("initing");
    net.perform_get("/schema/specs", function(data) {
        schema = data;
        turtlegui.reload()
    });
});

