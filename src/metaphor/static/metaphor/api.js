
var api = {};

api.path = '';

api.spec = null;
api.resource = null;

api.report_error = function(errorText, jqXHR) {
    try {
        alert("Error: " + jqXHR.responseJSON['error']);
    } catch (e) {
        alert("Error: " + jqXHR.responseText);
    }
}

api.load_resource = function(resource_path) {
    if (resource_path) {
        api.path = resource_path;
    }
    net.perform_get('/schema/specfor' + api.path, function(data) {
        api.spec = data;
        net.perform_get('/api' + api.path, function(data) {
            api.resource = data;
            turtlegui.reload();
        }, api.report_error);
    }, api.report_error);
}

$(document).ready(function() {
    console.log("initing");

//    api.load_resource()
    turtlegui.reload();
});

