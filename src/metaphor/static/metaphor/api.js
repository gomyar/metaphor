
var api = {};

api.path = '';

api.show_postform = false;
api.current_resource = {};

api.report_error = function(errorText, jqXHR) {
    try {
        alert("Error: " + jqXHR.responseJSON['error']);
    } catch (e) {
        alert("Error: " + jqXHR.responseText);
    }
}

api.reload_resource = function() {
    net.perform_get(window.location.pathname, function(data) {
        api.resource = data;
        turtlegui.reload();
    }, api.report_error);
}

api.post_resource = function() {
    net.perform_post(window.location.pathname, api.current_resource, function(e) {
        api.reload_resource();
    }, api.report_error);
}

$(document).ready(function() {
    console.log("initing");

//    api.load_resource()
    turtlegui.reload();
});

