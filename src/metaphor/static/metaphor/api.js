
var api = {};

api.path = '';

api.postform_shown = false;
api.current_resource = {};
api.resource = null;
api.spec = null;
api.resource_path = null;  // set in template
var net = {loading: false};

api.show_postform = function(shown) {
    api.postform_shown = shown;
    turtlegui.reload();
}

api.report_error = function(errorText, jqXHR) {
    try {
        alert("Error: " + jqXHR.responseJSON['error']);
    } catch (e) {
        alert("Error: " + jqXHR.responseText);
    }
}

api.reload_resource = function() {
    turtlegui.ajax.get('/api/' + api.resource_path, function(content) {
        api.resource = JSON.parse(content);
        turtlegui.reload();
    }, api.report_error);
}

api.post_resource = function() {
    turtlegui.ajax.post('/api/' + api.resource_path, api.current_resource, function(e) {
        api.reload_resource();
    }, api.report_error);
}

api.delete_resource = function(resource) {
    if (confirm("Are you sure you wish to delete this resource?")) {
        turtlegui.ajax.delete(resource.self, api.reload_resource, api.report_error);
    }
}

// ---



var gui = {};

gui.is_field_basic = function(field) {
    return ['str', 'int', 'float'].includes(field.type);
}

gui.is_field_calc = function(field) {
    return ['calc'].includes(field.type);
}

gui.is_field_collection = function(field) {
    return ['link', 'collection'].includes(field.type);
}

document.addEventListener("DOMContentLoaded", function(){
    api.reload_resource();
});


