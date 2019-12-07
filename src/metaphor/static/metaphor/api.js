
var api = {};

api.path = '';

api.postform_shown = false;
api.current_resource = {};
api.resource = null;
api.spec = null;
api.specs = null;
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
    turtlegui.ajax.get('/schema/specs', function(content) {
        api.specs = JSON.parse(content).specs;
        turtlegui.ajax.get('/api/' + api.resource_path, function(content) {
            api.resource = JSON.parse(content);
            turtlegui.reload();
        }, api.report_error);
    }, api.report_error)
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

gui.is_null = function(value) {
    return value == null;
}

gui.is_empty = function(value) {
    return value.length == 0;
}

gui.is_field_basic = function(field) {
    return ['str', 'int', 'float'].includes(field.type);
}

gui.is_field_basic_calc = function(field) {
    return ['calc'].includes(field.type) && ['str', 'int', 'float'].includes(field.calc_type);
}

gui.is_field_resource_calc = function(field) {
    return ['calc'].includes(field.type) && !['str', 'int', 'float'].includes(field.calc_type);
}

gui.is_field_collection = function(field) {
    return ['collection'].includes(field.type);
}

gui.is_calc_collection = function(calc_spec) {
    return calc_spec.is_collection;
}

gui.is_calc_resource = function(calc_spec) {
    return !calc_spec.is_collection;
}

gui.is_field_link = function(field) {
    return ['link'].includes(field.type);
}

gui.create_collection_child = function(parent_resource, collection_field) {
    console.log("Create child resource and add to collection");
}

gui.show_search_popup = function(field) {
    console.log("Show search popup for ", field);
}

document.addEventListener("DOMContentLoaded", function(){
    api.reload_resource();
});


