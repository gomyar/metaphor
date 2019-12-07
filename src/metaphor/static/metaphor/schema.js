
var schema = {}

var gui = {};

gui.popup_field = false;
gui.workingfield = {
    'spec_name': null,
    'name': null,
    'type': 'str',
    'target': null,
    'calc': null,
    'calc_type': null,
};
gui.working_spec = {
    'name': null
};
gui.field_types = ['str', 'int', 'float', 'collection', 'link', 'calc', 'linkcollection'];
gui.calc_types = ['str', 'int', 'float'];

gui.calc_types = function() {
    var calc_types = ['str', 'int', 'float'];
    for (var spec_type in schema.specs) {
        if (spec_type != 'root') {
            calc_types[calc_types.length] = spec_type;
        }
    }
    return calc_types;
}


gui.report_error = function(errorText, jqXHR) {
    try {
        alert("Error: " + jqXHR.responseJSON['error']);
    } catch (e) {
        alert("Error: " + jqXHR.responseText);
    }
}


gui.delete_field = function(spec_name, field_name) {
    if (confirm('Are you sure you want to delete ' + field_name + '?')) {
        net.perform_delete("/schema/specs/" + spec_name + "/" + field_name, gui.load_specs,
            gui.report_error)
    }
}

gui.load_specs = function(onload) {
    net.perform_get("/schema/specs", function(data) {
        schema = data;
        turtlegui.reload();
        if (onload) {
            onload();
        }
    }, gui.report_error);
}

gui.show_new_schema_popup = function() {
    gui.popup_spec = true;
    turtlegui.reload();
}

gui.create_field_id = function(field) {
    return 'field_' + gui.workingfield.spec_name + '_' + field.name;
}

gui.not_reverse_link = function(field) {
    return field.type != 'reverse_link';
}

gui.is_field_collection = function(field) {
    return ['link', 'collection', 'linkcollection'].includes(field.type);
}

gui.is_field_calc = function(field) {
    return ['calc'].includes(field.type);
}

gui.is_field_calc_primitive = function(field) {
    return ['calc'].includes(field.type) && ![null, 'int', 'str', 'float'].includes(field.calc_type);
}

gui.close_spec_popup = function() {
    gui.popup_spec = false;
    turtlegui.reload();
}

gui.close_field_popup = function() {
    gui.popup_field = false;
    turtlegui.reload();
}

gui.show_new_field_popup = function(spec_name) {
    gui.workingfield.spec_name = spec_name;
    gui.popup_field = true;
    turtlegui.reload()
}


gui.create_field = function() {
    var field_data = {};
    field_data[gui.workingfield.name] = gui.workingfield; 
    net.perform_patch("/schema/specs/" + gui.workingfield.spec_name,
        field_data, function(e) {
            gui.load_specs(function (e) {
                var field_div = document.getElementById('#field_' + gui.workingfield.spec_name + '_' + gui.workingfield.name)
                field_div.hide();
                field_div.fadeIn();
            });
        }, gui.report_error);
}

gui.create_spec = function() {
    var spec_data = {'name': gui.working_spec.name};
    net.perform_post("/schema/specs",
        spec_data, function(e) {
            gui.load_specs();
        });
}


document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    gui.load_specs();
});



