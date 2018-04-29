
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
gui.field_types = ['str', 'int', 'float', 'collection', 'link', 'calc'];
gui.calc_types = ['str', 'int', 'float'];

gui.delete_field = function(spec_name, field_name) {
    if (confirm('Are you sure you want to delete ' + field_name + '?')) {
        net.perform_delete("/schema/specs/" + spec_name + "/" + field_name, gui.load_specs)
    }
}

gui.load_specs = function(onload) {
    net.perform_get("/schema/specs", function(data) {
        schema = data;
        turtlegui.reload();
        if (onload) {
            onload();
        }
    }, function(errorText, jqXHR) {
        alert("Error: " + jqXHR.responseText);
    });
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
                var field_div = $('#field_' + gui.workingfield.spec_name + '_' + gui.workingfield.name)
                field_div.hide();
                field_div.fadeIn();
            });
        }, function(errorText, jqXHR) {
        alert("Error: " + jqXHR.responseText);
    });
}

gui.create_spec = function() {
    var spec_data = {'name': gui.working_spec.name};
    net.perform_post("/schema/specs",
        spec_data, function(e) {
            gui.load_specs();
        });
}



$(document).ready(function() {
    console.log("initing");
    gui.load_specs();
});

