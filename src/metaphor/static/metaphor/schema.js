
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
gui.field_types = ['str', 'int', 'float', 'collection', 'link', 'calc'];
gui.calc_types = ['str', 'int', 'float'];

gui.add_field = function(field_name, field_type) {
}

gui.add_calc = function(field_name, calc, calc_type) {
}

gui.add_link = function(field_name, target) {
}

gui.add_collection = function(field_name, target) {
}

gui.delete_field = function(spec_name, field_name) {
    if (confirm('Are you sure you want to delete ' + field_name + '?')) {
        net.perform_delete("/schema/specs/" + spec_name + "/" + field_name, gui.load_specs)
    }
}

gui.load_specs = function(onload) {
    net.perform_get("/schema/specs", function(data) {
        schema = data;
        turtlegui.reload();
        if (onload)
            onload();
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
        });
}


$(document).ready(function() {
    console.log("initing");
    gui.load_specs();
});

