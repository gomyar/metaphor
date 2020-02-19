

var schema = {
    specs: {},
    root: {},

    load_specs: function() {
        loading.inc_loading();
        turtlegui.ajax.get(
            '/admin/schema_editor/api',
            function(data) {
                result = JSON.parse(data);
                schema.specs = result.specs;
                schema.root = result.root;
                loading.dec_loading();
            },
            function(data) {
                loading.dec_loading();
                alert("Error loading spec: " + data.error);
            }
        );
    }
};

var loading = {
    is_loading: 0,
    inc_loading: function() {
        loading.is_loading += 1;
        turtlegui.reload();
    },
    dec_loading: function() {
        loading.is_loading -= 1;
        turtlegui.reload();
    }
};

var create_spec = {
    is_shown: false,
    spec_name: null,

    show_popup: function() {
        create_field.field_name = null;
        create_spec.is_shown = true;
        turtlegui.reload();
    },
    hide_popup: function() {
        create_spec.is_shown = false;
        turtlegui.reload();
    },

    create_spec: function() {
        loading.inc_loading();
        create_spec.hide_popup();
        turtlegui.ajax.post(
            '/admin/schema_editor/api/specs',
            {'spec_name': create_spec.name},
            function(data) {
                schema.load_specs();
            },
            function(data) {
                loading.dec_loading();
                alert("Error creating spec: " + data.error);
            }
        );
    }
};


var create_field = {
    
    show_popup: false,
    spec_name: null,

    field_name: null,
    field_type: null,
    field_target: null,
    calc_str: null,

    all_field_types: ['int', 'float', 'str', 'bool', 'collection', 'link', 'linkcollection', 'calc'],

    create_field: function() {
        if (!create_field.field_name) {
            alert("Name is required");
            return;
        }
        turtlegui.ajax.post(
            '/admin/schema_editor/api/specs/' + create_field.spec_name + '/fields',
            {'field_name': create_field.field_name,
             'field_type': create_field.field_type,
             'field_target': create_field.field_target,
             'calc_str': create_field.calc_str
            },
            function(data) {
                create_field.hide_popup();
                schema.load_specs();
            },
            function(data) {
                loading.dec_loading();
                alert("Error creating spec: " + data.error);
            }
        );
    },

    show_create: function(spec_name) {
        create_field.spec_name = spec_name;
        create_field.show_popup = true;
        create_field.field_name = null;
        create_field.field_type = 'int';
        create_field.field_target = null;
        create_field.calc_str = null;
        turtlegui.reload();
    },

    hide_popup: function() {
        create_field.show_popup = false;
        turtlegui.reload();
    }
};


document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    schema.load_specs();
});
