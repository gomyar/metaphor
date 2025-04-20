

var schema = {
    specs: {},
    root: {},
    current: false,

    load_specs: function() {
        loading.inc_loading();
        turtlegui.ajax.get(
            '/admin/api/schemas/' + schema_id,
            function(data) {
                result = JSON.parse(data);
                schema.specs = result.specs;
                schema.root = result.root;
                schema.current = result.current;
                schema.name = result.name;
                schema.description = result.description;
                schema.version = result.version;
                schema.groups = result.groups;
                loading.dec_loading();
            },
            function(data) {
                loading.dec_loading();
                alert("Error loading spec: " + data.error);
            }
        );
    },

    significant_field: function(spec, field_name) {
        return spec.significant_field == field_name;
    },

    select_significant_field: function(spec, field_name) {
        spec.significant_field = field_name;
        turtlegui.reload();
    },

    delete_field: function(spec_name, field_name) {
        if (confirm("Delete " + spec_name + "." + field_name + "?")) {
            turtlegui.ajax.delete(
                '/admin/api/schemas/' + schema_id + '/specs/' + spec_name + '/fields/' + field_name,
                function(data) {
                    schema.load_specs();
                },
                function(data) {
                    loading.dec_loading();
                    alert("Error deleting field: " + JSON.parse(data.response).error);
                }
            );
        }
    },

    delete_spec: function(spec_name) {
        if (confirm("Delete " + spec_name + "?")) {
            turtlegui.ajax.delete(
                '/admin/api/schemas/' + schema_id + '/specs/' + spec_name,
                function(data) {
                    schema.load_specs();
                },
                function(data) {
                    loading.dec_loading();
                    alert("Error deleting spec: " + JSON.parse(data.response).error);
                }
            );
        }
    },
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

var create_group = {
    is_shown: false,
    group_name: null,

    show_popup: function() {
        create_group.is_shown = true;
        create_group.group_name = null;
        turtlegui.reload();
    },
    hide_popup: function() {
        create_group.is_shown = false;
        turtlegui.reload();
    },

    create_group: function() {
        loading.inc_loading();
        create_group.hide_popup();
        turtlegui.ajax.post(
            '/admin/api/schemas/' + schema_id + '/groups',
            {'group_name': create_group.name},
            function(data) {
                schema.load_specs();
            },
            function(data) {
                loading.dec_loading();
                alert("Error creating group: " + data.error);
            }
        );
    },

    delete_group: function(group_name) {
        if (confirm("Delete group " + group_name + "?")) {
            turtlegui.ajax.delete(
                '/admin/api/schemas/' + schema_id + '/groups/' + group_name,
                function(data) {
                    schema.load_specs();
                },
                function(data) {
                    loading.dec_loading();
                    alert("Error deleting group: " + data.error);
                }
            );
        }
    }
};


var create_grant = {
    is_shown: false,
    group_name: null,
    grant_type: null,
    url: null,

    show_popup: function(group_name) {
        create_grant.is_shown = true;
        create_grant.group_name = group_name;
        create_grant.grant_type = null;
        create_grant.url = null;
        turtlegui.reload();
    },
    hide_popup: function() {
        create_grant.is_shown = false;
        turtlegui.reload();
    },

    create_grant: function() {
        loading.inc_loading();
        create_grant.hide_popup();
        turtlegui.ajax.post(
            '/admin/api/schemas/' + schema_id + '/groups/' + this.group_name + '/grants',
            {'grant_type': create_grant.grant_type,
             'url': create_grant.url
            },
            function(data) {
                schema.load_specs();
            },
            function(data) {
                loading.dec_loading();
                alert("Error creating grant: " + data.error);
            }
        );
    },

    delete_grant: function(group_name, grant) {
        if (confirm("Delete grant?")) {
            loading.inc_loading();
            turtlegui.ajax.delete(
                '/admin/api/schemas/' + schema_id + '/groups/' + group_name + '/grants/' + grant.grant_type + "/" + grant.url,
                function(data) {
                    schema.load_specs();
                },
                function(data) {
                    loading.dec_loading();
                    alert("Error creating grant: " + data.error);
                }
            );
        }
    }
};


var create_spec = {
    is_shown: false,
    spec_name: null,

    show_popup: function() {
        create_field.field_name = null;
        create_spec.is_shown = true;
        create_spec.spec_name = null;
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
            '/admin/api/schemas/' + schema_id + '/specs',
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
    is_required: false,
    indexed: false,
    unique: false,
    unique_global: false,

    is_editing: false,

    all_field_types: ['int', 'float', 'str', 'bool', 'datetime', 'collection', 'link', 'linkcollection', 'orderedcollection', 'calc', 'file'],

    is_primitive: function() {
        return ['int', 'float', 'str', 'bool', 'datetime', 'file'].indexOf(this.field_type) != -1;
    },

    create_field: function() {
        if (!create_field.field_name) {
            alert("Name is required");
            return;
        }
        turtlegui.ajax.post(
            '/admin/api/schemas/' + schema_id + '/specs/' + create_field.spec_name + '/fields',
            {'field_name': create_field.field_name,
             'field_type': create_field.field_type,
             'field_target': create_field.field_target,
             'calc_str': create_field.calc_str,
             'required': create_field.is_required,
             'indexed': create_field.indexed,
             'unique': create_field.unique,
             'unique_global': create_field.unique_global,
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

    update_field: function() {
        turtlegui.ajax.patch(
            '/admin/api/schemas/' + schema_id + '/specs/' + create_field.spec_name + '/fields/' + create_field.field_name,
            {'field_type': create_field.field_type,
             'field_target': create_field.field_target,
             'calc_str': create_field.calc_str,
             'required': create_field.is_required,
             'indexed': create_field.indexed,
             'unique': create_field.unique,
             'unique_global': create_field.unique_global,
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
        create_field.is_editing = false;
        create_field.is_required = false;
        create_field.indexed = false;
        create_field.unique = false;
        create_field.unique_global = false;
        turtlegui.reload();
    },

    show_edit: function(spec_name, field_name) {
        var field = schema.specs[spec_name].fields[field_name];
        create_field.spec_name = spec_name;
        create_field.show_popup = true;
        create_field.field_name = field_name;
        create_field.field_type = field.type;
        create_field.field_target = field.target_spec_name;
        create_field.calc_str = field.calc_str;
        create_field.is_editing = true;
        create_field.is_required = field.required || false;
        create_field.indexed = field.indexed || false;
        create_field.unique = field.unique || false;
        create_field.unique_global = field.unique_global || false;
        turtlegui.reload();
    },

    hide_popup: function() {
        create_field.show_popup = false;
        turtlegui.reload();
    }
};


var agent = {
    shown: false,
    prompt_text: null,

    check_focus: function() {
        if (event.keyCode == 13 && event.altKey) {
            if (!this.shown) {
                this.shown = true;
                turtlegui.reload();
            }
        }
    },

    submit_prompt: function(text_elem) {
        if (event.keyCode == 13 && event.altKey) {
            turtlegui.ajax.post('/admin/api/schemas/' + schema_id + "/agent",
                {"prompt_text": text_elem.value},
                (d) => {
                    this.prompt_text = null;
                    this.shown = false;
                    schema.load_specs();
                })
        }
    },

    agent_button_pressed: function() {
        this.shown = !this.shown;
        turtlegui.reload();
    }
}


document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    schema.load_specs();
});

