
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

var not = function(value) {
    return !(value);
}

var browser = {
    editing_field_name: null,
    editing_resource_id: null,
    creating_resource_spec: null,
    creating_resource_fields: {},

    is_field_link: function(field) {
        return field.field_type == 'link';
    },
    create_href: function(field_value) {
        return window.location.protocol + '//' + window.location.host + "/browser" + field_value;
    },
    create_relative_href: function(resource_root, relative_path) {
        return window.location.protocol + '//' + window.location.host + "/browser" + resource_root + "/" + relative_path;
    },
    create_relative_link: function(resource_root, relative_path) {
        return resource_root + "/" + relative_path;
    },

    set_editing_field: function(resource_id, field_name) {
        browser.editing_field_name = field_name;
        browser.editing_resource_id = resource_id;
        turtlegui.reload();
        var input_element = document.getElementById(browser.field_edit_id(resource_id, field_name));
        input_element.value = browser.get_editing_resource()[field_name];
        input_element.focus();
    },
    is_editing_field: function(resource_id, field_name) {
        return resource_id == browser.editing_resource_id && field_name == browser.editing_field_name;
    },
    get_editing_resource: function() {
        if (api.is_collection) {
            for (var index in api.resource) {
                if (api.resource[index].id == browser.editing_resource_id) {
                    return api.resource[index];
                }
            }
        } else {
            return api.resource;
        }
    },
    field_edit_id: function(resource_id, field_name) {
        return "field_edit_" + resource_id + field_name;
    },
    parse_value: function(value) {
        var field = api.spec.fields[browser.editing_field_name];
        if (field.type == 'int') {
            return parseInt(value);
        } else if (field.type == 'float') {
            return parseFloat(value);
        } else {
            return value;
        }
    },
    field_updated: function() {
        if (event.keyCode == 13) {
            var resource = browser.get_editing_resource();    
            var new_value = browser.parse_value(this.value);
            resource[browser.editing_field_name] = new_value;
            var resource_url = window.location.protocol + '//' + window.location.host + "/api" + resource.self;
            var resource_data = {};
            resource_data[browser.editing_field_name] = new_value;

            loading.inc_loading();
            turtlegui.ajax.patch(
                resource_url, 
                resource_data,
                function(data) {
                    console.log("Updated");
                    browser.editing_field_name = null;
                    browser.editing_resource_id = null;
                    loading.dec_loading();
                },
                loading.dec_loading);
        } else if (event.keyCode == 27) {
            browser.editing_field_name = null;
            browser.editing_resource_id = null;
            turtlegui.reload();
        }
    },
    show_create_popup: function() {
        browser.creating_resource_spec = api.spec;
        browser.creating_resource_fields = {};
        turtlegui.reload();
    },
    hide_create_popup: function() {
        browser.creating_resource_spec = null;
        browser.creating_resource_fields = {};
        turtlegui.reload();
    },
    can_edit_field: function(field) {
        return field.type == 'int' || field.type == 'str';
    }
};


document.addEventListener("DOMContentLoaded", function(){
    turtlegui.reload();
});

