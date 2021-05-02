
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


class ResourceSearch {
    constructor(spec, select_callback) {
        this.search_spec = spec;
        this.select_callback = select_callback;
        this.search_text = '';
        this.results = [];
    }

    perform_search() {
        var query_str = "";
        for (var field_name in this.search_spec.fields) {
            var field = this.search_spec.fields[field_name];
            if (field.type == 'str') {
                if (query_str) {
                    query_str += "|";
                }
                query_str += field_name + "~'" + this.search_text + "'";
            }
        }
        turtlegui.ajax.get('/search/' + this.search_spec.name + '?query=' + query_str, (response) => {
            this.results = JSON.parse(response);
            turtlegui.reload();
        }, function(error) {
            alert(error.statusText);
        });
    }

    select_result(result) {
        this.select_callback(result);
    }
}


var search = {
    search: null,

    show: function(spec_name, select_callback) {
        this.search = new ResourceSearch(api.schema.specs[spec_name], select_callback);
        turtlegui.reload();
        return false;
    },

    hide: function() {
        this.search = null;
        turtlegui.reload();
    }
}


var browser = {
    editing_field_name: null,
    editing_resource_id: null,
    creating_resource_spec: false,
    creating_resource_fields: {},
    creating_link: false,
    collection_search: new ResourceSearch(api.spec, function(result) {
        console.log("Selected: ", result);
    }),

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
    field_updated: function(field_element) {
        if (event.keyCode == 13) {
            var new_value = browser.parse_value(field_element.value);
            var resource = browser.get_editing_resource();    
            browser._perform_field_update(resource, browser.editing_field_name, new_value);
        } else if (event.keyCode == 27) {
            browser.editing_field_name = null;
            browser.editing_resource_id = null;
            turtlegui.reload();
        }
    },
    _perform_field_update: function(resource, field_name, field_value) {
        resource[field_name] = field_value;
        var resource_url = window.location.protocol + '//' + window.location.host + "/api" + resource.self;
        var resource_data = {};
        resource_data[field_name] = field_value;

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
    },
    unlink_field: function(resource, field_name) {
        if (confirm("Unlink field: " + field_name + "?")) {
            browser._perform_field_update(resource, field_name, null);
        }
        return false;
    },
    show_create_popup: function() {
        browser.creating_resource_spec = true;
        browser.creating_resource_fields = {};
        turtlegui.reload();
    },
    hide_create_popup: function() {
        browser.creating_resource_spec = false;
        browser.creating_resource_fields = {};
        turtlegui.reload();
    },
    can_edit_field: function(field) {
        return field.type == 'int' || field.type == 'str' || field.type == 'bool' ||  field.type == 'float' || field.type == 'link';
    },
    perform_create: function() {
        var resource_url = window.location.protocol + '//' + window.location.host + "/api/" + api.path;
        loading.inc_loading();
        turtlegui.ajax.post(
            resource_url,
            browser.creating_resource_fields,
            function(data) {
                console.log("Created");
                browser.creating_resource_spec = false;
                browser.creating_resource_fields = {};
                location.reload();
            },
            loading.dec_loading);
    },
    show_create_link_popup: function() {
        browser.creating_link = true;
        browser.creating_link_value = null;
        turtlegui.reload();
    },
    hide_create_link_popup: function() {
        browser.creating_link = false;
        browser.creating_link_value = {};
        turtlegui.reload();
    },
    perform_create_link: function() {
        var resource_url = window.location.protocol + '//' + window.location.host + "/api/" + api.path;
        loading.inc_loading();
        turtlegui.ajax.post(
            resource_url,
            {'id': browser.creating_link_value},
            function(data) {
                console.log("Linked");
                browser.creating_link = false;
                browser.creating_link_value = null;
                location.reload();
            },
            loading.dec_loading);
    },
    delete_resource: function(resource) {
        if (confirm("Are you sure you wish to delete : " + resource.id + " ?")) {
            browser._perform_delete(resource);
        }
    },
    delete_link: function(resource) {
        if (confirm("Are you sure you wish to delete link to : " + resource.id + " ?")) {
            browser._perform_delete(resource);
        }
    },
    _perform_delete: function(resource) {
        loading.inc_loading();
        var resource_url = window.location.protocol + '//' + window.location.host + "/api" + resource.self;
        turtlegui.ajax.delete(
            resource_url,
            function(data) {
                console.log("Deleted");
                location.reload();
            },
            loading.dec_loading
        );
    },

    show_link_search: function(field_name, field) {
        search.show(field.target_spec_name, function(result) {
            browser.creating_resource_fields[field_name] = result.id;
            search.hide();
        });
    },

    show_resource_link_search: function(resource, field_name, field) {
        search.show(field.target_spec_name, function(result) {
            console.log('Selecting', result);
            resource[field_name] = result.id;
            var resource_url = window.location.protocol + '//' + window.location.host + "/api" + resource.self;
            var resource_data = {};
            resource_data[field_name] = result.id;
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
            search.hide();
        });
    },


    check_esc: function() {
        if (event.keyCode == 27) {
            if (search.is_shown) {
                search.hide();
            } else {
                browser.hide_create_popup();
            }
        }
    },
};


document.addEventListener("DOMContentLoaded", function(){
    turtlegui.reload();
});

