
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
    constructor(spec, select_callback, reload_callback) {
        this.spec = spec;
        this.query = '';
        this.results = [];
        this.reload_callback = reload_callback || turtlegui.reload;
        this.select_callback = select_callback;

        this.results_popup_visible = false;
    }

    perform_search() {
        var query_str = this.query ? '?query=' + this.query: '';
        var self = this;  // todo: get turtlegui's ajax calls working with this
        turtlegui.ajax.get('/search/' + this.spec.name + query_str, function(response) {
            self.results = JSON.parse(response);
            self.show_results_popup();
        }, function(error) {
            alert(error.statusText);
        });
    }

    select_result(result) {
        this.select_callback(result);
        this.hide_results_popup();
    }

    show_results_popup() {
        this.results_popup_visible = true;
        console.log("got results: ", this.results, this.reload_callback, this.results_popup_visible);
        this.reload_callback();
    }

    hide_results_popup() {
        this.results_popup_visible = false;
        this.reload_callback();
    }
}


var search = {
    is_shown: false,
    search_text: null,
    search_spec: null,

    search_text_entered: function(spec) {
        this.is_shown = true;
        this.search_spec = spec;
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
    field_updated: function(field_element) {
        if (event.keyCode == 13) {
            var resource = browser.get_editing_resource();    
            var new_value = browser.parse_value(field_element.value);
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
        return field.type == 'int' || field.type == 'str' || field.type == 'bool' ||  field.type == 'float';
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

    check_esc: function() {
        console.log('event', event);
        if (event.keyCode == 27) {
            browser.hide_create_popup();
        }
    },
};


document.addEventListener("DOMContentLoaded", function(){
    turtlegui.reload();
});

