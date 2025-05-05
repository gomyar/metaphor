

var Schema = {
    root: {},
    specs: {}
};


var ego_path = null;

function handle_http_error(error, msg) {
    if (error.status == 401) {
        login.show_login();
    } else {
        alert(error.status + ": " + msg);    
    }
}


class ResourceSearch {
    constructor(target_spec_name, selected_callback) {
        this.spec = metaphor.schema.specs[target_spec_name];
        this.selected_callback = selected_callback;
        this.search_text = '';
        this.result_page = {results: []};
        this.search_metaphor = new Metaphor("/search", '/' + this.spec.name);
        this.search_metaphor.register_listener((event_data) => {
            if (event_data.type == 'get_completed') {
                turtlegui.reload();
            }
        });
    }

    perform_search() {
        var query_str = "";
        for (var field_name in this.spec.fields) {
            var field = this.spec.fields[field_name];
            if (field.type == 'str') {
                if (query_str) {
                    query_str += "|";
                }
                query_str += field_name + "~'" + this.search_text + "'";
            }
        }
        this.result_page = new MCollection(this.search_metaphor, this.spec, "/" + this.spec.name, {query: query_str});
        this.result_page._get();
    }

}


class Search {

    constructor() {
        this.search=null;
        this.expanded = [];
    }

    show_linkcollection_field_search(resource, field) {
        this.search = new ResourceSearch(field.target_spec_name, (selected_resource) => {
            resource[field.name].push(selected_resource.id);
            this.hide();
        });
        turtlegui.reload();
    }

    show_link_field_search(resource, field) {
        this.search = new ResourceSearch(field.target_spec_name, (selected_resource) => {
            resource[field.name] = selected_resource.id;
            this.hide();
        });
        turtlegui.reload();
    }

    show_create_linkcollection_search(collection) {
        this.search = new ResourceSearch(collection._meta.spec.name, (selected_resource) => {
            collection._post({"id": selected_resource.id}, (data) => {
                this.hide();
            });
        });
        turtlegui.reload();
    }

    show_linkcollection_field_search_and_save(resource, field) {
        this.search = new ResourceSearch(field.target_spec_name, (selected_resource) => {
            var collection = resource[field.name] ? resource[field.name] : resource._create_collection(field.name);
            collection._post({"id": selected_resource.id}, (data) => {
                this.hide();
            });
        });
        turtlegui.reload();
    }

    show_link_field_search_and_save(resource, field) {
        this.search = new ResourceSearch(field.target_spec_name, (selected_resource) => {
            api.perform_update_resource(resource, field.name, selected_resource.id);
            this.hide();
        });
        turtlegui.reload();
    }


    hide() {
        this.search = null;
        turtlegui.reload();
    }

    expand(resource) {
        this.expanded.push(resource);
        turtlegui.reload();
    }

    is_expanded(resource) {
        return this.expanded.indexOf(resource) != -1;
    }

    collapse(resource) {
        var index = this.expanded.indexOf(resource);
        this.expanded.splice(index, index + 1);
        turtlegui.reload();
    }
}

var search = new Search();

class Resource {
    constructor(data) {
        this._expanded = {};
        this._construct(data);
    }

    _construct(data) {
        Object.assign(this, data);
        if (data._meta.spec.name == 'root') {
            this._meta.spec = {
                "name": "root",
                "fields": Schema.root,
            }
        } else {
            this._meta.spec = Schema.specs[data._meta.spec.name];
        }
    }

    _fetch() {
        turtlegui.ajax.get(
            api.api_root + (this._url || ''),
            (success) => {
                var data = JSON.parse(success);
                this._construct(data);
                turtlegui.reload();
            },
            (error) => {
                handle_http_error(error, "Error getting resource at " + this._url + ": " + error.status); 
            });
    }

    _url() {
        return this._url || '/';
    }
}


function parseDate(date_str) {
    return new Date(date_str);
}


class ApiClient {
    constructor() {
        this.api_root = '/api';
        this.path = window.location.pathname.replace(/^\/client/g, '');

        this.root_resource = null;

        this.creating_resource = null;
        this.creating_collection = null;
        this.creating_resource_url = null;
        this.creating_parent_resource = null;

        this.creating_link_spec = null;
        this.creating_link_url = null;
        this.creating_link_id = null;

        this.editing_resource = null;
        this.editing_field = null;
        this.editing_element = null;

        this.parse_funcs = {
            'str': String,
            'int': parseInt,
            'float': parseFloat,
            'datetime': parseDate
        }

        this.show_reverse_links = false;
        this.show_parents = false;
    }

    full_path() {
        return this.api_root + this.path;
    }

    get_breadcrumbs() {
        var crumbs = [];
        crumbs.push({'name': 'api', 'url': '/'});
        var baseurl = '';
        var urls = this.path.split('/');
        for (var i=1; i<urls.length; i++) {
            baseurl = baseurl + '/' + urls[i];
            crumbs.push({'name': urls[i], 'url': baseurl});
        }
        return crumbs;
    }

    is_simple(field) {
        return ['str', 'int', 'float'].includes(field.type);
    }

    is_collection(resource) {
        return resource._meta.is_collection;
    }

    is_not_collection(resource) {
        return (!resource) || (!resource._meta) || (!resource._meta.is_collection);
    }

    is_field_array(field) {
        return field.is_collection;
    }

    is_field_collection(field) {
        return field.type=='collection' || field.type=='orderedcollection' || field.type=='linkcollection';
    }

    is_field_link(field) {
        return field.type=='link';
    }

    is_field_parent_collection(field) {
        return field.type=='parent_collection';
    }

    is_field_reverse_link_collection(field) {
        return field.type=='reverse_link_collection' || field.type=='reverse_link';
    }

    is_linkcollection(resource) {
        return resource._meta.can_link && resource._meta.is_collection;
    }

    is_parent_collection(resource) {
        return !resource._meta.can_link && resource._meta.is_collection;
    }

    expand_collection(resource, field_name) {
        resource._get_collection(field_name);
    }

    collapse_collection(resource, field_name) {
        resource._unset_field(field_name);
    }

    collapse_link(element, resource, field_name, field) {
        resource._unset_field(field_name);
    }

    is_expanded(resource, field_name) {
        return resource[field_name] != null;
    }

    show_create_resource(collection) {
        this.creating_resource = {
            "_meta": {
                "spec": collection._spec
            },
            "_spec": collection._spec
        }
        this.creating_collection = collection;
        turtlegui.reload();
    }

    show_create_resource_from_field(parent_resource, field_name) {
        if (parent_resource[field_name]) {
            this.show_create_resource(parent_resource[field_name]);
        } else {
            var collection = parent_resource._create_collection(field_name);
            this.show_create_resource(collection);
        }
    }

    show_create_link_collection(parent_resource, collection_field) {
        var spec = Schema.specs[collection_field.target_spec_name];
        this.creating_link_spec = spec;
        this.creating_link_url = parent_resource[collection_field.name];
        this.creating_link_id = null;
        turtlegui.reload();
    }

    hide_create_popup() {
        this.creating_resource = null;
        turtlegui.reload();
    }

    hide_create_link_popup() {
        this.creating_link_spec = null;
        turtlegui.reload();
    }


    check_esc() {
        if (event.keyCode == 27) {
            if (search.search) {
                search.hide();
            } else if (this.creating_resource) {
                this.hide_create_popup();
            } else if (this.editing_field) {
                this.editing_field = null;
                this.editing_resource = null;
                turtlegui.reload();
            }
        }
    }

    can_link(resource) {
        return resource._meta.resource_type == 'linkcollection';
    }


    can_create(resource) {
        return resource._meta.resource_type == 'collection' || resource._meta.resource_type == 'orderedcollection';
    }

    can_edit_field(field) {
        return field.type == 'int' || field.type == 'str' || field.type == 'bool' ||  field.type == 'float' || field.type == 'link' || field.type == 'datetime';
    }

    build_client_url(resource, ...fields) {
        var url = resource._url + '/' + (fields ? fields.join('/') : '');
        return '/client' + url;
    }

    build_api_url(resource, ...fields) {
        return resource._m.api_root + resource._url + '/' + (fields ? fields.join('/') : '');
    }
 
    perform_create_resource() {
        console.log('Creating', this.creating_resource);
        this.creating_collection._post(this.creating_resource, (response) => {
            this.creating_resource = null;
            this.creating_collection = null;
            turtlegui.reload();
        });
    }

    perform_update_resource(resource, field_name, field_value) {
        var patch_data = {};
        patch_data[field_name] = field_value;
        resource._patch(patch_data);
    }

    perform_upload_file(elem, resource, field_name) {
        var file = elem.files[0];
        if (file) {
            fetch(this.build_api_url(resource, field_name), {
                method: 'POST',
                headers: {
                'Content-Type': file.type || 'application/octet-stream'
                },
                body: file
            })
            .then(function(response) {
                resource._get();
            }).error(function(error) {
                handle_http_error(error, "Error uploading file");
            });
        } else {
            alert("No file selected");
        }
    }

    set_editing_field(element, resource, field) {
        if (this.can_edit_field(field)) {
            this.editing_resource = resource;
            this.editing_field = field;
            this.editing_value = resource[field.name];
            if (this.editing_element) {
                turtlegui.reload(this.editing_element);
            }
            this.editing_element = element;
            turtlegui.reload(element);
        }
    }

    is_editing_field(resource, field) {
        return this.editing_resource == resource && this.editing_field == field;
    }

    field_updated(field_element) {
        if (event.keyCode == 13) {
            this.editing_resource[this.editing_field.name] = this.parse_funcs[this.editing_field.type](this.editing_value);
            this.perform_update_resource(this.editing_resource, this.editing_field.name, this.editing_resource[this.editing_field.name]);
            this.editing_resource = null;
            this.editing_field = null
            turtlegui.reload(this.editing_element);
            this.editing_element = null;
        } else if (event.keyCode == 27) {
            this.editing_resource = null;
            this.editing_field = null
            turtlegui.reload(field_element);
            turtlegui.reload(this.editing_element);
            this.editing_element = null;
        }
    }

    unlink_field(resource, field) {
        if (confirm("Delete link '" + field.name + "'?")) {
            this.perform_update_resource(resource, field.name, null);
        }
    }

    unlink_from_collection(resource, parent_resource, field_name) {
        if (confirm("Unlink resource: " + resource._url + "?")) {
            resource._delete();
        }
    }

    perform_delete_resource(resource) {
        if (confirm("Delete resource at: " + resource._url)) {
            resource._delete();
        }
    }

    toggle_show_parents() {
        this.show_parents = !this.show_parents;
        turtlegui.reload();
    }

    toggle_show_reverse_links() {
        this.show_reverse_links = !this.show_reverse_links;
        turtlegui.reload();
    }

}


class ListenClient {
    constructor() {
        this.socket = null;
        this.resources = {};
        this.connected = false;
    }

    init() {
        this.socket = io(location.host, {transports: ["websocket", "polling"]});
        this.socket.on('resource_update', (msg) => {this.resource_updated(msg)});
        this.socket.on('lost_stream', (msg) => {this.lost_stream(msg)});

        this.socket.on('connect', () => {
            console.log('Connected');
            this.connected = true;
            for (var url in this.resources) {
                console.log('Listening to ' + url);
                this.add_resource(this.resources[url]);
            }
        });
        this.socket.on('disconnect', () => {
            console.log('Disconnected');
            this.connected = false;
        });

    }

    lost_stream(msg) {
        console.log('Lost stream:' + msg['url']);
        if (msg['url'] in this.resources) {
            console.log('Reestablishing: ' + msg['url']);
            this.socket.emit('add_resource', {'url': msg['url']});
        }
    }

    resource_updated(msg) {
        console.log('Resource updated', msg);
        console.log('type', msg.change.type);
        if (this.resources[msg['url']]) {
            this.resources[msg['url']]._get();
        }
        if (msg.change.type == 'deleted') {
            if (msg.change.document._url in this.resources) {
                this.remove_resource(this.resources[msg.change.document._url]);
            }
        }
    }

    add_resource(resource) {
        // If empty, establish socket connection
        if (Object.keys(this.resources).length === 0) {
            listen_client.init();
        }
        var url = resource._url;
        if (!(this.resources[url])) {
            console.log('Add resource', resource);
            this.resources[url] = resource;
        }
        if (this.connected) {
            this.socket.emit('add_resource', {'url': url});
        }
        resource._get();
    }

    remove_resource(resource) {
        var url = resource._url;
        if (this.resources[url]) {
            console.log('Remove resource', resource);
            delete this.resources[url];
            this.socket.emit('remove_resource', {'url': url});
        }
        // If empty, disconnect socket connection
        if (Object.keys(this.resources).length === 0) {
            console.log('Disconnecting socket');
            listen_client.socket.disconnect();
        }
        turtlegui.reload();
    }

    is_listening(resource) {
        return resource._url in this.resources;
    }
}


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
            turtlegui.ajax.post('/api/agent',
                {"prompt_text": text_elem.value},
                (d) => {
                    this.prompt_text = null;
                    this.shown = false;
                    turtlegui.reload();
                })
        }
    },

    agent_button_pressed: function() {
        this.shown = !this.shown;
        turtlegui.reload();
    }
}


var api = new ApiClient();
var listen_client = new ListenClient();

var listen_dropdown = {
    is_open: false,

    toggle: function() {
        this.is_open = !this.is_open;
        turtlegui.reload();
    }
}


function load_initial_api() {
    turtlegui.ajax.get('/api/schema', function(response) {
        Schema = JSON.parse(response);
        api.load();
    }, (error) => {
        handle_http_error(error, "Cannot load schema");
    });
}


document.addEventListener("DOMContentLoaded", function(){
    var path = window.location.pathname.replace(/^\/client\//, '/');
    if (path.endsWith('/')) { path = path.slice(0, -1); }
    metaphor = new Metaphor('/api', path, window.location.search);
    login = new Login(metaphor);
    metaphor.register_listener((event_data) => {
        if (event_data.type == "unauthorized") {
            login.show_login();
        } else {
            if (event_data.error) {
                handle_http_error(event_data.error, "Error");
            }
            turtlegui.reload();
        }
    });
    metaphor.load_schema();
});

