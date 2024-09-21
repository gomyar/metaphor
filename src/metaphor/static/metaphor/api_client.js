

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
        this.spec = Schema.specs[target_spec_name];
        this.selected_callback = selected_callback;
        this.search_text = '';
        this.result_page = {results: []};
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
        var search_query = this.spec.name + '?query=' + query_str;
        turtlegui.ajax.get('/search/' + search_query, (response) => {
            this.result_page = new Collection(JSON.parse(response), '/search/', search_query);
            turtlegui.reload();
        }, function(error) {
            handle_http_error(error, error.statusText);
        });
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
            api.perform_post_link_to_collection(collection, selected_resource.id);
            this.hide();
        });
        turtlegui.reload();
    }

    show_linkcollection_field_search_and_save(resource, field) {
        this.search = new ResourceSearch(field.target_spec_name, (selected_resource) => {
            api.perform_post_link_to_url(api.api_root + resource[field.name], selected_resource.id);
            resource._fetch();
            this.hide();
        });
        turtlegui.reload();
    }

    show_link_field_search_and_save(resource, field) {
        this.search = new ResourceSearch(field.target_spec_name, (selected_resource) => {
            api.perform_update_resource(resource, field.name, selected_resource.id);
            resource._fetch();
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
            api.api_root + (this.self || ''),
            (success) => {
                var data = JSON.parse(success);
                this._construct(data);
                turtlegui.reload();
            },
            (error) => {
                handle_http_error(error, "Error getting resource at " + this.self + ": " + error.status); 
            });
    }

    _url() {
        return this.self || '/';
    }
}

class Collection {
    constructor(collection, api_root, collection_url) {
        this.results = this._parse_results(collection.results);
        this.count = collection.count;
        this.previous = collection.previous;
        this.next = collection.next;

        this._meta = collection._meta;
        this._meta.spec = Schema.specs[collection._meta.spec.name];

        this._page_size = 10;
        this._page = 0;
        this._api_root = api_root;
        this._collection_url = collection_url;
    }

    _url() {
        return this._collection_url;
    }

    _parse_results(results) {
        var resources = []
        for (var i=0; i<results.length; i++) {
            resources[i] = new Resource(results[i]);
        }
        return resources;
    }

    _total_pages() {
        return Math.ceil(this.count / this._page_size);
    }

    _next() {
        if (this._page + 1 < this._total_pages()) {
            this._page += 1;
            this._fetch();
        }
    }

    _previous() {
        if (this._page > 0) {
            this._page -= 1;
            this._fetch();
        }
    }

    _last() {
        if (this._page + 1 < this._total_pages()) {
            this._page = this._total_pages() - 1;
            this._fetch();
        }
    }

    _first() {
        if (this._page > 0) {
            this._page = 0;
            this._fetch();
        }
    }

    _fetch() {
        turtlegui.ajax.get(
            this._api_root + this._url()+ "?page=" + this._page + "&page_size=" + this._page_size,
            (success) => {
                var collection = JSON.parse(success);

                this.results = this._parse_results(collection.results);
                this.count = collection.count;
                this.previous = collection.previous;
                this.next = collection.next;

                turtlegui.reload();
            },
            (error) => {
                handle_http_error(error, "Error getting api " + this._url()+ ": " + error.status); 
            });
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
        this.show_self_links = false;
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

    load() {
        turtlegui.ajax.get(
            this.full_path(),
            (success) => {
                var resource_data = JSON.parse(success);
                if (this.is_resource(resource_data)) {
                    this.root_resource = new Resource(resource_data);
                } else {
                    this.root_resource = new Collection(resource_data, this.api_root, this.path);
                }
                if (api.path.startsWith('/ego')) {
                    ego_path = api.path;
                }
                turtlegui.reload();
            },
            (error) => {
                handle_http_error(error, "Error getting api " + this.full_path() + ": " + error.status); 
            });
    }

    is_simple(field) {
        return ['str', 'int', 'float'].includes(field.type);
    }

    is_collection(resource) {
        return resource._meta.is_collection;
    }

    is_resource(resource) {
        //return !resource._meta.is_collection;
        return !resource._meta.can_link && !resource._meta.is_collection;
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

    is_linkcollection(resource, field_name) {
        if (resource != null && field_name != null) {
            return resource._meta.can_link && resource._meta.is_collection;
        }
    }

    is_parent_collection(resource, field_name) {
        if (resource != null && field_name != null) {
            return !resource._meta.can_link && resource._meta.is_collection;
        }
    }

    expand_collection(element, resource, field_name, field, ego_path) {
        console.log('Expand');
        turtlegui.ajax.get(
            this.api_root + (ego_path ? ego_path.replaceAll('.', '/') : resource[field_name]),
            (success) => {
                resource._expanded[field_name] = new Collection(JSON.parse(success), this.api_root, resource[field_name]);
                turtlegui.reload(element);
            },
            (error) => {
                handle_http_error(error, "Error loading " + field_name);
            });
    }

    expand_link(element, resource, field_name, field, ego_path) {
        console.log('Expanding', resource, field_name, ego_path);
        turtlegui.ajax.get(
            this.api_root + (ego_path ? ego_path.replaceAll('.', '/') : resource[field_name]),
            (success) => {
                resource._expanded[field_name] = new Resource(JSON.parse(success));
                turtlegui.reload(element);
            },
            (error) => {
                handle_http_error(error, 'Error loading');
            });
    }

    collapse_collection(element, resource, field_name, field) {
        delete resource._expanded[field_name];
        turtlegui.reload(element);
    }

    collapse_link(element, resource, field_name, field) {
        delete resource._expanded[field_name];
        turtlegui.reload(element);
    }

    is_expanded(resource, field_name) {
        return resource._expanded[field_name] != null;
    }

    show_create_resource(parent_resource, target_spec_name, parent_canonical_url, collection) {
        var spec = Schema.specs[target_spec_name];
        this.creating_resource = {
            "_meta": {
                "spec": spec
            }
        }
        this.creating_parent_resource = parent_resource;
        this.creating_resource_url = parent_canonical_url;
        this.creating_collection = collection;
        turtlegui.reload();
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

    can_edit_field(field) {
        return field.type == 'int' || field.type == 'str' || field.type == 'bool' ||  field.type == 'float' || field.type == 'link' || field.type == 'datetime';
    }
 
    perform_create_resource() {
        console.log('Creating', api.creating_resource);
        var data = Object.assign({}, api.creating_resource);
        delete data._meta;
        turtlegui.ajax.post(
            this.api_root + api.creating_resource_url,
            data,
            (success) => {
                console.log('Created');
                api.creating_resource = null;
                if (api.creating_collection) {
                    api.creating_collection._fetch();
                }
                turtlegui.reload();
            },
            (error) => {
                handle_http_error(error, 'Error creating ');
            });
        
    }

    perform_update_resource(resource, field_name, field_value) {
        var data = {};
        data[field_name] = field_value;
        turtlegui.ajax.patch(
            this.api_root + resource.self,
            data,
            (success) => {
                resource._fetch();
            },
            (error) => {
                handle_http_error(error, 'Error updating' + resource.self);
            });
    }

    perform_post_link_to_collection(collection, link_id) {
        turtlegui.ajax.post(
            collection._api_root + collection._url(),
            {id: link_id},
            (success) => {
                collection._fetch();
            },
            (error) => {
                handle_http_error(error, "Error updating " + resource.self);
            });
    }

    perform_post_link_to_url(collection_url, link_id) {
        turtlegui.ajax.post(
            collection_url,
            {id: link_id},
            (success) => {
            },
            (error) => {
                handle_http_error(error, "Error updating " + resource.self);
            });
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

    unlink_from_collection(resource, parent_resource, field_name, ego_path) {
        if (confirm("Unlink resource: " + resource.self + "?")) {
            turtlegui.ajax.delete(
                (ego_path ? this.api_root + ego_path.replaceAll('.', '/') : (parent_resource._api_root + parent_resource._url()) + "/" + resource.id),
                (success) => {
                    parent_resource._fetch()
                },
                (error) => {
                    handle_http_error(error, "Error deleting" + parent_resource._url()+ "/" + resource.id);
                });
        }
    }

    perform_delete_resource(resource, parent_resource, ego_path) {
        if (confirm("Delete resource at: " + resource.self + "?" + " - " + ego_path)) {
            turtlegui.ajax.delete(
                this.api_root + (ego_path ? ego_path.replaceAll('.', '/') : resource.self),
                (success) => {
                    parent_resource._fetch();
                },
                (error) => {
                    handle_http_error(error, "Error deleting" + resource.self);
                });
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

    toggle_show_self_links() {
        this.show_self_links = !this.show_self_links;
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
            this.resources[msg['url']]._fetch();
        }
        if (msg.change.type == 'deleted') {
            if (msg.change.document.self in this.resources) {
                this.remove_resource(this.resources[msg.change.document.self]);
            }
        }
    }

    add_resource(resource) {
        // If empty, establish socket connection
        if (Object.keys(this.resources).length === 0) {
            listen_client.init();
        }
        var url = resource._url();
        if (!(this.resources[url])) {
            console.log('Add resource', resource);
            this.resources[url] = resource;
        }
        if (this.connected) {
            this.socket.emit('add_resource', {'url': url});
        }
        resource._fetch();
    }

    remove_resource(resource) {
        var url = resource._url();
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
        return resource._url() in this.resources;
    }
}

var api = new ApiClient();
var listen_client = new ListenClient();
var login = new Login();

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
    load_initial_api();
});

