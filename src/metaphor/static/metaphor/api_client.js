

var Schema = {
    root: {},
    specs: {}
};



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
        var search_query = '/search/' + this.spec.name + '?query=' + query_str;
        turtlegui.ajax.get(search_query, (response) => {
            this.result_page = new Collection(JSON.parse(response), search_query);
            turtlegui.reload();
        }, function(error) {
            alert(error.statusText);
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
            this.hide();
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
        this._construct(data);
    }

    _construct(data) {
        Object.assign(this, data);
        this._expanded = {};
        if (data._meta.spec.name == 'root') {
            this._meta.spec = Schema.root;
        } else {
            this._meta.spec = Schema.specs[data._meta.spec.name];
        }
    }

    _fetch() {
        turtlegui.ajax.get(
            api.api_root + this.self,
            (success) => {
                var data = JSON.parse(success);
                this._construct(data);
                turtlegui.reload();
            },
            (error) => {
                alert("Error getting resource at " + this.self + ": " + error.status); 
            });
    }
}

class Collection {
    constructor(collection, collection_url) {
        this.results = this._parse_results(collection.results);
        this.count = collection.count;
        this.previous = collection.previous;
        this.next = collection.next;

        this._meta = collection._meta;
        this._meta.spec = Schema.specs[collection._meta.spec.name];

        this._page_size = 10;
        this._page = 0;
        this._collection_url = collection_url;
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
            this._collection_url + "?page=" + this._page + "&page_size=" + this._page_size,
            (success) => {
                var collection = JSON.parse(success);

                this.results = this._parse_results(collection.results);
                this.count = collection.count;
                this.previous = collection.previous;
                this.next = collection.next;

                turtlegui.reload();
            },
            (error) => {
                alert("Error getting api " + this._collection_url + ": " + error.status); 
            });
    }
}


class ApiClient {
    constructor() {
        this.api_root = '/api';
        this.path = window.location.pathname.replace(/^\/client/g, '');

        this.root_resource = null;

        this.creating_resource = null;
        this.creating_resource_url = null;

        this.creating_link_spec = null;
        this.creating_link_url = null;
        this.creating_link_id = null;

        this.editing_resource = null;
        this.editing_field = null;
        this.editing_element = null;

        this.parse_funcs = {
            'str': String,
            'int': parseInt,
            'float': parseFloat
        }
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
                    this.root_resource = new Collection(resource_data, this.full_path());
                }
                turtlegui.reload();
            },
            (error) => {
                alert("Error getting api " + this.full_path() + ": " + error.status); 
            });
    }

    is_simple(field) {
        return ['str', 'int', 'float'].includes(field.type);
    }

    is_collection(resource) {
        return resource._meta.is_collection;
    }

    is_resource(resource) {
        return !resource._meta.is_collection;
    }

    is_field_collection(field) {
        return field.is_collection;
    }

    is_field_link(field) {
        return field.type=='link' || field.type=='parent_collection';
    }

    expand_collection(element, resource, field_name, field) {
        turtlegui.ajax.get(
            this.api_root + resource[field_name],
            (success) => {
                resource._expanded[field_name] = new Collection(JSON.parse(success), this.api_root + resource[field_name]);
                turtlegui.reload(element);
            },
            (error) => {
                alert("Error loading " + field_name);
            });
    }

    expand_link(element, resource, field_name, field) {
        console.log('Expanding', resource, field_name);
        turtlegui.ajax.get(
            this.api_root + resource[field_name],
            (success) => {
                resource._expanded[field_name] = new Resource(JSON.parse(success));
                turtlegui.reload(element);
            },
            (error) => {
                console.log('Error loading', error);
                alert("Error loading " + field_name);
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

    show_create_resource(parent_resource, collection_field) {
        var spec = Schema.specs[collection_field.target_spec_name];
        this.creating_resource = {
            "_meta": {
                "spec": spec
            }
        }
        this.creating_resource_url = parent_resource[collection_field.name];
        turtlegui.reload();
    }

    show_create_resource(parent_resource, target_spec_name, parent_canonical_url) {
        var spec = Schema.specs[target_spec_name];
        this.creating_resource = {
            "_meta": {
                "spec": spec
            }
        }
        this.creating_resource_url = parent_canonical_url;
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
        return field.type == 'int' || field.type == 'str' || field.type == 'bool' ||  field.type == 'float' || field.type == 'link';
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
                turtlegui.reload();
            },
            (error) => {
                console.log('Error creating ', error);
                alert("Error creating " + api.creating_resource);
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
                console.log('Error updating', error);
                alert("Error updating " + resource.self);
            });
    }

    perform_post_link_to_collection(collection, link_id) {
        turtlegui.ajax.post(
            collection._collection_url,
            {id: link_id},
            (success) => {
                collection._fetch();
            },
            (error) => {
                console.log('Error updating', error);
                alert("Error updating " + resource.self);
            });
    }

    perform_delete_resource(resource, resource_element) {
        if (confirm("Delete resource at: " + resource.self + "?")) {
            turtlegui.ajax.delete(
                this.api_root + resource.self,
                (success) => {
                    resource_element.remove();
                },
                (error) => {
                    console.log('Error deleting', error);
                    alert("Error deleting" + resource.self);
                });
        }
    }

    perform_post_link_to_url(collection_url, link_id) {
        turtlegui.ajax.post(
            collection_url,
            {id: link_id},
            (success) => {
            },
            (error) => {
                console.log('Error updating', error);
                alert("Error updating " + resource.self);
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

    unlink_from_collection(resource, parent_resource, field_name) {
        console.log('Unlinking', resource);
        console.log('from', parent_resource);
        console.log('field_name', field_name);
        if (confirm("Delete resource at: " + resource.self + "?")) {
            turtlegui.ajax.delete(
                parent_resource._collection_url + "/" + resource.id,
                (success) => {
                    parent_resource._fetch()
                },
                (error) => {
                    console.log('Error deleting', error);
                    alert("Error deleting" + parent_resource._collection_url + "/" + resource.id);
                });
        }
 
    }

    unlink_field(resource, field) {
        this.perform_update_resource(resource, field.name, null);
    }

    delete_resource(resource, parent_element) {
        console.log('Delete', resource);
        this.perform_delete_resource(resource, parent_element);
    }
}



var api = new ApiClient();


document.addEventListener("DOMContentLoaded", function(){
    turtlegui.ajax.get('/admin/schema_editor/api', function(response) {
        Schema = JSON.parse(response);
        api.load();
    });
});

