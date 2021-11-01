

var Schema = {
    root: {},
    specs: {}
};


class Resource {
    constructor(data) {
        Object.assign(this, data);
        this._expanded = {};
        if (data._meta.spec.name == 'root') {
            this._meta.spec = Schema.root;
        } else {
            this._meta.spec = Schema.specs[data._meta.spec.name];
        }
    }
}

class Collection {
    constructor(collection, collection_url) {
        this.results = this._parse_results(collection.results);
        this.count = collection.count;
        this.previous = collection.previous;
        this.next = collection.next;

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
        return ['str', 'int', 'float', 'bool'].includes(field.type);
    }

    is_collection(resource) {
        return resource instanceof Collection;
    }

    is_resource(resource) {
        return resource._meta != undefined;
    }

    is_field_collection(field) {
        return field.is_collection;
    }

    is_field_link(field) {
        return field.type=='link';
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
}

var api = new ApiClient();


document.addEventListener("DOMContentLoaded", function(){
    turtlegui.ajax.get('/admin/schema_editor/api', function(response) {
        Schema = JSON.parse(response);
        api.load();
    });
});

