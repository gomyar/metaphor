

var Schema = {
    root: {},
    specs: {}
};


class Resource {
    constructor(data) {
        Object.assign(this, data);
        this._expanded = {};
        if (data._meta.spec.name == 'root') {
            this._meta.spec = {'fields': Schema.root};
            this._meta.spec.name = 'root';
        } else {
            this._meta.spec = Schema.specs[data._meta.spec.name];
            this._meta.spec.name = data._meta.spec.name;
        }
    }
}

class Collection {
    constructor(collection) {
        for (var i=0; i<collection.results.length; i++) {
            collection.results[i] = new Resource(collection.results[i]);
        }
        Object.assign(this, collection);
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

    load() {
        turtlegui.ajax.get(
            this.full_path(),
            (success) => {
                var resource_data = JSON.parse(success);
                if (this.is_resource(resource_data)) {
                    this.root_resource = new Resource(resource_data);
                } else {
                    this.root_resource = new Collection(resource_data);
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
        return resource._meta == undefined && resource.count != null && resource.results !=null;
    }

    is_resource(resource) {
        return resource._meta != undefined;
    }

    expand_collection(element, resource, field_name, field) {
        turtlegui.ajax.get(
            this.api_root + resource[field_name],
            (success) => {
                resource._expanded[field_name] = new Collection(JSON.parse(success));
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

