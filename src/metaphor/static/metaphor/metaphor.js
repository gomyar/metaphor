

var resources_loading = 0;



class MResource {
    constructor(m, spec, url, data, params, parent) {
        this._m = m;
        this._spec = spec;
        this._url = url;
        this._params = params;
        this._parent = parent;

        if (data != null) {
            this._assign_data(data);
        }
        this._loading = 0;
        this._meta = {
            is_collection: false,
            spec: { name: spec.name }
        }
    }

    _assign_data(data) {
        this._meta = data._meta;
        var collection_data = {};
        for (var field_name in this._spec.fields) {
            if (field_name in data && this._spec.fields[field_name].is_collection) {
                collection_data[field_name] = this._create_collection(field_name);
                collection_data[field_name]._apply_resource_list(data[field_name]);
                collection_data[field_name]._count = data[field_name].length;
            }
        }
        Object.assign(this, collection_data);
 
        var field_data = this._m.extract_field_data(this._spec, data);
        Object.assign(this, field_data);
    }

    _get(callback) {
        this._loading += 1;
        this._m.fire_event({method: 'GET', type: "get_started", resource: this})
        this._m.net.get(this._url, (data) => {
            this._loading -= 1;
            this._assign_data(data);
            if (callback) {
                callback(data);
            }
            this._m.fire_event({method: 'GET', type: "get_completed", resource: this})
        }, (error) => {
            this._loading -= 1;
            console.log("Error loading", this, error);
            this._m.fire_event({method: 'GET', type: "get_error", resource: this, error: error})
        })
    }

    _get_link(field_name, params) {
        var spec = this._m.schema.specs[this._spec.fields[field_name].target_spec_name];
        this[field_name] = new MResource(this._m, spec, this._url + "/" + field_name, null, null, this);
        this[field_name]._get();
    }

    _unset_field(field_name) {
        delete this[field_name];
        this._m.fire_event({method: 'DELETE', type: "unset_field", resource: this})
    }

    _get_collection(field_name, params) {
        this[field_name] = this._create_collection(field_name, params);
        this[field_name]._get();
    }

    _create_collection(field_name, params) {
        var spec = this._m.schema.specs[this._spec.fields[field_name].target_spec_name];
        return new MCollection(this._m, spec, this._url + "/" + field_name, params, this);
    }

    _patch(data) {
        var patch_data = this._m.extract_field_data(this._spec, data);
        this._loading += 1;
        this._m.fire_event({method: 'PATCH', type: "patch_started", resource: this})
        this._m.net.patch(this._url, patch_data, (data) => {
            this._loading -= 1;
            this._m.fire_event({method: 'PATCH', type: "patch_completed", resource: this})
            this._get();
        }, (error) => {
            this._loading -= 1;
            console.log("Error patching ", this, error);
            this._m.fire_event({method: 'PATCH', type: "patch_error", resource: this, error: error})
        });
    }

    _delete() {
        this._loading += 1;
        this._m.fire_event({method: 'DELETE', type: "delete_started", resource: this})
        this._m.net.delete(this._url, (data) => {
            this._loading -= 1;
            this._m.fire_event({method: 'DELETE', type: "delete_completed", resource: this})
            if (this._parent) {
                this._parent._get();
            }
        }, (error) => {
            this._loading -= 1;
            console.log("Error deleting ", this, error);
            this._m.fire_event({method: 'DELETE', type: "delete_error", resource: this, error: error})
        })
    }
}


class MCollection {
    constructor(m, spec, url, params, parent) {
        this._m = m;
        this._spec = spec;
        this._url = url;
        this._params = params || '';
        this._parent = parent;

        this._page = null;
        this._page_size = null;
        this._count = null;

        this.items = [];
        this._loading = 0;
        this._meta = {
            is_collection: true,
            spec: { name: spec.name }
        }
    }

    _get(callback) {
        var params = new URLSearchParams(this._params);
        if (this._page) params.set('page', this._page);
        if (this._page_size) params.set('page_size', this._page_size);
        this._loading += 1;
        this._m.fire_event({method: 'GET', type: "get_started", resource: this})
        this._m.net.get(this._url + "?" + params.toString(), (data) => {
            this._loading -= 1;
            this._apply_resource_list(data.results);
            this._meta = data._meta;
            this._page_size = data.page_size;
            this._page = data.page;
            this._count = data.count;
            if (callback) {
                callback(data);
            }
            this._m.fire_event({method: 'GET', type: "get_completed", resource: this})
        }, (error) => {
            this._loading -= 1;
            console.log("Error loading", this, error);
            this._m.fire_event({method: 'GET', type: "get_error", resource: this, error: error})
        }) 
    }

    _apply_resource_list(resources) {
        this.items = [];
        var stripped_url = this._url.replace(/\[.*?\]/g, '');;
        for (var r of resources) {
            this.items.push(new MResource(this._m, this._spec, stripped_url + "/" + r['id'], r, null, this));
        }
    }

    _post(data, callback) {
        var post_data = this._m.extract_field_data(this._spec, data);
        this._loading += 1;
        this._m.fire_event({method: 'POST', type: "post_started", resource: this})
        this._m.net.post(this._url, post_data, (response) => {
            this._loading -= 1;
            if (callback) {
                callback(response);
            }
            this._m.fire_event({method: 'POST', type: "post_completed", resource: this})
            this._get();
        }, (error) => {
            this._loading -= 1;
            console.log("Error posting ", this, error);
            this._m.fire_event({method: 'POST', type: "post_error", resource: this, error: error})
        });
    }

    _total_pages() {
        return this._page_size != null ? Math.ceil(this._count / this._page_size): 1;
    }

    _next() {
        if (this._page + 1 < this._total_pages()) {
            this._page += 1;
            this._get();
        }
    }

    _previous() {
        if (this._page > 0) {
            this._page -= 1;
            this._get();
        }
    }

    _last() {
        if (this._page + 1 < this._total_pages()) {
            this._page = this._total_pages() - 1;
            this._get();
        }
    }

    _first() {
        if (this._page > 0) {
            this._page = 0;
            this._get();
        }
    }

    _has_previous() {
        return this._page > 0;
    }

    _has_next() {
        return this._page < this._total_pages() - 1;
    }
}

class Net {
    constructor(m) {
        this._m = m;
        this.loading = 0;
    }

    handle_http_error(error, msg) {
        if (error.status == 401) {
            this._m.fire_event({type: "unauthorized", error: "unauthorized"})
        } else {
            alert(error.status + ": " + msg);    
        }
    }

    _fetch(url, data, callback, error_callback) {
        this.loading += 1;
        data['headers'] = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        };
        fetch(
            this._m.api_root + url,
            data
        ).then((response) => {
            if (response.ok) {
                return response.json();
            } else {
                var error = new Error(response.error);
                error.status = response.status;
                error.statusText = response.statusText;
                throw error;
            }
        }).then((data) => {
            callback(data);
        }).catch((error) => {
            if (error_callback) {
                try {
                    error_callback(error);
                } catch (e) {
                    console.log('Exception reporting error for GET on ', url, e);
                }
            } else {
                this.handle_http_error(error, error.statusText);
            }
        }).finally(() => {
            this.loading -= 1;
        });;
    }

    get(url, callback, error_callback) {
        this._fetch(url, {method: 'GET'}, callback, error_callback);
    }

    post(url, data, callback, error_callback) {
        this._fetch(url, {method: 'POST', body: JSON.stringify(data)}, callback, error_callback);
    }

    patch(url, data, callback, error_callback) {
        this._fetch(url, {method: 'PATCH', body: JSON.stringify(data)}, callback, error_callback);
    }

    delete(url, callback, error_callback) {
        this._fetch(url, {method: 'DELETE'}, callback, error_callback);
    }
}


class Metaphor {
    constructor(api_root, path, params) {
        this.api_root = api_root || '/api';
        this.path = path || '';
        this.params = params;
        this.root = {};
        this.schema = {};
        this.net = new Net(this);
        this.listeners = [];
    }

    load_schema() {
        this.net.get('/schema', (schema_data) => {
            this.schema = schema_data;
            this.load_root_resource();
        });
    }

    load_root_resource() {
        this.net.get(this.path, (data) => {
            var spec = this.schema.specs[data._meta.spec.name]

            if (data._meta.is_collection) {
                this.root = new MCollection(this, spec, this.path, this.params);
            } else {
                this.root = new MResource(this, spec, this.path, null, this.params);
            }
            this.root._get((response) => {
                turtlegui.reload();
            })
        });
    }

    register_listener(callback) {
        this.listeners.push(callback);
    }

    fire_event(event_data) {
        for (var listener of this.listeners) {
            try {
                listener(event_data);
            } catch (e) {
                console.log('Error firing event', e);
            }
        }
    }

    extract_field_data(spec, data) {
        var field_data = {};
        for (var field_name in spec.fields) {
            if (field_name in data && !spec.fields[field_name].is_collection) {
                field_data[field_name] = data[field_name];
            }
        }
        field_data['id'] = data['id'];
        return field_data;
    }
}


