

var resources_loading = 0;



class MResource {
    constructor(m, spec, url, data, params) {
        this._m = m;
        Object.assign(this, data);

        this._spec = spec;
        this._url = url;
        this._params = params;
        this._loading = 0;
    }

    _get() {
        this._loading += 1;
        this._m.fire_event({method: 'GET', type: "get_started", resource: this})
        this._m.net.get(this._url, (data) => {
            this._loading -= 1;
            this._m.fire_event({method: 'GET', type: "get_completed", resource: this})
        }, (error) => {
            this._loading -= 1;
            console.log("Error loading", this, error);
            this._m.fire_event({method: 'GET', type: "get_error", resource: this, error: error})
        })
    }

    _get_collection(field_name, params) {
        var spec = this._m.schema.specs[this._spec.fields[field_name].target_spec_name];
        this[field_name] = new MCollection(this._m, spec, this._url + "/" + field_name, params);
        this[field_name]._get();
    }

    _patch(data) {
        var patch_data = this._m.extract_field_data(this._spec, data);
        this._loading += 1;
        this._m.fire_event({method: 'PATCH', type: "patch_started", resource: this})
        this._m.net.patch(this._url + "/" + field_name, patch_data, (data) => {
            this._loading -= 1;
            Object.assign(this, patch_data);
            this._m.fire_event({method: 'PATCH', type: "patch_completed", resource: this})
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
        }, (error) => {
            this._loading -= 1;
            console.log("Error deleting ", this, error);
            this._m.fire_event({method: 'DELETE', type: "delete_error", resource: this, error: error})
        })
    }
}


class MCollection {
    constructor(m, spec, url, params) {
        this._m = m;
        this._spec = spec;
        this._url = url;
        this._params = params || '';

        this._page = null;
        this._page_size = null;
        this._count = null;

        this.items = [];
        this._loading = 0;
    }

    _get() {
        var params = new URLSearchParams(this._params);
        if (this._page) params.set('page', this._page);
        if (this._page_size) params.set('page_size', this._page_size);
        var stripped_url = this._url.replace(/\[.*?\]/g, '');;
        this._loading += 1;
        this._m.fire_event({method: 'GET', type: "get_started", resource: this})
        this._m.net.get(this._url + "?" + params.toString(), (data) => {
            this._loading -= 1;
            this.items = [];
            for (var r of data.results) {
                this.items.push(new MResource(this._m.schema, this._spec, stripped_url + "/" + r['id'], r));
            }
            this._page_size = data.page_size;
            this._page = data.page;
            this._count = data.count;
            this._m.fire_event({method: 'GET', type: "get_completed", resource: this})
        }, (error) => {
            this._loading -= 1;
            console.log("Error loading", this, error);
            this._m.fire_event({method: 'GET', type: "get_error", resource: this, error: error})
        }) 
    }

    _post(data) {
        var post_data = this._m.extract_field_data(this._spec, data);
        this._loading += 1;
        this._m.fire_event({method: 'POST', type: "post_started", resource: this})
        this._m.net.post(this._url, post_data, (response) => {
            this._loading -= 1;
            this._get();
        }, (error) => {
            this._loading -= 1;
            console.log("Error posting ", this, error);
            this._m.fire_event({method: 'POST', type: "post_error", resource: this, error: error})
        });
    }
}

class Net {
    constructor(m) {
        this._m = m;
        this.loading = 0;
    }

    handle_http_error(error, msg) {
        alert(error.status + ": " + msg);    
    }

    extract_field_data(spec, data) {
        var field_data = {};
        for (var field_name in this._spec.fields) {
            if (field_name in data && !this._spec.fields[field_name].is_collection) {
                field_data[field_name] = data[field_name];
            }
        }
        return field_data;
    }

    _fetch(url, data, callback, error_callback) {
        this.loading += 1;
        fetch(url).then((response) => {
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

    post(url, callback, error_callback) {
        this._fetch(url, {method: 'POST', body: JSON.stringify(data)}, callback, error_callback);
    }

    patch(url, callback, error_callback) {
        this._fetch(url, {method: 'PATCH', body: JSON.stringify(data)}, callback, error_callback);
    }

    delete(url, callback, error_callback) {
        this._fetch(url, {method: 'DELETE'}, callback, error_callback);
    }
}


class Metaphor {
    constructor(api_url) {
        this.api_url = api_url || "/api";
        this.root = {};
        this.schema = {};
        this.net = new Net(this);
        this.listeners = [];
    }

    load_schema() {
        this.net.get(this.api_url + "/schema", (schema_data) => {
            this.schema = schema_data;
            this.root = new MResource(this, {"fields": this.schema.root, "name": "root"}, this.api_url, {});
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
}

metaphor = new Metaphor("/api");
metaphor.register_listener((event_data) => {
    console.log(event_data);
});
metaphor.load_schema();
