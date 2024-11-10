

var resources_loading = 0;


function handle_http_error(error, msg) {
    if (error.status == 401) {
        login.show_login();
    } else {
        alert(error.status + ": " + msg);    
    }
}



class MResource {
    constructor(schema, spec, url, data, params) {
        Object.assign(this, data);

        this._schema = schema;
        this._spec = spec;
        this._url = url;
        this._params = params;
    }

    _fetch() {
    }

    _load_collection(field_name, params) {
        var spec = this._schema.specs[this._spec.fields[field_name].target_spec_name];
        this[field_name] = new MCollection(this._schema, spec, this._url + "/" + field_name, params);
        this[field_name]._fetch();
    }
}


class MCollection {
    constructor(schema, spec, url, params) {
        this._schema = schema;
        this._spec = spec;
        this._url = url;
        this._params = params || '';

        this._page = null;
        this._page_size = null;
        this._count = null;

        this.items = [];
        this._loading = 0;
    }

    _fetch() {
        var params = new URLSearchParams(this._params);
        if (this._page) params.set('page', this._page);
        if (this._page_size) params.set('page_size', this._page_size);
        var stripped_url = this._url.replace(/\[.*?\]/g, '');;
        this._loading += 1;
        net.get(this._url + "?" + params.toString(), (data) => {
            this._loading -= 1;
            this.items = [];
            for (var r of data.results) {
                this.items.push(new MResource(this._schema, this._spec, stripped_url + "/" + r['id'], r));
            }
            this._page_size = data.page_size;
            this._page = data.page;
            this._count = data.count;
            turtlegui.reload();
        }, (error) => {
            this._loading -= 1;
            console.log("Error loading ", this, error);
            turtlegui.reload();
        }) 
    }
}

class Net {
    constructor() {
        this.loading = 0;
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
                handle_http_error(error, error.statusText);
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
    constructor() {
        this.root = {}
        this.schema = {}
    }

    load_schema() {
        net.get("/api/schema", (schema_data) => {
            this.schema = schema_data;
            this.root = new MResource(this.schema, {"fields": this.schema.root, "name": "root"}, "/api", {});
        });
    }
}


var net = new Net();
