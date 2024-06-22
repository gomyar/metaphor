

function handle_http_error(error, msg) {
    if (error.status == 401) {
        login.show_login();
    } else {
        alert(error.status + ": " + (msg || error.statusText));    
    }
}


class Mutation {
    constructor(to_schema, from_schema) {
        this.from_schema = from_schema;
        this.to_schema = to_schema;
    }
}


var admin = {
    schemas: [],
    current_schema: null,

    load_schemas: function() {
        turtlegui.ajax.get('/admin/api/schemas', (response) => {
            var schemas = JSON.parse(response).schemas;
            admin.schemas = [];
            for (var i=0; i<schemas.length; i++) {
                if (schemas[i].current) {
                    admin.current_schema = schemas[i];
                } else {
                    admin.schemas.push(schemas[i]);
                }
            }
            turtlegui.reload();
        }, handle_http_error);
    },

    get_schema: function(schema_id) {
        if (schema_id == this.current_schema.id) return this.current_schema;
        for (schema of this.schemas) if (schema_id == schema.id) return schema;
    },

    create_schema: function() {
        turtlegui.ajax.post('/admin/api/schemas', {}, (response) => {
            admin.load_schemas();
        }, handle_http_error)
    },

    copy_schema: function(schema) {
        var name = prompt("Copy schema with new name:", schema.name);
        if (name) {
            turtlegui.ajax.post('/admin/api/schemas', {"_from_id": schema.id, "name": name}, (response) => {
                admin.load_schemas();
            }, handle_http_error);
        }
    },

    delete_schema: function(schema) {
        if (confirm("Delete schema?")) {
            turtlegui.ajax.delete('/admin/api/schemas/' + schema.id, (response) => {
                admin.load_schemas();
            }, handle_http_error);
        }
    }
}


var schema_import = {
    import_schema: (file_element) => {
        var file = file_element.files[0];

        var reader = new FileReader();
        reader.readAsText(file, 'UTF-8');

        reader.onload = readerEvent => {
            var content = JSON.parse(readerEvent.target.result);
            if (confirm("Import Schema?")) {
                turtlegui.ajax.post(
                    '/admin/api/schemas',
                    content,
                    admin.load_schemas,
                    function(data) {
                        loading.dec_loading();
                        alert("Error creating spec: " + data.error);
                    }
                );
            }
        }
    }
}


var mutations = {
    mutation: null,
    to_schema_index: null,
    from_schema_index: null,

    show_create: function(schema) {
        this.mutation = new Mutation(schema, admin.current_schema);
        turtlegui.reload();
    },

    close_create: function() {
        this.mutation = null;
        turtlegui.reload();
    },

    perform_create: function() {
        turtlegui.ajax.post(
            '/admin/api/mutations',
            {"to_schema_id": this.mutation.to_schema.id,
             "from_schema_id": this.mutation.from_schema.id},
            (d) => {mutations.mutation = null; admin.load_schemas()});
    },

    promote_schema: function(mutation) {
        if (confirm("Promote this schema to current?")) {
            turtlegui.ajax.patch('/admin/api/mutations/' + mutation.id, {"promote": true}, (data) => {
                manage.load();
            }, (e) => {
                alert("Error promoting: " + e);
            });
        }
    },

    delete_mutation: function(mutation) {
        if (confirm("Delete mutation?")) {
            turtlegui.ajax.delete('/admin/api/mutations/' + mutation.id, () => {
                manage.load(); 
            }, (e) => {
                alert("Error deleting: " + e);
            });
        }
    }
}

var login = new Login();

document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    admin.load_schemas();
});

