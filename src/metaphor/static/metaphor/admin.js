

function handle_http_error(error, msg) {
    if (error.status == 401) {
        login.show_login();
    } else {
        alert(error.status + ": " + (msg || error.statusText));    
    }
}


var admin = {
    schemas: [],

    load_schemas: function() {
        turtlegui.ajax.get('/admin/api/schemas', (response) => {
            this.schemas = JSON.parse(response).schemas;
            turtlegui.reload();
        }, handle_http_error);
    },

    create_schema: function() {
        turtlegui.ajax.post('/admin/api/schemas', {}, (response) => {
            this.load_schemas();
        }, handle_http_error)
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
                    function(data) {
                        create_field.hide_popup();
                        schema.load_specs();
                    },
                    function(data) {
                        loading.dec_loading();
                        alert("Error creating spec: " + data.error);
                    }
                );
                        
            }
        }
    },

}





var login = new Login();

document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    admin.load_schemas();
});

