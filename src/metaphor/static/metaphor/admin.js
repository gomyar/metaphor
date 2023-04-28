

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
            this.schemas = JSON.parse(response);
            turtlegui.reload();
        }, handle_http_error);
    }
}


var login = new Login();

document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    admin.load_schemas();
});

