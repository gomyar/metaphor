

class Integration {
    constructor() {
        this.id = null;
        this.name = null;
        this.mongo_connection = null;
        this.mongo_db = null;
        this.mongo_collection = null;
        this.mongo_aggregation = null;
        this.change_stream_callback = null;
        this.last_update = null;
        this.state = 'stopped';
    }
}


var integrations = {
    integrations: [],
    new_integration: null,

    load_integrations: function() {
        turtlegui.ajax.get(
            '/admin/integrations/api',
            (data) => {
                this.integrations = JSON.parse(data);
                turtlegui.reload();
            },
            (data) => {
                alert("Error loading spec: " + data.error);
            }
        );
    },

    show_create_popup: function() {
        this.new_integration = new Integration();
        turtlegui.reload();
    },

    perform_create: function() {
        turtlegui.ajax.post(
            '/admin/integrations/api',
            this.new_integration,
            (data) => {
                this.new_integration = null;
                this.load_integrations();
            },
            (data) => {
                alert("Error creating integration: " + data.error);
            }
        );
    },

    perform_update: function() {
        turtlegui.ajax.patch(
            '/admin/integrations/api/' + this.new_integration.id,
            this.new_integration,
            (data) => {
                this.new_integration = null;
                this.load_integrations();
            },
            (data) => {
                alert("Error creating integration: " + data.error);
            }
        );
    },

    edit_integration: function(integration) {
        this.new_integration = integration;
        turtlegui.reload();
    },

    close_create: function() {
        this.new_integration = null;
        turtlegui.reload();
    },

    start_integration: function(integration) {
         turtlegui.ajax.patch(
            '/admin/integrations/api/' + integration.id,
            {'id': integration.id,
             'state': 'starting'},
            (data) => {
                this.load_integrations();
            },
            (data) => {
                alert("Error starting integration: " + data.error);
            }
        );
    },

    stop_integration: function(integration) {
         turtlegui.ajax.patch(
            '/admin/integrations/api/' + integration.id,
            {'id': integration.id,
             'state': 'stopping'},
            (data) => {
                this.load_integrations();
            },
            (data) => {
                alert("Error stopping integration: " + data.error);
            }
        );
    },

    delete_integration: function(integration) {
         turtlegui.ajax.delete(
            '/admin/integrations/api/' + integration.id,
            (data) => {
                this.load_integrations();
            },
            (data) => {
                alert("Error deleting integration: " + data.error);
            }
        );
    }


}

document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    integrations.load_integrations();
});

