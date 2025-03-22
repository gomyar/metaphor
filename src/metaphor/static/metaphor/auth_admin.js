

var auth_admin = {
    user_groups: {},
    adding_user: null,

    load: function() {
        turtlegui.ajax.get("/auth/api/usergroups", (data) => {
            this.user_groups = JSON.parse(data);
            turtlegui.reload();
        });
    },

    remove_user_from_group: function(user, group_name) {
        if (confirm("Remove user " + user.email + " from group " + group_name + "?")) {
            turtlegui.ajax.delete("/auth/api/usergroups/" + group_name + "/users/" + user.user_id, () => {
                this.load();
            });
        }
    },

    show_add_to_group: function(user) {
        this.adding_user = user;
        turtlegui.reload();
    },

    add_user_to_group: function(group_name) {
        if (this.adding_user) {
            turtlegui.ajax.post("/auth/api/usergroups/" + group_name + "/users", {"user_id": this.adding_user.user_id}, (data) => {
                this.adding_user = null;
                this.load();
            });
        }
    },

    should_show_adding_group: function(group_name) {
        return this.adding_user != null && this.adding_user.groups.indexOf(group_name) == -1;
    }
}


var invite_user = {
    shown: false,
    email: null,
    groups: [],
    admin: false,
    adding_group: null,

    show: function() {
        this.shown = true;
        turtlegui.reload();
    },

    close: function() {
        this.shown = false;
        turtlegui.reload();
    },

    add_group: function() {
        if (this.adding_group && this.groups.indexOf(this.adding_group) == -1) {
            this.groups.push(this.adding_group);
            turtlegui.reload();
        }
    },

    remove_group: function(group_name) {
        this.groups.splice(this.groups.indexOf(group_name), 1);
        turtlegui.reload();
    },

    invite: function() {
        turtlegui.ajax.post("/auth/api/identities", {
            "email": this.email,
            "groups": this.groups,
            "admin": this.admin
        }, () => {
            this.close();
        });
    }
}


document.addEventListener("DOMContentLoaded", function(){
    metaphor = new Metaphor('/api', '/', window.location.search);
    login = new Login(metaphor);
    metaphor.register_listener((event_data) => {
        if (event_data.type == "unauthorized") {
            login.show_login();
        } else {
            if (event_data.error) {
                handle_http_error(event_data.error, "Error");
            }
            turtlegui.reload();
        }
    });
    metaphor.load_schema();
    auth_admin.load();
});

