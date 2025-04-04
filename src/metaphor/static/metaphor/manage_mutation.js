
function handle_http_error(error, msg) {
    if (error.status == 401) {
        login.show_login();
    } else {
        try {
            var err_data = JSON.parse(error.responseText);
            alert(err_data.error);
        } catch {
            alert(error.status + ": " + msg);
        }
    }
}


var manage = {
    mutation: null,
    spec_names: [],
    diff: [],
    step: null,
    tab: 'diff',

    load: function() {
        turtlegui.ajax.get('/admin/api/mutations/' + mutation_id, (data) => {
            manage.mutation = JSON.parse(data);
            manage.spec_names = manage.all_spec_names();
            manage.diff = [];
            manage.create_diff();
            group_diff.create_diff();
            turtlegui.reload();
        });
    },

    reload_from_step: function() {
        turtlegui.ajax.get('/api' + this.step.from_path + '?page_size=1', (data) => {
            var result = JSON.parse(data);
            if (result._meta.is_collection) {
                manage.step.from_spec = manage.mutation.from_schema.specs[result._meta.spec.name];
                manage.step.from_is_collection = true;
            } else {
                manage.step.from_spec = manage.mutation.from_schema.specs[result._meta.spec.name];
                manage.step.from_is_collection = true;
            }
            turtlegui.reload();
        }, handle_http_error);
    },

    reload_to_step: function() {
        turtlegui.ajax.get('/api' + this.step.to_path + '?page_size=1', (data) => {
            var result = JSON.parse(data);
            if (result._meta.is_collection) {
                manage.step.to_spec = manage.mutation.to_schema.specs[result._meta.spec.name];
                manage.step.to_is_collection = true;
            } else {
                manage.step.to_spec = manage.mutation.to_schema.specs[result._meta.spec.name];
                manage.step.to_is_collection = true;
            }
            turtlegui.reload();
        }, handle_http_error);
    },

    enabled_attrs: function() {
        return manage.step.from_spec != null ? {"disabled": true} : null;
    },

    create_diff: function() {
        var spec_names = this.all_spec_names();

        for (var i=0; i<spec_names.length; i++) {
            this.create_diff_for_spec(spec_names[i], spec_names[i]);
        }
    },

    get_step: function(spec_name) {
        for (var step of this.mutation.steps) {
            if (step.params.spec_name == spec_name && (!step.params.to_field_name) && (!step.params.field_name)) {
                return step;
            }
        }
        return null;
    },

    get_field_step: function(spec_name, field_name) {
        for (var step of this.mutation.steps) {
            if (step.params.spec_name == spec_name && field_name == step.params.field_name) {
                return step;
            }
        }
        return null;
    },

    get_from_field_step: function(spec_name, field_name) {
        for (var step of this.mutation.steps) {
            if (step.params.spec_name == spec_name && field_name == step.params.from_field_name) {
                return step;
            }
        }
        return null;
    },

    get_to_field_step: function(spec_name, field_name) {
        for (var step of this.mutation.steps) {
            if (step.params.spec_name == spec_name && field_name == step.params.to_field_name) {
                return step;
            }
        }
        return null;
    },

    find_rename_step_for: function(to_spec_name) {
        var rename_steps = this.mutation.steps.filter((step) => (step.params.to_spec_name == to_spec_name && step.action == 'rename_spec'));
        return rename_steps.length > 0 ? rename_steps[0] : null;
    },

    create_diff_for_spec: function(spec_name) {
        console.log('create diff for spec', spec_name);
        var step = this.get_step(spec_name);

        var diff = null;
        if (step) {
            diff = {'spec_name': spec_name, 'target_spec_name': step.params.to_spec_name || spec_name, 'fields': [], 'change': step.action};
        } else {
            if (!this.find_rename_step_for(spec_name)) {
                diff = {'spec_name': spec_name, 'target_spec_name': spec_name, 'fields': [], 'change': 'same'};
            }
        }

        if (diff) {
            var field_names = [];
            if (diff.change == 'rename_spec') {
                field_names = this.all_field_names(step.params.spec_name, step.params.to_spec_name);
            } else if (diff.change == 'same') {
                field_names = this.all_field_names(spec_name, spec_name);
            }
            for (var f=0; f<field_names.length; f++) {
                var field_name = field_names[f];

                var field_diff = {'field_name': field_name, 'change': 'same'};

                var field_step = this.get_field_step(spec_name, field_name);
                var from_field_step = this.get_from_field_step(spec_name, field_name);
                var to_field_step = this.get_to_field_step(spec_name, field_name);
                if (field_step) {
                    field_diff['change'] = field_step.action;
                    diff['fields'].push(field_diff);
                } else if (from_field_step && to_field_step) {
                } else if (from_field_step) {
                } else if (to_field_step) {
                    field_diff['change'] = to_field_step.action;
                    field_diff['target_spec_name'] = to_field_step.params.spec_name;
                    field_diff['from_field_name'] = to_field_step.params.from_field_name;
                    field_diff['to_field_name'] = to_field_step.params.to_field_name;
                    diff['fields'].push(field_diff);
                } else {
                    diff['fields'].push(field_diff);
                }
            }

            this.diff.push(diff);
        }
    },

    select_tab: function(tab) {
        this.tab = tab;
        turtlegui.reload();
    },

    all_spec_names: function() {
        var spec_names = Object.keys(this.mutation.to_schema.specs);
        spec_names = spec_names.concat(Object.keys(this.mutation.from_schema.specs));
        spec_names = spec_names.filter((v, i, a) => { return a.indexOf(v) === i; });
        spec_names.sort();
        spec_names.splice(spec_names.indexOf('root'), 1);
        spec_names.unshift('root');
        return spec_names;
    },

    all_field_names: function(spec_name, target_spec_name) {
        console.log('target_spec_name', target_spec_name);
        var field_names = Object.keys(this.mutation.to_schema.specs[target_spec_name].fields);
        field_names = field_names.concat(Object.keys(this.mutation.from_schema.specs[spec_name].fields));
        field_names = field_names.filter((v, i, a) => { return a.indexOf(v) === i; });
        field_names.sort();
        return field_names;
    },

    _all_field_names: function(step) {
        console.log('target_spec_name', step.spec_name);
        var field_names = Object.keys(this.mutation.from_schema.specs[step.spec_name].fields);
        if (step.params.to_spec_name) {
            field_names = field_names.concat(Object.keys(this.mutation.to_schema.specs[step.params.to_spec_name].fields));
        }
        field_names = field_names.filter((v, i, a) => { return a.indexOf(v) === i; });
        field_names.sort();
        return field_names;
    },


    spec_change: function(spec_name) {
        var from_spec = this.mutation.from_schema.specs[spec_name];
        var to_spec = this.mutation.to_schema.specs[spec_name];

        if (!from_spec) { return "create_spec"; }
        if (!to_spec) { return "delete_spec"; }
    },

    show_create_step: function() {
        this.step = {
            "action": "move"
        }
        turtlegui.reload();
    },

    hide_create_step: function() {
        this.step = null;
        turtlegui.reload();
    },

    perform_create_step: function() {
        turtlegui.ajax.post('/admin/api/mutations/' + this.mutation.id + '/steps', this.step, (data) => {
            manage.load();
            manage.hide_create_step();
        });
    },

    perform_mutation: function() {
        if (confirm("Perform Mutation?")) {
            turtlegui.ajax.patch('/admin/api/mutations/' + this.mutation.id, {"promote": true}, (data) => {
                window.location = '/admin';
            });
        }
    }
}


var group_diff = {
    diff: [],

    create_diff: function() {
        for (var group_name of this.all_group_names()) {
            if ((group_name in manage.mutation.from_schema.groups) && !(group_name in manage.mutation.to_schema.groups)) {
                // removed
                this.diff.push({
                    change: "deleted",
                    group_name: group_name
                });
            } else if (!(group_name in manage.mutation.from_schema.groups) && (group_name in manage.mutation.to_schema.groups)) {
                // created
                this.diff.push({
                    change: "created",
                    group_name: group_name
                });
            } else {
                // same, check fields
                var from_group = manage.mutation.from_schema.groups[group_name];
                var to_group = manage.mutation.to_schema.groups[group_name];

                from_grant_names = from_group.grants.map(a => a.grant_type + ':' + a.url);
                to_grant_names = to_group.grants.map(a => a.grant_type + ':' + a.url);

                var grant_diff = [];
                var grant_names = [];
                grant_names = grant_names.concat(from_grant_names);
                grant_names = grant_names.concat(to_grant_names);
                grant_names = grant_names.filter((v, i, a) => { return a.indexOf(v) === i; });
                grant_names.sort();

                for (var grant_name of grant_names) {
                    if (from_grant_names.indexOf(grant_name) != -1 && to_grant_names.indexOf(grant_name) == -1) {
                        grant_diff.push({"change": "removed", "name": grant_name});
                    } else if (from_grant_names.indexOf(grant_name) == -1 && to_grant_names.indexOf(grant_name) != -1) {
                        grant_diff.push({"change": "created", "name": grant_name});
                    } else {
                        grant_diff.push({"change": "same", "name": grant_name});
                    }
                }

                this.diff.push({
                    change: "same",
                    group_name: group_name,
                    grants: grant_diff
                });
            }
        }
    },

    all_group_names: function() {
        var group_names = Object.keys(manage.mutation.to_schema.groups);
        group_names = group_names.concat(Object.keys(manage.mutation.from_schema.groups));
        group_names = group_names.filter((v, i, a) => { return a.indexOf(v) === i; });
        group_names.sort();
        return group_names;
    }
}


var change_spec_delete_popup = {
    action: "rename",
    diff: null,
    target_spec_name: null,
    created_specs: [],

    open: function(diff) {
        this.diff = diff;
        this.target_spec_name = null;
        this.created_specs = this.list_created_specs();
        turtlegui.reload();
    },

    close: function() {
        this.diff = null;
        turtlegui.reload();
    },

    list_created_specs: function() {
        var specs = [];
        for (var diff of manage.diff) {
            if (diff.change == 'create_spec') {
                specs.push(diff.spec_name);
            }
        }
        return specs;
    },

    find_diff: function(spec_name) {
        for (diff of manage.diff) {
            if (diff.spec_name == this.target_spec_name) {
                return diff;
            }
        }
        return null;
    },

    perform_change: function() {
        var data = {
            "action": "rename_spec",
            "from_spec_name": this.diff.spec_name,
            "to_spec_name": this.target_spec_name
        }
        turtlegui.ajax.post('/admin/api/mutations/' + mutation_id + "/steps", data, (response) => {
            change_spec_delete_popup.diff = null;
            manage.load();
        });
    },

    remove_diff: function(diff) {
        manage.diff.splice(manage.diff.indexOf(diff), 1);
    },

    cancel_rename: function(diff) {
        if (confirm("Cancel rename for " + diff.spec_name + "?")) {
            turtlegui.ajax.delete('/admin/api/mutations/' + mutation_id + "/steps/" + diff.spec_name, (data) => {
                change_spec_delete_popup.diff = null;
                manage.load();
            });
        }
    }
}


var change_field_delete_popup = {
    action: "rename",
    diff: null,
    field: null,
    target_field_name: null,
    created_fields: [],

    open: function(diff, field) {
        this.diff = diff;
        this.field = field;
        this.target_field_name = null;
        this.created_fields = this.list_created_fields();
        turtlegui.reload();
    },

    close: function() {
        this.diff = null;
        this.field = null;
        turtlegui.reload();
    },

    list_created_fields: function() {
        var fields = [];
        for (var field of this.diff.fields) {
            if (field.change == 'create_field') {
                fields.push(field.field_name);
            }
        }
        return fields;
    },

    find_field: function(field_name) {
        for (field of this.diff.fields) {
            if (field.field_name == this.target_field_name) {
                return field;
            }
        }
        return null;
    },

    perform_change: function() {
        var data = {
            "action": "rename_field",
            "spec_name": this.diff.spec_name,
            "from_field_name": this.field.field_name,
            "to_field_name": this.target_field_name
        }
        turtlegui.ajax.post('/admin/api/mutations/' + mutation_id + "/steps", data, (response) => {
            change_field_delete_popup.diff = null;
            manage.load();
        });
    },

    remove_field: function(field) {
        this.diff.fields.splice(this.diff.fields.indexOf(field), 1);
    },

    cancel_rename: function(diff, field) {
        if (confirm("Cancel rename for " + field.from_field_name + "?")) {
            turtlegui.ajax.delete('/admin/api/mutations/' + mutation_id + "/steps/" + diff.spec_name + "/" + field.from_field_name, (data) => {
                change_field_delete_popup.diff = null;
                manage.load();
            });

        }
    },

}



var login = new Login();

document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    manage.load();
});

