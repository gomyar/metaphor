
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
            manage.mutation.to_schema.specs['root'] = {"fields": manage.mutation.to_schema.root};
            manage.mutation.from_schema.specs['root'] = {"fields": manage.mutation.from_schema.root};
            manage.spec_names = manage.all_spec_names();
            manage.create_diff();
            turtlegui.reload();
        });
    },

    reload_step: function() {
        turtlegui.ajax.post('/admin/api/schemas/' + this.mutation.from_schema.id + '/calcs', {'calc_str': this.step.target_calc}, (data) => {
            var meta = JSON.parse(data);
            manage.step.target_spec = manage.mutation.from_schema.specs[meta.meta.spec_name];
            manage.step.target_is_collection = meta.meta.is_collection;
            manage.step.target_field_name = manage.target_collection_fields() ? manage.target_collection_fields()[0] : null;
            turtlegui.reload();
        }, handle_http_error);
    },

    enabled_attrs: function() {
        return manage.step.target_spec != null ? {"disabled": true} : null;
    },

    target_collection_fields: function() {
        var field_names = [];
        for (var name in manage.step.target_spec.fields) {
            if (manage.step.target_spec.fields[name].type == 'collection') {
                field_names.push(name);
            }
        }
        return field_names;
    },

    create_diff: function() {
        var spec_names = this.all_spec_names();

        for (var i=0; i<spec_names.length; i++) {
            this.create_diff_for_spec(spec_names[i], spec_names[i]);
        }
    },

    create_diff_for_spec: function(spec_name, target_spec_name) {
        var diff = {'spec_name': spec_name, 'target_spec_name': target_spec_name, 'fields': [], 'change': 'same'};
        this.diff.push(diff);
        var from_spec = this.mutation.from_schema.specs[spec_name];
        var to_spec = this.mutation.to_schema.specs[target_spec_name];
        if (spec_name != target_spec_name) {
            diff['change'] = 'spec_renamed';
        } else {
            if (!from_spec) { diff['change'] = 'spec_created'; return; }
            if (!to_spec) { diff['change'] = 'spec_deleted'; return; }
        }

        var field_names = this.all_field_names(spec_name, target_spec_name);
        for (var f=0; f<field_names.length; f++) {
            var field_name = field_names[f];
            var from_field = from_spec.fields[field_name];
            var to_field = to_spec.fields[field_name];
            var field_diff = {'field_name': field_name};
            diff['fields'].push(field_diff);
            if (!from_field) { field_diff['change'] = 'field_created'; continue; }
            if (!to_field) { field_diff['change'] = 'field_deleted'; continue; }
            if (from_field.type != to_field.type) { field_diff['change'] = 'field_type_changed'; continue; }
            if (from_field.calc_str != null && from_field.calc_str != to_field.calc_str) { field_diff['change'] = 'field_calc_changed'; continue; }
            field_diff['change'] = 'same';
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
        var field_names = Object.keys(this.mutation.to_schema.specs[target_spec_name].fields);
        field_names = field_names.concat(Object.keys(this.mutation.from_schema.specs[spec_name].fields));
        field_names = field_names.filter((v, i, a) => { return a.indexOf(v) === i; });
        field_names.sort();
        return field_names;
    },

    spec_change: function(spec_name) {
        var from_spec = this.mutation.from_schema.specs[spec_name];
        var to_spec = this.mutation.to_schema.specs[spec_name];

        if (!from_spec) { return "spec_created"; }
        if (!to_spec) { return "spec_deleted"; }
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
            manage.hide_create_step();
        });
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
            if (diff.change == 'spec_created') {
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
        var target_diff = this.find_diff(this.target_spec_name);
        this.remove_diff(this.diff);
        this.remove_diff(target_diff);
        manage.create_diff_for_spec(this.diff.spec_name, this.target_spec_name);
        manage.diff.sort((lhs, rhs) => (lhs.spec_name > rhs.spec_name) ? 1 : ((rhs.spec_name > lhs.spec_name) ? -1 : 0));
        this.diff = null;

        turtlegui.reload();
    },

    remove_diff: function(diff) {
        manage.diff.splice(manage.diff.indexOf(diff), 1);
    },

    cancel_rename: function(diff) {
        if (confirm("Cancel rename for " + diff.spec_name + "?")) {
            this.remove_diff(diff);
            manage.create_diff_for_spec(diff.spec_name, diff.spec_name);
            manage.create_diff_for_spec(diff.target_spec_name, diff.target_spec_name);
            manage.diff.sort((lhs, rhs) => (lhs.spec_name > rhs.spec_name) ? 1 : ((rhs.spec_name > lhs.spec_name) ? -1 : 0));
            turtlegui.reload();
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
        this.field = field;
        turtlegui.reload();
    },

    list_created_fields: function() {
        var fields = [];
        for (var field of this.diff.fields) {
            if (field.change == 'field_created') {
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
        var target_field = this.find_field(this.target_field_name);
        this.remove_field(this.field);
        this.remove_field(target_field);
        this.diff.fields.push({"field_name": this.field.field_name, "change": "field_renamed", "target_field_name": target_field.field_name});
        this.diff.fields.sort((lhs, rhs) => (lhs.field_name > rhs.field_name) ? 1 : ((rhs.field_name > lhs.field_name) ? -1 : 0));
        this.diff = null;

        turtlegui.reload();
    },

    remove_field: function(field) {
        this.diff.fields.splice(this.diff.fields.indexOf(field), 1);
    },

    cancel_rename: function(diff, field) {
        if (confirm("Cancel rename for " + field.field_name + "?")) {
            diff.fields.splice(diff.fields.indexOf(field), 1);
            diff.fields.push({"field_name": field.field_name, "change": "field_deleted"});
            diff.fields.push({"field_name": field.target_field_name, "change": "field_created"});
            diff.fields.sort((lhs, rhs) => (lhs.field_name > rhs.field_name) ? 1 : ((rhs.field_name > lhs.field_name) ? -1 : 0));
            turtlegui.reload();
        }
    }
}



var login = new Login();

document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    manage.load();
});

