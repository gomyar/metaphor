
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
            var diff = {'spec_name': spec_names[i], 'fields': [], 'change': 'same'};
            this.diff.push(diff);
            var from_spec = this.mutation.from_schema.specs[spec_names[i]];
            var to_spec = this.mutation.to_schema.specs[spec_names[i]];
            if (!from_spec) { diff['change'] = 'spec_created'; continue; }
            if (!to_spec) { diff['change'] = 'spec_deleted'; continue; }

            var field_names = this.all_field_names(spec_names[i]);
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

    all_field_names: function(spec_name) {
        var field_names = Object.keys(this.mutation.to_schema.specs[spec_name].fields);
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


var login = new Login();

document.addEventListener("DOMContentLoaded", function(){
    // call turtlegui.reload() when the page loads
    manage.load();
});

