
/** TurtleGUI - a javascript GUI library. Shared using MIT license (see LICENSE file)
*/

$.ajaxSetup ({
    // Disable caching of AJAX responses
    cache: false
});

var turtlegui = {};

turtlegui.root_element = $(document);


turtlegui._get_safe_value = function(elem, datasrc) {
    var gres = elem.attr(datasrc);
    if (!gres) {
        throw "No " + datasrc + " field on element " + elem.attr('id');
    }
    return turtlegui._relative_eval(elem, gres);
}


turtlegui.getstack = function(elm) {
    return elm.parent().length > 0 && !elm.parent().is('body') ? turtlegui.getstack(elm.parent()).concat([elm]) : [elm];
}


turtlegui._relative_eval = function(elem, gres) {
    try {
    var rel = elem.data('data-rel');
    }catch(e) {
        console.log("error ");
    }
    var switcharoo = {};
    for (var key in rel) {
        if (key in window) {
            switcharoo[key] = window[key];
        }
        window[key] = rel[key];
    }
    try {
        result = eval(gres);
    } catch(e) {
        try {
            console.log("Error at:");
            var stack = turtlegui.getstack(elem);
            for (var i in stack) {
                var elm = $(stack[i]);
                var desc = elm.prop('nodeName') + (elm.attr('id')?'#'+elm.attr('id'):"") + "[" + elm.index() + "]";
                console.log("  ".repeat(i) + desc);
            }
            console.log("Error evaluating " + gres + " on elem : " + e);
        } catch (noconsole) {
            throw e;
        }
    } finally {
        for (var key in switcharoo) {
            window[key] = switcharoo[key];
        }
    }
    return result;
}


turtlegui.load_snippet = function(elem, url, rel_data) {
    elem.load(url, function(response, status, xhr) {
        if (status == 'success') {
            turtlegui.reload(elem, rel_data);
        } else {
            console.log("Could not load html snippet: " + url + " - " + xhr.status + " " + xhr.statusText);
        }
    });
}


turtlegui.reload = function(elem, rel_data) {
    var path = turtlegui.getstack($(document.activeElement));
    var path_indices = [];
    for (var i=0; i<path.length; i++) {
        path_indices[i] = path[i].index();
    }
    turtlegui._reload(elem, rel_data);
    var current_elem = $('body');
    for (var i=0; i<path_indices.length; i++) {
        current_elem = $(current_elem.children()[path_indices[i]]);
    }
    current_elem.focus();
}


turtlegui._reload = function(elem, rel_data) {
    if (!rel_data) rel_data = {};

    if (!elem) {
        elem = turtlegui.root_element;
    }
    if (rel_data) {
        elem.data('data-rel', rel_data);
    }
    if (elem.attr('data-gui-text')) {
        value = turtlegui._get_safe_value(elem, 'data-gui-text');
        elem.text(value);
    }
    if (elem.attr('data-gui-attr') && elem.attr('data-gui-attrval')) {
        var key = turtlegui._get_safe_value(elem, 'data-gui-attr');
        var value = turtlegui._get_safe_value(elem, 'data-gui-attrval');
        elem.attr(key, value);
    }
    if (elem.attr('data-gui-class')) {
        var value = turtlegui._get_safe_value(elem, 'data-gui-class');
        elem.addClass(value);
    }
    if (elem.attr('data-gui-id')) {
        var value = turtlegui._get_safe_value(elem, 'data-gui-id');
        $(elem).attr('id', value);
    }
    if (elem.attr('data-gui-show')) {
        var value = turtlegui._get_safe_value(elem, 'data-gui-show');
        if (value) {
            elem.show();
        } else {
            elem.hide();
            if (elem.attr('data-gui-id')) {
                $(elem).attr('id', null);
            }
            return;
        }
    }
    if (elem.attr('data-gui-click')) {
        elem.unbind('click').click(function() {
            return turtlegui._get_safe_value(elem, 'data-gui-click');
        });
    }
    if (elem.attr('data-gui-list')) {
        var list = turtlegui._get_safe_value(elem, 'data-gui-list');
        if ($.isPlainObject(list)) {
            var rel_item = elem.attr('data-gui-item');
            var rel_key = elem.attr('data-gui-key');
            var rel_order = elem.attr('data-gui-ordering');
            if (elem.attr('data-gui-reversed')) {
                var rel_reverse = turtlegui._get_safe_value(elem, 'data-gui-reversed');
            } else {
                var rel_reverse = false;
            }

            var obj = [];
            for (var key in list) {
                if (rel_order) {
                    var rel = elem.data('data-rel');
                    rel[rel_item] = list[key];
                    var order_key = turtlegui._relative_eval(elem, rel_order);
                    obj[obj.length] = [order_key, key, list[key]];
                } else {
                    obj[obj.length] = [key, key, list[key]];
                }
            }
            obj.sort();
            if (rel_reverse) {
                obj.reverse();
            }

            var orig_elems = elem.children();
            var first_elem = $(orig_elems[0]);
            first_elem.hide();
            var new_elems = [];
            for (var i in obj) {
                var new_elem = $(first_elem).clone();
                new_elems[new_elems.length] = new_elem;
            }
            for (var i in new_elems) {
                var obj_item = obj[i];
                var obj_key = obj_item[1];
                var item = obj_item[2];

                var new_elem = new_elems[i];
                elem.append(new_elem);
                new_elem.show();
                var rel_data = jQuery.extend({}, rel_data);
                rel_data[rel_item] = item;
                if (rel_key != null) {
                    rel_data[rel_key] = obj_key;
                }
                turtlegui._reload(new_elem, rel_data);
            }
            orig_elems.remove();
            elem.prepend(first_elem);
        } else {
            var rel_item = elem.attr('data-gui-item');
            var rel_key = elem.attr('data-gui-key');
            var orig_elems = elem.children();
            var first_elem = $(orig_elems[0]);
            first_elem.hide();
            var new_elems = [];
            for (var i in list) {
                var new_elem = $(first_elem).clone();
                new_elems[new_elems.length] = new_elem;
            }
            for (var i in new_elems) {
                var item = list[i];
                var new_elem = new_elems[i];
                elem.append(new_elem);
                new_elem.show();
                var rel_data = jQuery.extend({}, rel_data);
                rel_data[rel_item] = item;
                if (rel_key != null) {
                    rel_data[rel_key] = i;
                }
                turtlegui._reload(new_elem, rel_data);
            }
            orig_elems.remove();
            elem.prepend(first_elem);
        }
    }
    else if (elem.attr('data-gui-tree')) {
        var tree = turtlegui._get_safe_value(elem, 'data-gui-tree');
        var rel_item = elem.attr('data-gui-nodeitem');

        var orig_elems = elem.children();
        var first_elem = $(orig_elems[0]);
        first_elem.hide();
        var rel_data = jQuery.extend({}, rel_data);
        rel_data[rel_item] = tree;
        rel_data['_last_tree_elem'] = first_elem;
        rel_data['_last_tree_item'] = rel_item;
         
        var new_elem = $(first_elem).clone();
        elem.append(new_elem);
        new_elem.show();
        turtlegui._reload(new_elem, rel_data);

        orig_elems.remove();
        elem.prepend(first_elem);
    }
    else if (elem.attr('data-gui-node')) {
        var node = turtlegui._get_safe_value(elem, 'data-gui-node');

        var orig_elems = elem.children();
        var first_elem = rel_data['_last_tree_elem'];
        first_elem.hide();
        var rel_data = jQuery.extend({}, rel_data);
        rel_data[rel_item] = node;
        rel_data[rel_data['_last_tree_item']] = node;
 
        var new_elem = $(first_elem).clone();
        elem.append(new_elem);
        new_elem.show();
        turtlegui._reload(new_elem, rel_data);

        orig_elems.remove();
        elem.prepend(first_elem);
    }
    else if (elem.attr('data-gui-include') && !elem.attr('data-gui-included')) {
        elem.attr('data-gui-included', true);
        var url = turtlegui._get_safe_value(elem, 'data-gui-include');
        turtlegui.load_snippet(elem, url, rel_data);
    }
    else {
        elem.children().each(function() {
            turtlegui._reload($(this), rel_data);
        });
    }

    if (elem.attr('data-gui-val') || elem.attr('data-gui-change')) {
        // This overwrites any manually bound change events
        $(elem).unbind('change');
    }

    if (elem.attr('data-gui-val')) {
        var value = turtlegui._get_safe_value(elem, 'data-gui-val');
        if ($(elem).is(':checkbox')) {
            $(elem).prop('checked', value);
        } else {
            if (elem.attr('data-gui-format-func')) {
                if (value != null) {
                    value = turtlegui._relative_eval(elem, elem.attr('data-gui-format-func'))(value);
                }
            }
            $(elem).val(value);
        }
        $(elem).change(function () {
            var gres = elem.attr('data-gui-val');
            if ($(elem).is(':checkbox')) {
                turtlegui._relative_eval(elem, gres + " = " + $(elem).prop('checked'));
            } else if (elem.attr('data-gui-parse-func')) {
                var elem_val = $(elem).val();
                if (elem_val != null) {
                    // Complex objects don't parse so well with eval, so putting the result into the data-rel structure
                    var __formatted = turtlegui._relative_eval(elem, elem.attr('data-gui-parse-func'))(elem_val);
                    var rel = elem.data('data-rel');
                    rel['__formatted'] = __formatted;
                    turtlegui._relative_eval(elem, gres + " = __formatted");
                } else {
                    turtlegui._relative_eval(elem, gres + " = null");
                }
            } else {
                var elemval = $(elem).val();
                elemval = elemval.replace(/(?:\r\n|\r|\n)/g, '\\n');
                elemval = elemval.replace(/'/g, '\\\'');
                turtlegui._relative_eval(elem, gres + " = '" + elemval + "'");
            }
        });
    }
    if (elem.attr('data-gui-change')) {
        $(elem).change(function (){
            turtlegui._get_safe_value(elem, 'data-gui-change');
        });
    }
}
