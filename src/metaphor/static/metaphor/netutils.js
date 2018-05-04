var net = {};

net.loading = false;

net.perform_get = function(url, callback, onerror)
{
    net.loading = true;
    $.ajax({
        'url': url,
        'success': function(data) {
            net.loading = false;
            if (callback != null)
                callback(data);
        },
        'error': function(jqXHR, errorText) {
            net.loading = false;
            console.log("Error calling "+url+" : "+errorText);
            if (onerror)
                onerror(errorText, jqXHR);
        },
        'contentType': "application/json",
        'headers': {
            'accept': "application/json; charset=utf-8"
        },
        'type': 'GET'
    });
}


net.perform_post = function(url, data, callback, onerror, method)
{
    $.ajax({
        'url': url,
        'data': JSON.stringify(data),
        'success': function(data) {
            net.loading = false;
            if (callback != null)
                callback(data);
        },
        'contentType': "application/json",
        'dataType': 'json',
        'error': function(jqXHR, errorText) {
            net.loading = false;
            console.log("Error calling "+url+" : "+errorText);
            if (onerror)
                onerror(errorText, jqXHR);
        },
        'headers': {
            'accept': "application/json; charset=utf-8"
        },
        'type': method ? method : 'POST'
    });
}

net.perform_patch = function(url, data, callback, onerror)
{
    net.perform_post(url, data, callback, onerror, 'PATCH')
}

net.perform_delete = function(url, callback, onerror)
{
    $.ajax({
        'url': url,
        'success': function(data) {
            net.loading = false;
            if (callback != null)
                callback(data);
        },
        'contentType': "application/json",
        'dataType': 'json',
        'error': function(jqXHR, errorText) {
            net.loading = false;
            console.log("Error calling "+url+" : "+errorText);
            if (onerror)
                onerror(errorText, jqXHR);
        },
        'headers': {
            'accept': "application/json; charset=utf-8"
        },
        'type': 'DELETE'
    });
}
