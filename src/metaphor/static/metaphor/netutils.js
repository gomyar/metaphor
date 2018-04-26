var net = {};

net.perform_get = function(url, callback, onerror)
{
    $.ajax({
        'url': url,
        'success': function(data) {
            if (callback != null)
                callback(data);
        },
        'error': function(jqXHR, errorText) {
            console.log("Error calling "+url+" : "+errorText);
            if (onerror)
                onerror(errorText, jqXHR);
        },
        'type': 'GET'
    });
}


net.perform_post = function(url, data, callback, onerror)
{
    $.ajax({
        'url': url,
        'data': JSON.stringify(data),
        'success': function(data) {
            if (callback != null)
                callback(data);
        },
        'contentType': "application/json",
        'dataType': 'json',
        'error': function(jqXHR, errorText) {
            console.log("Error calling "+url+" : "+errorText);
            if (onerror)
                onerror(errorText, jqXHR);
        },
        'type': 'POST'
    });
}


net.perform_delete = function(url, callback, onerror)
{
    $.ajax({
        'url': url,
        'success': function(data) {
            if (callback != null)
                callback(data);
        },
        'contentType': "application/json",
        'dataType': 'json',
        'error': function(jqXHR, errorText) {
            console.log("Error calling "+url+" : "+errorText);
            if (onerror)
                onerror(errorText, jqXHR);
        },
        'type': 'DELETE'
    });
}
