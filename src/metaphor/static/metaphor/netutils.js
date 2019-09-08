var net = {};

net.loading = false;

net._create_request = function(url, callback, onerror) {
    var xmlhttp = new XMLHttpRequest();
    xmlhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            net.loading = false;
            if (callback != null)
                callback(JSON.parse(this.responseText));
        } else if (this.readyState == 4 && this.status != 200) {
            net.loading = false;
            console.log("Error calling "+url+" : "+this.responseText);
            if (onerror)        
                onerror(this.responseText);
        }
    };
    return xmlhttp;
}

net.perform_get = function(url, callback, onerror)
{
    net.loading = true;
    var xmlhttp = net._create_request(url, callback, onerror);
    xmlhttp.open("GET", url, true);
    xmlhttp.send();
}


net.perform_post = function(url, data, callback, onerror, method)
{
    net.loading = true;
    var xmlhttp = net._create_request(url, callback, onerror);
    xmlhttp.open("POST", url, true);
    xmlhttp.setRequestHeader('Content-Type', 'application/json');
    xmlhttp.send(JSON.stringify(data));
}

net.perform_patch = function(url, data, callback, onerror)
{
    net.loading = true;
    var xmlhttp = net._create_request(url, callback, onerror);
    xmlhttp.open("PATCH", url, true);
    xmlhttp.setRequestHeader('Content-Type', 'application/json');
    xmlhttp.send(JSON.stringify(data));
}

net.perform_delete = function(url, callback, onerror)
{
    net.loading = true;
    var xmlhttp = net._create_request(url, callback, onerror);
    xmlhttp.open("DELETE", url, true);
    xmlhttp.send();
}
