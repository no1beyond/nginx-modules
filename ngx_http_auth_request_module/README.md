# auth request module
## Desc

Add read body and return body support.
Usage:
    Replace the file in nginx path.

## Example Configuration

```
upstream auth-daemon {
    server 127.0.0.1:5020;
}

upstream business-daemon {
    server 127.0.0.1:5000;
}

location = /auth-proxy {
    internal;
    proxy_pass http://auth-daemon;
    proxy_set_header X-Target $request_uri;
}

location  /business_api {
    auth_request /auth-proxy read_body return_body; 
    proxy_pass http://business-daemon;
}

```

## Directives

### auth_request

```
Syntax:	auth_request path [read_body] [return_body];
Default:	—
Context:	location
```
read_body: read http_request body for auth request backend.
return_body: return auth backend returned body to http response.


