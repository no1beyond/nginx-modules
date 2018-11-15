# nodeid module
## Desc
Specifies a load balancing method for a server group where the client-server mapping is based on the nodeid value. 
The backup load balancing method is consistent hash.

## Example Configuration

```
upstream backend {
    nodeid $arg_nodeid $remote_addr;
    nodeid_server 127.0.0.1:8080 nodeid=1;
    server 127.0.0.1:8080 weight=1;
}
```

## Directives

### nodeid

```
Syntax:	nodeid nodeid_value [consistent_hash_key];
Default:	—
Context:	upstream
```
sets the nodeid and consistent hash key of the request.

### nodeid_server

```
Syntax:	nodeid_server address nodeid=nodeid_value;
Default:	—
Context:	upstream
```
sets the nodeid of the server.
