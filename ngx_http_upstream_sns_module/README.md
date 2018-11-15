# sns module
## Desc

Add sns support. Based on upsync module of Weibo(https://github.com/weibocom/nginx-upsync-module).

## Example Configuration

```
    upstream testserver {
        sns smartsns.service.local:10010/api/internal/nameServer/snapMetas name=testServer timeout=6m interval=5s type=sns strong_dependency=on;
        sns_dump_path config/servers_vodServer.conf;
        sns_lb hash_ketama;
    }
    
    server {
        listen       80 default deferred;
        server_name  localhost;
    
    	location /upstream_list {
    		upstream_show;
    	}
    }


```





