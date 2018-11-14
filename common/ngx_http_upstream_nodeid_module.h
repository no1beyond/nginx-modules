
#ifndef _NGX_HTTP_UPSTREAM_NODEID_MODULE_H_INCLUDED_
#define _NGX_HTTP_UPSTREAM_NODEID_MODULE_H_INCLUDED_
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

ngx_int_t ngx_http_upstream_nodeid_dump_server(ngx_http_upstream_srv_conf_t *us, ngx_buf_t *b);
ngx_int_t ngx_http_upstream_nodeid_add_server(ngx_http_upstream_srv_conf_t *us, ngx_uint_t nodeid, u_char *server);
ngx_int_t ngx_http_upstream_nodeid_del_server(ngx_http_upstream_srv_conf_t *us, u_char *server);


#endif /* _NGX_HTTP_UPSTREAM_NODEID_MODULE_H_INCLUDED_ */

