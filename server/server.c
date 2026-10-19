#include "protocol.h"
#include "net.h"

#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DEFAULT_PORT 50007   /* 临时端口，报告前改成学号后四位 */

typedef struct server server_t;

typedef struct client_node {
    server_t *server;
    int id;
    int sockfd;
    struct sockaddr_in addr;
    int connected;

    pthread_t thread;
    pthread_mutex_t send_lock;
    struct client_node *next;
} client_node_t;

struct server {
    int listen_fd;
    pthread_mutex_t list_lock;
    client_node_t *clients;
    int next_id;
};

static volatile sig_atomic_t g_stop = 0;

static void install_signal_handlers(void);
static void on_signal(int sig);

static void server_init(server_t *s);
static void server_cleanup(server_t *s);
static int server_run(server_t *s, int port);
static void *client_thread(void *arg);

static client_node_t *client_list_add(server_t *s, int fd,
                                      const struct sockaddr_in *addr);
static void client_list_remove(server_t *s, client_node_t *client);
static client_node_t *client_list_find(server_t *s, int id);
static void server_shutdown_clients(server_t *s);

static int send_response(client_node_t *client, uint8_t type, uint8_t status,
                         uint32_t seq, const void *body, uint32_t body_len);
static int send_indication(client_node_t *target, uint32_t seq,
                           const void *body, uint32_t body_len);

static void handle_request(client_node_t *client, const proto_message_t *msg);
static void handle_time(client_node_t *client, const proto_message_t *req);
static void handle_name(client_node_t *client, const proto_message_t *req);
static void handle_list(client_node_t *client, const proto_message_t *req);
static void handle_message(client_node_t *client, const proto_message_t *req);

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

static void install_signal_handlers(void)
{
    /* TODO: 注册 SIGINT/SIGTERM，并忽略 SIGPIPE。 */
}

static void server_init(server_t *s)
{
    memset(s, 0, sizeof(*s));
    s->listen_fd = -1;
    s->next_id = 1;
    pthread_mutex_init(&s->list_lock, NULL);
}

static void server_cleanup(server_t *s)
{
    /* TODO: 关闭监听 socket，通知并等待所有客户端线程退出。 */
    (void)s;
}

static int server_run(server_t *s, int port)
{
    /*
     * TODO:
     * 1. socket() + bind() + listen()。
     * 2. 主线程循环 accept()。
     * 3. 每个新连接加入客户端列表，并创建一个 client_thread 线程。
     */
    (void)s;
    (void)port;
    printf("TODO: 服务端监听和 accept 循环待实现。\n");
    return -1;
}

static void *client_thread(void *arg)
{
    /* TODO: 循环 proto_recv_packet()，调用 handle_request()。 */
    (void)arg;
    return NULL;
}

static client_node_t *client_list_add(server_t *s, int fd,
                                      const struct sockaddr_in *addr)
{
    /* TODO: 分配节点，记录编号、socket、IP、端口和连接状态。 */
    (void)s;
    (void)fd;
    (void)addr;
    return NULL;
}

static void client_list_remove(server_t *s, client_node_t *client)
{
    /* TODO: 从链表删除并释放客户端节点。 */
    (void)s;
    (void)client;
}

static client_node_t *client_list_find(server_t *s, int id)
{
    /* TODO: 按客户端编号查找已连接节点。 */
    (void)s;
    (void)id;
    return NULL;
}

static void server_shutdown_clients(server_t *s)
{
    /* TODO: 通知、join 并释放所有客户端线程。 */
    (void)s;
}

static int send_response(client_node_t *client, uint8_t type, uint8_t status,
                         uint32_t seq, const void *body, uint32_t body_len)
{
    /* TODO: 调用 proto_send_packet() 发送响应，注意加锁。 */
    (void)client;
    (void)type;
    (void)status;
    (void)seq;
    (void)body;
    (void)body_len;
    return -1;
}

static int send_indication(client_node_t *target, uint32_t seq,
                           const void *body, uint32_t body_len)
{
    /* TODO: 调用 proto_send_packet() 发送 PROTO_INDICATION，注意加锁。 */
    (void)target;
    (void)seq;
    (void)body;
    (void)body_len;
    return -1;
}

static void handle_request(client_node_t *client, const proto_message_t *msg)
{
    /* TODO: 按 msg->type 分发到下面四个处理函数。 */
    (void)client;
    (void)msg;
}

static void handle_time(client_node_t *client, const proto_message_t *req)
{
    /* TODO: time() + strftime()，发送时间响应。 */
    (void)client;
    (void)req;
}

static void handle_name(client_node_t *client, const proto_message_t *req)
{
    /* TODO: gethostname()，发送名字响应。 */
    (void)client;
    (void)req;
}

static void handle_list(client_node_t *client, const proto_message_t *req)
{
    /* TODO: 遍历客户端列表，组装 count + 每条客户端记录。 */
    (void)client;
    (void)req;
}

static void handle_message(client_node_t *client, const proto_message_t *req)
{
    /* TODO: 解析目标编号和文本，发送指示包，再给源客户端发响应。 */
    (void)client;
    (void)req;
}

int main(int argc, char **argv)
{
    server_t server;
    int port = DEFAULT_PORT;

    if (argc > 1) {
        port = atoi(argv[1]);
    }
    if (port < 1024 || port > 65535) {
        fprintf(stderr, "端口必须在 1024-65535 之间。\n");
        return 1;
    }

    install_signal_handlers();
    server_init(&server);
    if (server_run(&server, port) < 0) {
        server_cleanup(&server);
        return 1;
    }
    server_cleanup(&server);
    return 0;
}
