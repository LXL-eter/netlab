#include "protocol.h"
#include "net.h"

/* 框架版：监听、accept 和客户端列表骨架已给出，TODO 处补业务逻辑。 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_PORT 50007   /* 临时端口，报告前改成学号后四位 */

typedef struct server server_t;

typedef struct client_node {
    server_t *server;
    int id;
    int sockfd;
    struct sockaddr_in addr;
    volatile int connected;

    pthread_t thread;
    int thread_started;
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
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);
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
    if (s->listen_fd >= 0) {
        close(s->listen_fd);
        s->listen_fd = -1;
    }
    server_shutdown_clients(s);
    pthread_mutex_destroy(&s->list_lock);
}

static client_node_t *client_list_add(server_t *s, int fd,
                                      const struct sockaddr_in *addr)
{
    client_node_t *node = (client_node_t *)calloc(1, sizeof(*node));

    if (node == NULL) {
        return NULL;
    }

    node->server = s;
    node->sockfd = fd;
    node->addr = *addr;
    node->connected = 1;
    if (pthread_mutex_init(&node->send_lock, NULL) != 0) {
        free(node);
        return NULL;
    }

    pthread_mutex_lock(&s->list_lock);
    node->id = s->next_id++;
    node->next = s->clients;
    s->clients = node;
    pthread_mutex_unlock(&s->list_lock);
    return node;
}

static void client_list_remove(server_t *s, client_node_t *client)
{
    client_node_t **pp;

    pthread_mutex_lock(&s->list_lock);
    pp = &s->clients;
    while (*pp != NULL && *pp != client) {
        pp = &(*pp)->next;
    }
    if (*pp == client) {
        *pp = client->next;
    }
    pthread_mutex_unlock(&s->list_lock);

    if (client->sockfd >= 0) {
        close(client->sockfd);
    }
    pthread_mutex_destroy(&client->send_lock);
    free(client);
}

static client_node_t *client_list_find(server_t *s, int id)
{
    client_node_t *node;

    pthread_mutex_lock(&s->list_lock);
    for (node = s->clients; node != NULL; node = node->next) {
        if (node->id == id && node->connected) {
            break;
        }
    }
    pthread_mutex_unlock(&s->list_lock);
    return node;
}

static int send_response(client_node_t *client, uint8_t type, uint8_t status,
                         uint32_t seq, const void *body, uint32_t body_len)
{
    int rc;

    if (client == NULL) {
        return -1;
    }

    pthread_mutex_lock(&client->send_lock);
    if (!client->connected || client->sockfd < 0) {
        pthread_mutex_unlock(&client->send_lock);
        return -1;
    }
    rc = proto_send_packet(client->sockfd, PROTO_RESPONSE, type, status,
                           seq, body, body_len);
    pthread_mutex_unlock(&client->send_lock);
    return rc;
}

static int send_indication(client_node_t *target, uint32_t seq,
                           const void *body, uint32_t body_len)
{
    int rc;

    if (target == NULL) {
        return -1;
    }

    pthread_mutex_lock(&target->send_lock);
    if (!target->connected || target->sockfd < 0) {
        pthread_mutex_unlock(&target->send_lock);
        return -1;
    }
    rc = proto_send_packet(target->sockfd, PROTO_INDICATION,
                           PROTO_IND_MESSAGE, 0, seq, body, body_len);
    pthread_mutex_unlock(&target->send_lock);
    return rc;
}

static void handle_request(client_node_t *client, const proto_message_t *msg)
{
    if (msg->kind != PROTO_REQUEST) {
        send_response(client, msg->type, PROTO_ERR_BAD_REQUEST,
                      msg->seq, NULL, 0);
        return;
    }

    switch (msg->type) {
    case PROTO_REQ_TIME:
        handle_time(client, msg);
        break;
    case PROTO_REQ_NAME:
        handle_name(client, msg);
        break;
    case PROTO_REQ_LIST:
        handle_list(client, msg);
        break;
    case PROTO_REQ_MESSAGE:
        handle_message(client, msg);
        break;
    default:
        send_response(client, msg->type, PROTO_ERR_BAD_REQUEST,
                      msg->seq, NULL, 0);
        break;
    }
}

static void handle_time(client_node_t *client, const proto_message_t *req)
{
    /* TODO: time() + strftime()，把时间文本放进响应 body。 */
    send_response(client, req->type, PROTO_ERR_INTERNAL, req->seq, NULL, 0);
}

static void handle_name(client_node_t *client, const proto_message_t *req)
{
    /* TODO: gethostname()，把主机名放进响应 body。 */
    send_response(client, req->type, PROTO_ERR_INTERNAL, req->seq, NULL, 0);
}

static void handle_list(client_node_t *client, const proto_message_t *req)
{
    /* TODO: 遍历 client->server->clients，组装客户端列表 body。 */
    send_response(client, req->type, PROTO_ERR_INTERNAL, req->seq, NULL, 0);
}

static void handle_message(client_node_t *client, const proto_message_t *req)
{
    client_node_t *target;
    uint32_t target_id = 0; /* TODO: 从 req->body 前 4 字节读取 */

    target = client_list_find(client->server, (int)target_id);
    if (target != NULL) {
        /* TODO: 组装 sender 信息 + 消息文本，再调用 send_indication()。 */
        (void)send_indication(target, req->seq, NULL, 0);
    }

    send_response(client, req->type, PROTO_ERR_INTERNAL, req->seq, NULL, 0);
}

static void *client_thread(void *arg)
{
    client_node_t *client = (client_node_t *)arg;
    proto_message_t msg;

    while (!g_stop && client->connected) {
        if (proto_recv_packet(client->sockfd, &msg) < 0) {
            break;
        }
        handle_request(client, &msg);
    }

    pthread_mutex_lock(&client->send_lock);
    client->connected = 0;
    if (client->sockfd >= 0) {
        close(client->sockfd);
        client->sockfd = -1;
    }
    pthread_mutex_unlock(&client->send_lock);
    return NULL;
}

static int server_run(server_t *s, int port)
{
    struct sockaddr_in addr;
    int yes = 1;
    int fd;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, SOMAXCONN) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }

    s->listen_fd = fd;
    printf("服务器正在监听端口 %d\n", port);
    fflush(stdout);

    while (!g_stop) {
        fd_set readfds;
        struct timeval timeout;
        int ret;

        FD_ZERO(&readfds);
        FD_SET(fd, &readfds);
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        ret = select(fd + 1, &readfds, NULL, NULL, &timeout);
        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("select");
            break;
        }
        if (ret == 0) {
            continue;
        }

        {
            struct sockaddr_in peer;
            socklen_t peer_len = sizeof(peer);
            int client_fd = accept(fd, (struct sockaddr *)&peer, &peer_len);
            client_node_t *node;
            char ip[INET_ADDRSTRLEN];

            if (client_fd < 0) {
                if (errno == EINTR) {
                    continue;
                }
                perror("accept");
                continue;
            }

            node = client_list_add(s, client_fd, &peer);
            if (node == NULL) {
                close(client_fd);
                continue;
            }

            node->thread_started = 1;
            if (pthread_create(&node->thread, NULL, client_thread, node) != 0) {
                perror("pthread_create");
                node->thread_started = 0;
                client_list_remove(s, node);
                continue;
            }

            inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
            printf("客户端 #%d 已连接: %s:%u\n",
                   node->id, ip, ntohs(peer.sin_port));
            fflush(stdout);
        }
    }
    return 0;
}

static void server_shutdown_clients(server_t *s)
{
    client_node_t *node;
    client_node_t *next;

    for (node = s->clients; node != NULL; node = node->next) {
        pthread_mutex_lock(&node->send_lock);
        node->connected = 0;
        if (node->sockfd >= 0) {
            shutdown(node->sockfd, SHUT_RDWR);
        }
        pthread_mutex_unlock(&node->send_lock);
    }

    for (node = s->clients; node != NULL; node = node->next) {
        if (node->thread_started) {
            pthread_join(node->thread, NULL);
            node->thread_started = 0;
        }
    }

    node = s->clients;
    while (node != NULL) {
        next = node->next;
        if (node->sockfd >= 0) {
            close(node->sockfd);
        }
        pthread_mutex_destroy(&node->send_lock);
        free(node);
        node = next;
    }
    s->clients = NULL;
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
    printf("服务器已退出。\n");
    return 0;
}
