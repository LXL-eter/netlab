#include "protocol.h"
#include "net.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define CLIENT_QUEUE_SIZE 16

typedef struct {
    proto_message_t items[CLIENT_QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
} msg_queue_t;

typedef struct {
    int sockfd;
    int connected;
    int running;

    pthread_t recv_thread;
    uint32_t next_seq;

    msg_queue_t queue;
    int wake_pipe[2];
} client_t;

static void print_menu(void);
static int read_line(char *buf, size_t size);

static void client_init(client_t *c);
static void client_cleanup(client_t *c);
static void client_run(client_t *c);
static void client_handle_choice(client_t *c);

static void client_connect(client_t *c);
static void client_disconnect(client_t *c);
static void client_send_request(client_t *c, uint8_t type,
                                const void *body, uint32_t body_len);

static void *client_receiver(void *arg);
static void client_process_queue(client_t *c);
static void client_handle_message(client_t *c, const proto_message_t *msg);

static void client_get_time(client_t *c);
static void client_get_name(client_t *c);
static void client_get_list(client_t *c);
static void client_send_message(client_t *c);

static void queue_init(msg_queue_t *q);
static void queue_destroy(msg_queue_t *q);
static int queue_push(msg_queue_t *q, const proto_message_t *msg);
static int queue_pop(msg_queue_t *q, proto_message_t *msg);

static void print_menu(void)
{
    printf("\n===== 客户端菜单 =====\n");
    printf("1. 连接\n");
    printf("2. 断开连接\n");
    printf("3. 获取时间\n");
    printf("4. 获取名字\n");
    printf("5. 活动连接列表\n");
    printf("6. 发消息\n");
    printf("7. 退出\n");
    printf("请选择: ");
    fflush(stdout);
}

static int read_line(char *buf, size_t size)
{
    size_t len;

    if (fgets(buf, (int)size, stdin) == NULL) {
        return -1;
    }

    len = strlen(buf);
    if (len > 0 && buf[len - 1] == '\n') {
        buf[len - 1] = '\0';
    }
    return 0;
}

static void client_init(client_t *c)
{
    memset(c, 0, sizeof(*c));
    c->sockfd = -1;
    c->running = 1;
    queue_init(&c->queue);
}

static void client_cleanup(client_t *c)
{
    /* TODO: 关闭 socket、pipe，销毁队列。 */
    (void)c;
}

static void client_run(client_t *c)
{
    print_menu();
    while (c->running) {
        /*
         * TODO: 用 select() 同时等待 stdin 和接收线程的唤醒管道，
         * 这样主线程在等输入时也能处理消息队列。
         */
        client_handle_choice(c);
        if (c->running) {
            print_menu();
        }
    }
}

static void client_handle_choice(client_t *c)
{
    char line[64];
    int choice;

    if (read_line(line, sizeof(line)) < 0) {
        c->running = 0;
        return;
    }

    choice = atoi(line);
    if (!c->connected && choice != 1 && choice != 7) {
        printf("未连接时只能选择“连接”或“退出”。\n");
        return;
    }

    switch (choice) {
    case 1:
        client_connect(c);
        break;
    case 2:
        client_disconnect(c);
        break;
    case 3:
        client_get_time(c);
        break;
    case 4:
        client_get_name(c);
        break;
    case 5:
        client_get_list(c);
        break;
    case 6:
        client_send_message(c);
        break;
    case 7:
        c->running = 0;
        break;
    default:
        printf("无效选项。\n");
        break;
    }
}

static void client_connect(client_t *c)
{
    /* TODO: 输入 IP/端口，socket(), connect()，再创建 client_receiver 线程。 */
    (void)c;
    printf("TODO: 连接功能待实现。\n");
}

static void client_disconnect(client_t *c)
{
    /* TODO: 设置未连接，shutdown/close，并等待接收线程退出。 */
    (void)c;
    printf("TODO: 断开连接功能待实现。\n");
}

static void client_send_request(client_t *c, uint8_t type,
                                const void *body, uint32_t body_len)
{
    /* TODO: 调用 proto_send_packet() 发送一个请求。 */
    (void)c;
    (void)type;
    (void)body;
    (void)body_len;
}

static void *client_receiver(void *arg)
{
    /* TODO: 循环 proto_recv_packet()，把完整报文放进消息队列并唤醒主线程。 */
    (void)arg;
    return NULL;
}

static void client_process_queue(client_t *c)
{
    /* TODO: 从队列取出报文，调用 client_handle_message()。 */
    (void)c;
}

static void client_handle_message(client_t *c, const proto_message_t *msg)
{
    /* TODO: 区分响应和指示，打印时间、名字、列表或转发消息。 */
    (void)c;
    (void)msg;
}

static void client_get_time(client_t *c)
{
    /* TODO: 发送 PROTO_REQ_TIME。报告中的 100 次请求可在这里循环。 */
    (void)c;
}

static void client_get_name(client_t *c)
{
    /* TODO: 发送 PROTO_REQ_NAME。 */
    (void)c;
}

static void client_get_list(client_t *c)
{
    /* TODO: 发送 PROTO_REQ_LIST。 */
    (void)c;
}

static void client_send_message(client_t *c)
{
    /* TODO: 先获取列表，再输入目标编号和消息内容，组装 PROTO_REQ_MESSAGE。 */
    (void)c;
}

static void queue_init(msg_queue_t *q)
{
    /* TODO: 初始化互斥锁和队列下标。 */
    (void)q;
}

static void queue_destroy(msg_queue_t *q)
{
    /* TODO: 销毁互斥锁。 */
    (void)q;
}

static int queue_push(msg_queue_t *q, const proto_message_t *msg)
{
    /* TODO: 加锁后把报文放入环形队列。 */
    (void)q;
    (void)msg;
    return -1;
}

static int queue_pop(msg_queue_t *q, proto_message_t *msg)
{
    /* TODO: 加锁后从环形队列取出一个报文。 */
    (void)q;
    (void)msg;
    return -1;
}

int main(void)
{
    client_t client;

    client_init(&client);
    client_run(&client);
    client_cleanup(&client);
    return 0;
}
