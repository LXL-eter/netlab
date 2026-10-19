#ifndef LAB7_PROTOCOL_H
#define LAB7_PROTOCOL_H

#include <stdint.h>

/*
 * 固定 16 字节报文头：
 * magic(4) | version(1) | kind(1) | type(1) | status(1) |
 * seq(4) | body_len(4)
 *
 * 线上传输时，多字节整数一律使用网络字节序（大端）。
 */
#define PROTO_MAGIC        0x4C374E57u
#define PROTO_VERSION      1
#define PROTO_HEADER_SIZE  16
#define PROTO_MAX_BODY     8192
#define PROTO_MAX_TEXT     4096

/* 报文类别 */
enum {
    PROTO_REQUEST    = 1,
    PROTO_RESPONSE   = 2,
    PROTO_INDICATION = 3
};

/* 请求类型；响应报文的 type 与对应请求相同 */
enum {
    PROTO_REQ_TIME    = 1,
    PROTO_REQ_NAME    = 2,
    PROTO_REQ_LIST    = 3,
    PROTO_REQ_MESSAGE = 4
};

/* 指示类型：服务器主动发给客户端 */
enum {
    PROTO_IND_MESSAGE = 1
};

/* 响应状态码 */
enum {
    PROTO_OK              = 0,
    PROTO_ERR_BAD_REQUEST = 1,
    PROTO_ERR_NOT_FOUND   = 2,
    PROTO_ERR_SEND_FAILED = 3,
    PROTO_ERR_INTERNAL    = 4
};

/* 线上报文头，多字节字段为网络字节序 */
typedef struct {
    uint32_t magic;
    uint8_t  version;
    uint8_t  kind;
    uint8_t  type;
    uint8_t  status;
    uint32_t seq;
    uint32_t body_len;
} proto_header_t;

/* 程序内部使用的报文，字段已转换为主机字节序 */
typedef struct {
    uint8_t  kind;
    uint8_t  type;
    uint8_t  status;
    uint32_t seq;
    uint32_t body_len;
    uint8_t  body[PROTO_MAX_BODY];
} proto_message_t;

/* 客户端列表中的一条记录，线上为网络字节序，共 12 字节 */
typedef struct {
    uint32_t id;
    uint32_t ip;
    uint16_t port;
    uint16_t state;   /* 1 表示已连接 */
} proto_client_info_t;

/* 转发消息指示包的前 12 字节，后面紧跟 UTF-8 消息文本 */
typedef struct {
    uint32_t sender_id;
    uint32_t sender_ip;
    uint16_t sender_port;
    uint16_t reserved;
} proto_msg_indication_t;

void proto_header_encode(proto_header_t *wire, uint8_t kind, uint8_t type,
                         uint8_t status, uint32_t seq, uint32_t body_len);
int proto_header_decode(const proto_header_t *wire, proto_message_t *msg);

/* 发送/接收一个完整报文；返回 0 成功，-1 失败。 */
int proto_send_packet(int fd, uint8_t kind, uint8_t type, uint8_t status,
                      uint32_t seq, const void *body, uint32_t body_len);
int proto_recv_packet(int fd, proto_message_t *msg);

#endif
