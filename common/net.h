#ifndef LAB7_NET_H
#define LAB7_NET_H

#include <stddef.h>

/* 循环发送，直到 len 个字节全部发出。成功返回 0，失败返回 -1。 */
int net_send_all(int fd, const void *buf, size_t len);

/* 循环接收，直到读满 len 个字节。对端关闭或出错返回 -1。 */
int net_recv_all(int fd, void *buf, size_t len);

#endif
