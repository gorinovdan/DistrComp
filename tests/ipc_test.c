#define _POSIX_C_SOURCE 200809L
#include "process.h"
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static void initialize(ProcessContext *context, int fd, int sending)
{
    int i;
    memset(context, 0, sizeof(*context));
    context->id = 0;
    context->process_count = 2;
    for (i = 0; i < PROCESS_LIMIT; ++i) {
        context->read_fd[i] = -1;
        context->write_fd[i] = -1;
    }
    if (sending) context->write_fd[1] = fd;
    else context->read_fd[1] = fd;
}

static Message message(unsigned length)
{
    Message msg;
    unsigned i;
    memset(&msg, 0, sizeof(msg));
    msg.s_header.s_magic = MESSAGE_MAGIC;
    msg.s_header.s_type = ACK;
    msg.s_header.s_payload_len = (uint16_t)length;
    for (i = 0; i < length; ++i) msg.s_payload[i] = (char)(i % 251);
    return msg;
}

int main(int argc, char **argv)
{
    int p[2];
    ProcessContext context;
    Message expected;
    Message actual;
    if (argc != 2 || pipe(p) < 0) return 80;
    (void)fcntl(p[0], F_SETFL, O_NONBLOCK);
    (void)fcntl(p[1], F_SETFL, O_NONBLOCK);
    initialize(&context, p[0], 0);
    expected = message(MAX_PAYLOAD_LEN);
    if (strcmp(argv[1], "eof_any") == 0) {
        int result;
        close(p[1]);
        result = receive_any(&context, &actual);
        if (result == 0 || errno != EPIPE) return 98;
        printf("receive_any=%d\n", result);
    } else if (strcmp(argv[1], "eof_direct") == 0) {
        int result;
        close(p[1]);
        result = receive(&context, 1, &actual);
        if (result == 0) return 99;
        printf("receive=%d\n", result);
    } else if (strcmp(argv[1], "sigpipe") == 0) {
        int result;
        close(p[0]);
        initialize(&context, p[1], 1);
        result = send(&context, 1, &expected);
        if (result == 0 || errno != EPIPE) return 100;
        printf("send=%d\n", result);
    } else if (strcmp(argv[1], "roundtrip") == 0) {
        unsigned lengths[] = {0, 1, 409, MAX_PAYLOAD_LEN};
        unsigned i;
        ProcessContext sender;
        initialize(&sender, p[1], 1);
        for (i = 0; i < sizeof(lengths)/sizeof(lengths[0]); ++i) {
            expected = message(lengths[i]);
            if (send(&sender, 1, &expected) != 0 ||
                receive(&context, 1, &actual) != 0 ||
                memcmp(&expected, &actual, sizeof(MessageHeader)+lengths[i]) != 0) {
                return 81;
            }
        }
        printf("roundtrip OK max=%d PIPE_BUF=%ld\n", MAX_MESSAGE_LEN,
               fpathconf(p[1], _PC_PIPE_BUF));
    } else if (strcmp(argv[1], "interleaved") == 0) {
        int second[2];
        Message other = message(5);
        if (pipe(second) < 0) return 89;
        (void)fcntl(second[0], F_SETFL, O_NONBLOCK);
        context.process_count = 3;
        context.read_fd[2] = second[0];
        (void)write(p[1], &expected, 4);
        (void)write(second[1], &other, sizeof(MessageHeader)+5);
        if (receive_any(&context, &actual) != 0 || context.last_sender != 2 ||
            memcmp(&actual, &other, sizeof(MessageHeader)+5) != 0) return 92;
        (void)write(p[1], (const char *)&expected+4, sizeof(expected)-4);
        if (receive_any(&context, &actual) != 0 || context.last_sender != 1 ||
            memcmp(&actual, &expected, sizeof(expected)) != 0) return 93;
        close(second[0]);
        close(second[1]);
        puts("interleaved partial header OK");
    } else if (strcmp(argv[1], "backpressure") == 0) {
        ProcessContext sender;
        char filler[4096];
        size_t filled = 0;
        pid_t child;
        memset(filler, 'Z', sizeof(filler));
        while (write(p[1], filler, sizeof(filler)) == (ssize_t)sizeof(filler)) {
            filled += sizeof(filler);
        }
        if (errno != EAGAIN || filled == 0) return 94;
        child = fork();
        if (child == 0) {
            size_t received = 0;
            struct timespec pause = {0, 50000000};
            close(p[1]);
            (void)nanosleep(&pause, NULL);
            while (received < filled+sizeof(expected)) {
                ssize_t n = read(p[0], filler, sizeof(filler));
                if (n > 0) received += (size_t)n;
                else if (n < 0 && (errno == EAGAIN || errno == EINTR)) sched_yield();
                else _exit(95);
            }
            _exit(0);
        } else {
            int status;
            close(p[0]);
            initialize(&sender, p[1], 1);
            if (send(&sender, 1, &expected) != 0) return 96;
            if (waitpid(child, &status, 0) < 0 || status != 0) return 97;
            puts("EAGAIN backpressure OK");
        }
    } else if (strcmp(argv[1], "fragmented") == 0) {
        pid_t child = fork();
        if (child == 0) {
            const char *bytes = (const char *)&expected;
            size_t sent = 0;
            struct timespec delay = {0, 100000};
            close(p[0]);
            while (sent < sizeof(expected)) {
                ssize_t n = write(p[1], bytes + sent, 1);
                if (n == 1) { ++sent; (void)nanosleep(&delay, NULL); }
                else if (errno != EAGAIN && errno != EINTR) _exit(82);
            }
            close(p[1]);
            _exit(0);
        } else {
            int status;
            close(p[1]);
            if (receive_any(&context, &actual) != 0 ||
                memcmp(&expected, &actual, sizeof(expected)) != 0) return 83;
            if (waitpid(child, &status, 0) < 0 || status != 0) return 84;
            puts("fragmented OK");
        }
    } else if (strcmp(argv[1], "truncated") == 0) {
        (void)write(p[1], &expected, sizeof(MessageHeader)+3);
        close(p[1]);
        if (receive(&context, 1, &actual) == 0) return 85;
        puts("truncated rejected");
    } else if (strcmp(argv[1], "bad_magic") == 0) {
        expected.s_header.s_magic = 0;
        (void)write(p[1], &expected, sizeof(expected));
        if (receive(&context, 1, &actual) == 0) return 86;
        if (context.read_fd[1] >= 0 || receive(&context, 1, &actual) == 0) return 101;
        puts("bad magic rejected");
    } else if (strcmp(argv[1], "oversized") == 0) {
        expected.s_header.s_payload_len = MAX_PAYLOAD_LEN + 1;
        (void)write(p[1], &expected, sizeof(MessageHeader));
        if (receive(&context, 1, &actual) == 0) return 87;
        if (context.read_fd[1] >= 0 || receive(&context, 1, &actual) == 0) return 102;
        puts("oversized rejected");
    } else return 88;
    close(p[0]);
    close(p[1]);
    return 0;
}
