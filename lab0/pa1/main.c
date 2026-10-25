#define _POSIX_C_SOURCE 200809L
#include "common.h"
#include "pa1.h"
#include "process.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static int log_fd = -1;

static int write_complete(int fd, const char *text, size_t length)
{
    size_t written = 0;

    while (written < length) {
        ssize_t result = write(fd, text + written, length - written);
        if (result > 0) {
            written += (size_t)result;
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else {
            return -1;
        }
    }
    return 0;
}

static int log_line(const char *line)
{
    size_t length = strlen(line);
    if (write_complete(STDOUT_FILENO, line, length) != 0) {
        return -1;
    }
    return write_complete(log_fd, line, length);
}

static void make_message(Message *message, MessageType type, const char *payload)
{
    size_t length = payload == NULL ? 0 : strlen(payload);

    memset(message, 0, sizeof(*message));
    message->s_header.s_magic = MESSAGE_MAGIC;
    message->s_header.s_payload_len = (uint16_t)length;
    message->s_header.s_type = (int16_t)type;
    message->s_header.s_local_time = 0;
    if (length > 0) {
        memcpy(message->s_payload, payload, length);
    }
}

static int wait_for_messages(ProcessContext *context, MessageType type)
{
    int source;

    for (source = 1; source < context->process_count; ++source) {
        Message message;
        if (source == context->id) {
            continue;
        }
        if (receive(context, (local_id)source, &message) != 0 ||
            message.s_header.s_type != type) {
            return -1;
        }
    }
    return 0;
}

static int run_child(ProcessContext *context)
{
    Message message;
    char line[256];
    int length;

    length = snprintf(
        line,
        sizeof(line),
        log_started_fmt,
        context->id,
        (int)getpid(),
        (int)getppid()
    );
    if (length < 0 || (size_t)length >= sizeof(line) || log_line(line) != 0) {
        return -1;
    }
    make_message(&message, STARTED, line);
    if (send_multicast(context, &message) != 0 ||
        wait_for_messages(context, STARTED) != 0) {
        return -1;
    }

    length = snprintf(line, sizeof(line), log_received_all_started_fmt, context->id);
    if (length < 0 || (size_t)length >= sizeof(line) || log_line(line) != 0) {
        return -1;
    }

    length = snprintf(line, sizeof(line), log_done_fmt, context->id);
    if (length < 0 || (size_t)length >= sizeof(line) || log_line(line) != 0) {
        return -1;
    }
    make_message(&message, DONE, line);
    if (send_multicast(context, &message) != 0 ||
        wait_for_messages(context, DONE) != 0) {
        return -1;
    }

    length = snprintf(line, sizeof(line), log_received_all_done_fmt, context->id);
    if (length < 0 || (size_t)length >= sizeof(line) || log_line(line) != 0) {
        return -1;
    }
    return 0;
}

static int run_parent(ProcessContext *context, pid_t children[PROCESS_LIMIT])
{

    if (wait_for_messages(context, STARTED) != 0 ||
        wait_for_messages(context, DONE) != 0) {
        return -1;
    }

    return wait_for_children(children, context->process_count);
}

static int parse_process_count(int argc, char *argv[], int *process_count)
{
    char *end;
    long children;

    if (argc != 3 || strcmp(argv[1], "-p") != 0) {
        return -1;
    }
    errno = 0;
    children = strtol(argv[2], &end, 10);
    if (errno != 0 || *argv[2] == '\0' || *end != '\0' ||
        children < 0 || children > MAX_PROCESS_ID) {
        return -1;
    }
    *process_count = (int)children + 1;
    return 0;
}

int main(int argc, char *argv[])
{
    static int pipes[PROCESS_LIMIT][PROCESS_LIMIT][2];
    pid_t children[PROCESS_LIMIT];
    ProcessContext context;
    int process_count;
    int pipes_fd;
    local_id self_id = PARENT_ID;
    int id;
    int result;

    memset(children, 0, sizeof(children));
    if (parse_process_count(argc, argv, &process_count) != 0) {
        return 1;
    }

    pipes_fd = open(pipes_log, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    log_fd = open(events_log, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0644);
    if (pipes_fd < 0 || log_fd < 0 ||
        create_pipe_topology(process_count, pipes, pipes_fd) != 0) {
        if (pipes_fd >= 0) {
            close(pipes_fd);
        }
        if (log_fd >= 0) {
            close(log_fd);
        }
        return 1;
    }
    close(pipes_fd);

    for (id = 1; id < process_count; ++id) {
        pid_t child = fork();
        if (child < 0) {
            close_pipe_topology(pipes);
            terminate_children(children, process_count);
            close(log_fd);
            return 1;
        }
        if (child == 0) {
            self_id = (local_id)id;
            break;
        }
        children[id] = child;
    }

    prepare_process_context(&context, self_id, process_count, pipes);
    result = self_id == PARENT_ID
        ? run_parent(&context, children)
        : run_child(&context);

    close_process_context(&context);
    if (self_id == PARENT_ID && result != 0) {
        terminate_children(children, process_count);
    }
    close(log_fd);
    return result == 0 ? 0 : 1;
}
