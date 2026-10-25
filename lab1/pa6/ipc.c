#define _POSIX_C_SOURCE 200809L
#include "process.h"

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* Блокируем SIGPIPE только на время отправки. */
static int write_message(int fd, const void *buffer, size_t size)
{
    const char *data = (const char *)buffer;
    sigset_t blocked;
    sigset_t original_mask;
    sigset_t pending;
    size_t written = 0;
    int pending_before = 1;
    int result = -1;
    int saved_errno;

    if (sigemptyset(&blocked) < 0 || sigaddset(&blocked, SIGPIPE) < 0 ||
        sigprocmask(SIG_BLOCK, &blocked, &original_mask) < 0) {
        return -1;
    }
    if (sigpending(&pending) < 0) {
        goto restore_mask;
    }
    pending_before = sigismember(&pending, SIGPIPE);

    while (written < size) {
        size_t remaining = size - written;
        ssize_t count;

        /* libruntime ошибочно запрещает запись размером sizeof(Message).
         * В каждом pipe один писатель, поэтому кадр можно разбить. */
        if (remaining >= MAX_MESSAGE_LEN) {
            remaining = MAX_MESSAGE_LEN - 1;
        }
        count = write(fd, data + written, remaining);
        if (count > 0) {
            written += (size_t)count;
        } else if (count < 0 && errno == EINTR) {
            continue;
        } else if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            sched_yield();
            continue;
        } else {
            if (count == 0) {
                errno = EIO;
            }
            goto restore_mask;
        }
    }
    result = 0;

restore_mask:
    saved_errno = errno;
    if (result < 0 && saved_errno == EPIPE && !pending_before &&
        sigpending(&pending) == 0 && sigismember(&pending, SIGPIPE) == 1) {
        int signal_number;
        /* Забираем только сигнал от этой отправки. */
        (void)sigwait(&blocked, &signal_number);
    }
    if (sigprocmask(SIG_SETMASK, &original_mask, NULL) < 0) {
        return -1;
    }
    errno = saved_errno;
    return result;
}

static int write_log(int fd, const void *buffer, size_t size)
{
    const char *data = (const char *)buffer;
    size_t written = 0;

    while (written < size) {
        ssize_t result = write(fd, data + written, size - written);
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

static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0) {
        return -1;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void close_pipe_topology(int pipes[PROCESS_LIMIT][PROCESS_LIMIT][2])
{
    int source;
    int destination;
    int end;

    for (source = 0; source < PROCESS_LIMIT; ++source) {
        for (destination = 0; destination < PROCESS_LIMIT; ++destination) {
            for (end = 0; end < 2; ++end) {
                if (pipes[source][destination][end] >= 0) {
                    close(pipes[source][destination][end]);
                    pipes[source][destination][end] = -1;
                }
            }
        }
    }
}

int create_pipe_topology(
    int process_count,
    int pipes[PROCESS_LIMIT][PROCESS_LIMIT][2],
    int pipes_log_fd
)
{
    int source;
    int destination;

    for (source = 0; source < PROCESS_LIMIT; ++source) {
        for (destination = 0; destination < PROCESS_LIMIT; ++destination) {
            pipes[source][destination][0] = -1;
            pipes[source][destination][1] = -1;
        }
    }

    if (process_count < 1 || process_count > PROCESS_LIMIT || pipes_log_fd < 0) {
        errno = EINVAL;
        return -1;
    }

    for (source = 0; source < process_count; ++source) {
        for (destination = 0; destination < process_count; ++destination) {
            char line[128];
            int length;

            if (source == destination) {
                continue;
            }
            if (pipe(pipes[source][destination]) < 0 ||
                set_nonblocking(pipes[source][destination][0]) < 0 ||
                set_nonblocking(pipes[source][destination][1]) < 0) {
                goto failed;
            }

            length = snprintf(
                line,
                sizeof(line),
                "Pipe %d -> %d: read fd %d, write fd %d\n",
                source,
                destination,
                pipes[source][destination][0],
                pipes[source][destination][1]
            );
            if (length < 0 || (size_t)length >= sizeof(line) ||
                write_log(pipes_log_fd, line, (size_t)length) != 0) {
                goto failed;
            }
        }
    }
    return 0;

failed:
    {
        int saved_errno = errno;
        close_pipe_topology(pipes);
        errno = saved_errno;
    }
    return -1;
}

void prepare_process_context(
    ProcessContext *context,
    local_id id,
    int process_count,
    int pipes[PROCESS_LIMIT][PROCESS_LIMIT][2]
)
{
    int source;
    int destination;

    memset(context, 0, sizeof(*context));
    context->id = id;
    context->process_count = process_count;
    context->last_sender = -1;

    for (source = 0; source < PROCESS_LIMIT; ++source) {
        context->read_fd[source] = -1;
        context->write_fd[source] = -1;
    }

    for (source = 0; source < process_count; ++source) {
        for (destination = 0; destination < process_count; ++destination) {
            if (source == destination) {
                continue;
            }

            if (destination == id) {
                context->read_fd[source] = pipes[source][destination][0];
            } else {
                close(pipes[source][destination][0]);
            }

            if (source == id) {
                context->write_fd[destination] = pipes[source][destination][1];
            } else {
                close(pipes[source][destination][1]);
            }
        }
    }

    if (id != PARENT_ID) {
        for (source = 1; source < process_count; ++source) {
            if (source == id) {
                continue;
            }
            context->has_fork[source] = id < source;
            context->fork_dirty[source] = context->has_fork[source];
        }
    }
}

void close_process_context(ProcessContext *context)
{
    int id;

    for (id = 0; id < context->process_count; ++id) {
        if (context->read_fd[id] >= 0) {
            close(context->read_fd[id]);
            context->read_fd[id] = -1;
        }
        if (context->write_fd[id] >= 0) {
            close(context->write_fd[id]);
            context->write_fd[id] = -1;
        }
    }
}

int send(void *self, local_id destination, const Message *message)
{
    ProcessContext *context = (ProcessContext *)self;
    size_t total_size;

    if (context == NULL || message == NULL || destination < 0 ||
        destination >= context->process_count || destination == context->id ||
        context->write_fd[(int)destination] < 0 ||
        message->s_header.s_magic != MESSAGE_MAGIC ||
        message->s_header.s_payload_len > MAX_PAYLOAD_LEN) {
        return -1;
    }

    total_size = sizeof(MessageHeader) + message->s_header.s_payload_len;
    return write_message(context->write_fd[(int)destination], message, total_size);
}

int send_multicast(void *self, const Message *message)
{
    ProcessContext *context = (ProcessContext *)self;
    int destination;

    if (context == NULL) {
        return -1;
    }
    for (destination = 0; destination < context->process_count; ++destination) {
        if (destination == context->id) {
            continue;
        }
        if (send(context, (local_id)destination, message) != 0) {
            return -1;
        }
    }
    return 0;
}

static int fail_source(ProcessContext *context, local_id source, int error)
{
    close(context->read_fd[(int)source]);
    context->read_fd[(int)source] = -1;
    context->receive_state[(int)source].header_bytes = 0;
    context->receive_state[(int)source].payload_bytes = 0;
    errno = error;
    return -1;
}

static int pump_source(ProcessContext *context, local_id source, Message *message)
{
    ReceiveState *state = &context->receive_state[(int)source];
    ssize_t result;

    if (state->header_bytes < sizeof(MessageHeader)) {
        char *header = (char *)&state->message.s_header;
        result = read(
            context->read_fd[(int)source],
            header + state->header_bytes,
            sizeof(MessageHeader) - state->header_bytes
        );
        if (result > 0) {
            state->header_bytes += (size_t)result;
        } else if (result == 0 && state->header_bytes == 0) {
            close(context->read_fd[(int)source]);
            context->read_fd[(int)source] = -1;
            errno = EPIPE;
            return 2;
        } else if (result < 0 && errno == EINTR) {
            return 1;
        } else if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 1;
        } else {
            return fail_source(context, source, result == 0 ? EPROTO : errno);
        }

        if (state->header_bytes < sizeof(MessageHeader)) {
            return 1;
        }
        if (state->message.s_header.s_magic != MESSAGE_MAGIC) {
            return fail_source(context, source, EPROTO);
        }
        if (state->message.s_header.s_payload_len > MAX_PAYLOAD_LEN) {
            return fail_source(context, source, EMSGSIZE);
        }
    }

    while (state->payload_bytes < state->message.s_header.s_payload_len) {
        result = read(
            context->read_fd[(int)source],
            state->message.s_payload + state->payload_bytes,
            state->message.s_header.s_payload_len - state->payload_bytes
        );
        if (result > 0) {
            state->payload_bytes += (size_t)result;
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 1;
        } else {
            return fail_source(context, source, result == 0 ? EPROTO : errno);
        }
    }

    *message = state->message;
    state->header_bytes = 0;
    state->payload_bytes = 0;
    context->last_sender = source;
    return 0;
}

int receive(void *self, local_id source, Message *message)
{
    ProcessContext *context = (ProcessContext *)self;

    if (context == NULL || message == NULL || source < 0 ||
        source >= context->process_count || source == context->id ||
        context->read_fd[(int)source] < 0) {
        return -1;
    }

    for (;;) {
        int result = pump_source(context, source, message);
        if (result <= 0) {
            return result;
        }
        if (result == 2) {
            return -1;
        }
        sched_yield();
    }
}

int receive_any_nonblocking(ProcessContext *context, Message *message)
{
    int checked;
    int live_sources = 0;

    if (context == NULL || message == NULL || context->process_count < 1 ||
        context->process_count > PROCESS_LIMIT) {
        errno = EINVAL;
        return -1;
    }
    for (checked = 0; checked < context->process_count; ++checked) {
        int source = (context->next_source + checked) % context->process_count;
        int result;

        if (source == context->id || context->read_fd[source] < 0) {
            continue;
        }
        result = pump_source(context, (local_id)source, message);
        if (result == 0) {
            context->next_source = (source + 1) % context->process_count;
            return 0;
        }
        if (result < 0) {
            return -1;
        }
        if (result == 1) {
            ++live_sources;
        }
    }
    if (live_sources == 0) {
        errno = EPIPE;
        return -1;
    }
    errno = EAGAIN;
    return 1;
}

int receive_any(void *self, Message *message)
{
    ProcessContext *context = (ProcessContext *)self;

    for (;;) {
        int result = receive_any_nonblocking(context, message);
        if (result <= 0) {
            return result;
        }
        sched_yield();
    }
}
