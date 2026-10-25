#define _POSIX_C_SOURCE 200809L
#include "common.h"
#include "pa2345.h"
#include "process.h"

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
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

static int duplicate_to(int source, int destination)
{
    int result;
    do {
        result = dup2(source, destination);
    } while (result < 0 && errno == EINTR);
    return result < 0 ? -1 : 0;
}

static int print_iteration(const char *line)
{
    int saved_stderr;
    int result;
    int saved_errno;

    do {
        saved_stderr = dup(STDERR_FILENO);
    } while (saved_stderr < 0 && errno == EINTR);
    if (saved_stderr < 0) {
        return -1;
    }
    /* print() из libruntime пишет в fd 2; направляем её вывод в stdout. */
    result = duplicate_to(STDOUT_FILENO, STDERR_FILENO);
    if (result == 0) {
        print(line);
    }
    saved_errno = errno;
    if (duplicate_to(saved_stderr, STDERR_FILENO) != 0) {
        close(saved_stderr);
        return -1;
    }
    close(saved_stderr);
    errno = saved_errno;
    return result;
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

static int send_empty(ProcessContext *context, local_id destination, MessageType type)
{
    Message message;
    make_message(&message, type, NULL);
    return send(context, destination, &message);
}

static int request_missing_fork(ProcessContext *context, local_id peer)
{
    if (context->has_fork[(int)peer] ||
        context->request_outstanding[(int)peer]) {
        return 0;
    }
    if (send_empty(context, peer, CS_REQUEST) != 0) {
        return -1;
    }
    context->request_outstanding[(int)peer] = 1;
    return 0;
}

static int give_fork(ProcessContext *context, local_id peer)
{
    if (send_empty(context, peer, CS_REPLY) != 0) {
        return -1;
    }
    context->has_fork[(int)peer] = 0;
    context->fork_dirty[(int)peer] = 0;
    context->request_pending[(int)peer] = 0;
    if (context->hungry) {
        return request_missing_fork(context, peer);
    }
    return 0;
}

static int handle_message(
    ProcessContext *context,
    local_id source,
    const Message *message
)
{
    switch ((MessageType)message->s_header.s_type) {
        case STARTED:
            context->started_seen[(int)source] = 1;
            return 0;
        case DONE:
            context->done_seen[(int)source] = 1;
            return 0;
        case CS_REQUEST:
            if (source == PARENT_ID) {
                return -1;
            }
            if (context->has_fork[(int)source] &&
                context->fork_dirty[(int)source] &&
                !context->in_critical_section) {
                return give_fork(context, source);
            }
            if (context->has_fork[(int)source]) {
                context->request_pending[(int)source] = 1;
            }
            return 0;
        case CS_REPLY:
            if (source == PARENT_ID) {
                return -1;
            }
            context->has_fork[(int)source] = 1;
            context->fork_dirty[(int)source] = 0;
            context->request_outstanding[(int)source] = 0;
            return 0;
        case CS_RELEASE:
            return 0;
        default:
            return -1;
    }
}

static int handle_one_message(ProcessContext *context, int blocking)
{
    for (;;) {
        Message message;
        int source;
        int result = receive_any_nonblocking(context, &message);

        if (result < 0) {
            return -1;
        }
        if (result == 0 &&
            handle_message(context, context->last_sender, &message) != 0) {
            return -1;
        }
        /* EOF допустим только после завершения обмена DONE. */
        for (source = 0; source < context->process_count; ++source) {
            if (source != context->id && context->read_fd[source] < 0 &&
                (source == PARENT_ID || !context->done_seen[source] ||
                 !context->done_sent)) {
                errno = EPIPE;
                return -1;
            }
        }
        if (result == 0 || !blocking) {
            return result;
        }
        sched_yield();
    }
}

static int drain_messages(ProcessContext *context)
{
    for (;;) {
        int result = handle_one_message(context, 0);
        if (result == 1) {
            return 0;
        }
        if (result < 0) {
            return -1;
        }
    }
}

static int has_all_forks(const ProcessContext *context)
{
    int peer;
    for (peer = 1; peer < context->process_count; ++peer) {
        if (peer != context->id && !context->has_fork[peer]) {
            return 0;
        }
    }
    return 1;
}

int request_cs(const void *self)
{
    ProcessContext *context = (ProcessContext *)self;
    int peer;

    if (context == NULL || context->id == PARENT_ID ||
        context->in_critical_section) {
        return -1;
    }
    if (drain_messages(context) != 0) {
        return -1;
    }

    context->hungry = 1;
    for (peer = 1; peer < context->process_count; ++peer) {
        if (peer != context->id &&
            request_missing_fork(context, (local_id)peer) != 0) {
            return -1;
        }
    }

    while (!has_all_forks(context)) {
        if (handle_one_message(context, 1) != 0) {
            return -1;
        }
    }
    context->hungry = 0;
    context->in_critical_section = 1;
    return 0;
}

int release_cs(const void *self)
{
    ProcessContext *context = (ProcessContext *)self;
    int peer;

    if (context == NULL || !context->in_critical_section) {
        return -1;
    }
    context->in_critical_section = 0;
    for (peer = 1; peer < context->process_count; ++peer) {
        if (peer != context->id && context->has_fork[peer]) {
            context->fork_dirty[peer] = 1;
        }
    }
    if (drain_messages(context) != 0) {
        return -1;
    }
    for (peer = 1; peer < context->process_count; ++peer) {
        if (peer != context->id && context->request_pending[peer] &&
            context->has_fork[peer] && context->fork_dirty[peer] &&
            give_fork(context, (local_id)peer) != 0) {
            return -1;
        }
    }
    return 0;
}

static int log_with_optional_mutex(
    ProcessContext *context,
    const char *line,
    int mutex_enabled
)
{
    int result;

    if (mutex_enabled && request_cs(context) != 0) {
        return -1;
    }
    result = log_line(line);
    if (mutex_enabled && release_cs(context) != 0) {
        return -1;
    }
    return result;
}

static int all_children_seen(const ProcessContext *context, const int seen[])
{
    int id;
    for (id = 1; id < context->process_count; ++id) {
        if (id != context->id && !seen[id]) {
            return 0;
        }
    }
    return 1;
}

static int wait_for_child_event(ProcessContext *context, int started)
{
    int *seen = started ? context->started_seen : context->done_seen;
    while (!all_children_seen(context, seen)) {
        if (handle_one_message(context, 1) != 0) {
            return -1;
        }
    }
    return 0;
}

static int run_child(ProcessContext *context, int mutex_enabled)
{
    Message message;
    char line[256];
    int length;
    int iteration;
    int iteration_count = context->id * 5;

    length = snprintf(
        line,
        sizeof(line),
        log_started_fmt,
        0,
        context->id,
        (int)getpid(),
        (int)getppid(),
        0
    );
    if (length < 0 || (size_t)length >= sizeof(line) || log_line(line) != 0) {
        return -1;
    }
    make_message(&message, STARTED, line);
    if (send_multicast(context, &message) != 0 ||
        wait_for_child_event(context, 1) != 0) {
        return -1;
    }

    length = snprintf(
        line,
        sizeof(line),
        log_received_all_started_fmt,
        0,
        context->id
    );
    if (length < 0 || (size_t)length >= sizeof(line) ||
        log_with_optional_mutex(context, line, mutex_enabled) != 0) {
        return -1;
    }

    for (iteration = 1; iteration <= iteration_count; ++iteration) {
        if (mutex_enabled && request_cs(context) != 0) {
            return -1;
        }
        length = snprintf(
            line,
            sizeof(line),
            log_loop_operation_fmt,
            context->id,
            iteration,
            iteration_count
        );
        if (length < 0 || (size_t)length >= sizeof(line)) {
            return -1;
        }
        if (print_iteration(line) != 0) {
            return -1;
        }
        if (mutex_enabled && release_cs(context) != 0) {
            return -1;
        }
    }

    length = snprintf(line, sizeof(line), log_done_fmt, 0, context->id, 0);
    if (length < 0 || (size_t)length >= sizeof(line) ||
        log_with_optional_mutex(context, line, mutex_enabled) != 0) {
        return -1;
    }
    make_message(&message, DONE, line);
    if (send_multicast(context, &message) != 0) {
        return -1;
    }
    context->done_sent = 1;
    if (wait_for_child_event(context, 0) != 0) {
        return -1;
    }

    length = snprintf(
        line,
        sizeof(line),
        log_received_all_done_fmt,
        0,
        context->id
    );
    if (length < 0 || (size_t)length >= sizeof(line) || log_line(line) != 0) {
        return -1;
    }
    return 0;
}

static int run_parent(ProcessContext *context, pid_t children[PROCESS_LIMIT])
{
    int source;

    for (source = 1; source < context->process_count; ++source) {
        Message message;
        if (receive(context, (local_id)source, &message) != 0 ||
            message.s_header.s_type != STARTED) {
            return -1;
        }
    }
    for (source = 1; source < context->process_count; ++source) {
        Message message;
        if (receive(context, (local_id)source, &message) != 0 ||
            message.s_header.s_type != DONE) {
            return -1;
        }
    }
    return wait_for_children(children, context->process_count);
}

static int parse_arguments(
    int argc,
    char *argv[],
    int *process_count,
    int *mutex_enabled
)
{
    int index;
    int found_count = 0;

    *mutex_enabled = 0;
    for (index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--mutexl") == 0) {
            *mutex_enabled = 1;
        } else if (strcmp(argv[index], "-p") == 0 && index + 1 < argc &&
                   !found_count) {
            char *end;
            long children;
            errno = 0;
            children = strtol(argv[++index], &end, 10);
            if (errno != 0 || *argv[index] == '\0' || *end != '\0' ||
                children < 0 || children > MAX_PROCESS_ID) {
                return -1;
            }
            *process_count = (int)children + 1;
            found_count = 1;
        } else {
            return -1;
        }
    }
    return found_count ? 0 : -1;
}

int main(int argc, char *argv[])
{
    static int pipes[PROCESS_LIMIT][PROCESS_LIMIT][2];
    pid_t children[PROCESS_LIMIT];
    ProcessContext context;
    int process_count;
    int mutex_enabled;
    int pipes_fd;
    local_id self_id = PARENT_ID;
    int id;
    int result;

    memset(children, 0, sizeof(children));
    if (parse_arguments(argc, argv, &process_count, &mutex_enabled) != 0) {
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
        : run_child(&context, mutex_enabled);

    close_process_context(&context);
    if (self_id == PARENT_ID && result != 0) {
        terminate_children(children, process_count);
    }
    close(log_fd);
    return result == 0 ? 0 : 1;
}
