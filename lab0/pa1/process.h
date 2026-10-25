#ifndef PROCESS_H
#define PROCESS_H

#include <stddef.h>
#include <sys/types.h>

#include "ipc.h"

#define PROCESS_LIMIT (MAX_PROCESS_ID + 1)

typedef struct {
    Message message;
    size_t header_bytes;
    size_t payload_bytes;
} ReceiveState;

typedef struct {
    local_id id;
    int process_count;
    int read_fd[PROCESS_LIMIT];
    int write_fd[PROCESS_LIMIT];
    ReceiveState receive_state[PROCESS_LIMIT];
    local_id last_sender;
    int next_source;
} ProcessContext;

int create_pipe_topology(
    int process_count,
    int pipes[PROCESS_LIMIT][PROCESS_LIMIT][2],
    int pipes_log_fd
);

void prepare_process_context(
    ProcessContext *context,
    local_id id,
    int process_count,
    int pipes[PROCESS_LIMIT][PROCESS_LIMIT][2]
);

void close_process_context(ProcessContext *context);

void close_pipe_topology(int pipes[PROCESS_LIMIT][PROCESS_LIMIT][2]);

int receive_any_nonblocking(ProcessContext *context, Message *message);

int wait_for_children(pid_t children[PROCESS_LIMIT], int process_count);
void terminate_children(pid_t children[PROCESS_LIMIT], int process_count);

#endif
