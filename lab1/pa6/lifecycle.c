#define _POSIX_C_SOURCE 200809L
#include "process.h"

#include <errno.h>
#include <signal.h>
#include <sys/wait.h>

int wait_for_children(pid_t children[PROCESS_LIMIT], int process_count)
{
    int id;

    for (id = 1; id < process_count; ++id) {
        int status;
        pid_t result;

        if (children[id] <= 0) {
            continue;
        }
        do {
            result = waitpid(children[id], &status, 0);
        } while (result < 0 && errno == EINTR);
        if (result < 0) {
            if (errno == ECHILD) {
                children[id] = 0;
            }
            return -1;
        }
        children[id] = 0;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            return -1;
        }
    }
    return 0;
}

void terminate_children(pid_t children[PROCESS_LIMIT], int process_count)
{
    int id;

    /* Сначала завершаем всех детей, затем собираем их статусы. */
    for (id = 1; id < process_count; ++id) {
        if (children[id] > 0) {
            (void)kill(children[id], SIGKILL);
        }
    }
    for (id = 1; id < process_count; ++id) {
        if (children[id] > 0) {
            while (waitpid(children[id], NULL, 0) < 0 && errno == EINTR) {
                /* Повторяем ожидание после сигнала. */
            }
            children[id] = 0;
        }
    }
}
