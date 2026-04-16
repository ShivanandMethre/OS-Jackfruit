#define _GNU_SOURCE

/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * This implementation keeps the starter CLI contract intact while filling in
 * the missing supervisor, logging, and control-plane behavior.
 */

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int finished;
    int stop_requested;
    int producer_started;
    int log_read_fd;
    char log_path[PATH_MAX];
    char termination_reason[32];
    void *child_stack;
    pthread_t producer_thread;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    pthread_cond_t state_changed;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    container_record_t *record;
} producer_arg_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int client_fd;
    control_request_t req;
} request_thread_arg_t;

static volatile sig_atomic_t g_supervisor_stop = 0;
static volatile sig_atomic_t g_client_forward_stop = 0;
static char g_client_run_id[CONTAINER_ID_LEN];
static supervisor_ctx_t *g_supervisor_ctx = NULL;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static void copy_cstr(char *dst, size_t dst_len, const char *src)
{
    if (dst_len == 0)
        return;

    if (!src) {
        dst[0] = '\0';
        return;
    }

    snprintf(dst, dst_len, "%s", src);
}

static ssize_t write_full(int fd, const void *buf, size_t len)
{
    const char *p = (const char *)buf;
    size_t total = 0;

    while (total < len) {
        ssize_t rc = write(fd, p + total, len - total);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (rc == 0)
            break;
        total += (size_t)rc;
    }

    return (ssize_t)total;
}

static ssize_t read_full(int fd, void *buf, size_t len)
{
    char *p = (char *)buf;
    size_t total = 0;

    while (total < len) {
        ssize_t rc = read(fd, p + total, len - total);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (rc == 0)
            break;
        total += (size_t)rc;
    }

    return (ssize_t)total;
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (!buffer->shutting_down && buffer->count == LOG_BUFFER_CAPACITY)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int ensure_log_dir(void)
{
    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST) {
        perror("mkdir logs");
        return -1;
    }

    return 0;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur = ctx->containers;

    while (cur) {
        if (strncmp(cur->id, id, sizeof(cur->id)) == 0)
            return cur;
        cur = cur->next;
    }

    return NULL;
}

static int container_is_live(const container_record_t *rec)
{
    return rec->state == CONTAINER_STARTING || rec->state == CONTAINER_RUNNING;
}

static void format_container_line(const container_record_t *rec, char *buf, size_t buf_len)
{
    struct tm tm_buf;
    char time_buf[64];

    localtime_r(&rec->started_at, &tm_buf);
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &tm_buf);

    snprintf(buf,
             buf_len,
             "%-12.12s pid=%-6d state=%-8s reason=%-18s soft=%-8lu hard=%-8lu start=%s cmd=%.120s log=%.120s\n",
             rec->id,
             rec->host_pid,
             state_to_string(rec->state),
             rec->termination_reason[0] ? rec->termination_reason : "-",
             rec->soft_limit_bytes >> 20,
             rec->hard_limit_bytes >> 20,
             time_buf,
             rec->command,
             rec->log_path);
}

static int stream_file_to_fd(const char *path, int fd)
{
    int in_fd;
    char buf[LOG_CHUNK_SIZE];

    in_fd = open(path, O_RDONLY);
    if (in_fd < 0)
        return -1;

    for (;;) {
        ssize_t n = read(in_fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR)
                continue;
            close(in_fd);
            return -1;
        }
        if (n == 0)
            break;
        if (write_full(fd, buf, (size_t)n) < 0) {
            close(in_fd);
            return -1;
        }
    }

    close(in_fd);
    return 0;
}

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        int fd;
        char path[PATH_MAX];

        if (ensure_log_dir() != 0)
            continue;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_CREAT | O_APPEND | O_WRONLY, 0644);

        if (fd < 0) {
            perror("open log file");
            continue;
        }

        if (write_full(fd, item.data, item.length) < 0)
            perror("write log file");

        close(fd);
    }

    return NULL;
}

static void *producer_thread(void *arg)
{
    producer_arg_t *producer = (producer_arg_t *)arg;
    supervisor_ctx_t *ctx = producer->ctx;
    container_record_t *record = producer->record;
    char buf[LOG_CHUNK_SIZE];

    for (;;) {
        ssize_t n = read(record->log_read_fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (n == 0)
            break;

        log_item_t item;
        memset(&item, 0, sizeof(item));
        copy_cstr(item.container_id, sizeof(item.container_id), record->id);
        item.length = (size_t)n;
        memcpy(item.data, buf, (size_t)n);

        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            break;
    }

    close(record->log_read_fd);
    record->log_read_fd = -1;
    free(producer);
    return NULL;
}

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    char *argv_exec[] = { cfg->command, NULL };

    if (cfg->nice_value != 0 && setpriority(PRIO_PROCESS, 0, cfg->nice_value) < 0)
        perror("setpriority");

    if (setsid() < 0)
        perror("setsid");

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) < 0)
        perror("mount private");

    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir");
        return 1;
    }

    if (mkdir("/proc", 0555) < 0 && errno != EEXIST) {
        perror("mkdir /proc");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount proc");
        return 1;
    }

    execv(cfg->command, argv_exec);
    execvp(cfg->command, argv_exec);

    perror("exec");
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    copy_cstr(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static void supervisor_signal_handler(int signo)
{
    if (signo == SIGINT || signo == SIGTERM) {
        g_supervisor_stop = 1;
        if (g_supervisor_ctx)
            g_supervisor_ctx->should_stop = 1;
    }
}

static void run_client_signal_handler(int signo)
{
    (void)signo;
    g_client_forward_stop = 1;
}

static int send_stop_request_for_id(const char *id)
{
    control_request_t req;
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_cstr(req.container_id, sizeof(req.container_id), id);

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (write_full(fd, &req, sizeof(req)) != (ssize_t)sizeof(req)) {
        close(fd);
        return -1;
    }

    if (read_full(fd, &resp, sizeof(resp)) < (ssize_t)sizeof(resp)) {
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

static int install_supervisor_signals(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = supervisor_signal_handler;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGINT, &sa, NULL) < 0)
        return -1;
    if (sigaction(SIGTERM, &sa, NULL) < 0)
        return -1;

    return 0;
}

static int install_run_client_signals(struct sigaction *old_int, struct sigaction *old_term)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = run_client_signal_handler;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGINT, &sa, old_int) < 0)
        return -1;
    if (sigaction(SIGTERM, &sa, old_term) < 0) {
        sigaction(SIGINT, old_int, NULL);
        return -1;
    }

    return 0;
}

static void restore_run_client_signals(const struct sigaction *old_int,
                                       const struct sigaction *old_term)
{
    sigaction(SIGINT, old_int, NULL);
    sigaction(SIGTERM, old_term, NULL);
}

static void fill_response(control_response_t *resp, int status, const char *message)
{
    memset(resp, 0, sizeof(*resp));
    resp->status = status;
    if (message)
        copy_cstr(resp->message, sizeof(resp->message), message);
}

static int launch_container(supervisor_ctx_t *ctx,
                            const control_request_t *req,
                            control_response_t *resp)
{
    container_record_t *record;
    child_config_t *cfg;
    producer_arg_t *producer;
    pid_t pid;
    int log_pipe[2];

    if (ensure_log_dir() != 0) {
        fill_response(resp, 1, "failed to create logs directory");
        return 1;
    }

    record = calloc(1, sizeof(*record));
    cfg = calloc(1, sizeof(*cfg));
    if (!record || !cfg) {
        free(record);
        free(cfg);
        fill_response(resp, 1, "out of memory");
        return 1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    {
        container_record_t *cur = ctx->containers;
        while (cur) {
            if (strncmp(cur->id, req->container_id, sizeof(cur->id)) == 0) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                free(record);
                free(cfg);
                fill_response(resp, 1, "container id already exists");
                return 1;
            }
            if (container_is_live(cur) &&
                strncmp(cur->rootfs, req->rootfs, sizeof(cur->rootfs)) == 0) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                free(record);
                free(cfg);
                fill_response(resp, 1, "rootfs already in use by a live container");
                return 1;
            }
            cur = cur->next;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pipe(log_pipe) < 0) {
        perror("pipe");
        free(record);
        free(cfg);
        fill_response(resp, 1, "failed to create logging pipe");
        return 1;
    }

    memset(record, 0, sizeof(*record));
    copy_cstr(record->id, sizeof(record->id), req->container_id);
    copy_cstr(record->rootfs, sizeof(record->rootfs), req->rootfs);
    copy_cstr(record->command, sizeof(record->command), req->command);
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);
    record->state = CONTAINER_STARTING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->started_at = time(NULL);
    record->log_read_fd = log_pipe[0];
    copy_cstr(record->termination_reason, sizeof(record->termination_reason), "starting");

    record->child_stack = malloc(STACK_SIZE);
    if (!record->child_stack) {
        close(log_pipe[0]);
        close(log_pipe[1]);
        free(record);
        free(cfg);
        fill_response(resp, 1, "failed to allocate child stack");
        return 1;
    }

    memset(cfg, 0, sizeof(*cfg));
    copy_cstr(cfg->id, sizeof(cfg->id), req->container_id);
    copy_cstr(cfg->rootfs, sizeof(cfg->rootfs), req->rootfs);
    copy_cstr(cfg->command, sizeof(cfg->command), req->command);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = log_pipe[1];

    pid = clone(child_fn,
                (char *)record->child_stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                cfg);
    if (pid < 0) {
        perror("clone");
        close(log_pipe[0]);
        close(log_pipe[1]);
        free(record->child_stack);
        free(record);
        free(cfg);
        fill_response(resp, 1, "clone failed");
        return 1;
    }

    close(log_pipe[1]);
    free(cfg);

    record->host_pid = pid;
    record->state = CONTAINER_RUNNING;
    copy_cstr(record->termination_reason, sizeof(record->termination_reason), "running");

    producer = calloc(1, sizeof(*producer));
    if (!producer) {
        kill(pid, SIGKILL);
        fill_response(resp, 1, "failed to allocate producer thread");
        return 1;
    }
    producer->ctx = ctx;
    producer->record = record;

    if (pthread_create(&record->producer_thread, NULL, producer_thread, producer) != 0) {
        kill(pid, SIGKILL);
        free(producer);
        fill_response(resp, 1, "failed to start producer thread");
        return 1;
    }
    record->producer_started = 1;

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0 &&
        register_with_monitor(ctx->monitor_fd,
                              record->id,
                              record->host_pid,
                              record->soft_limit_bytes,
                              record->hard_limit_bytes) < 0) {
        fprintf(stderr,
                "warning: failed to register container %s with monitor: %s\n",
                record->id,
                strerror(errno));
    }

    fill_response(resp, 0, "container accepted by supervisor");
    return 0;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *rec;

        pthread_mutex_lock(&ctx->metadata_lock);
        rec = ctx->containers;
        while (rec && rec->host_pid != pid)
            rec = rec->next;

        if (rec) {
            rec->finished = 1;
            if (WIFEXITED(status)) {
                rec->exit_code = WEXITSTATUS(status);
                rec->exit_signal = 0;
                rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
                if (rec->stop_requested)
                    copy_cstr(rec->termination_reason, sizeof(rec->termination_reason), "manual_stop");
                else
                    copy_cstr(rec->termination_reason, sizeof(rec->termination_reason), "normal_exit");
            } else if (WIFSIGNALED(status)) {
                rec->exit_code = 128 + WTERMSIG(status);
                rec->exit_signal = WTERMSIG(status);
                rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_KILLED;
                if (rec->stop_requested) {
                    copy_cstr(rec->termination_reason, sizeof(rec->termination_reason), "manual_stop");
                } else if (WTERMSIG(status) == SIGKILL) {
                    copy_cstr(rec->termination_reason,
                              sizeof(rec->termination_reason),
                              "hard_limit_killed");
                } else {
                    copy_cstr(rec->termination_reason, sizeof(rec->termination_reason), "signaled");
                }
            }

            if (ctx->monitor_fd >= 0)
                unregister_from_monitor(ctx->monitor_fd, rec->id, rec->host_pid);

            pthread_cond_broadcast(&ctx->state_changed);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

static void stop_all_live_containers(supervisor_ctx_t *ctx)
{
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (cur = ctx->containers; cur; cur = cur->next) {
        if (container_is_live(cur)) {
            cur->stop_requested = 1;
            kill(cur->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void join_container_threads_and_free(supervisor_ctx_t *ctx)
{
    container_record_t *cur;
    container_record_t *next;

    cur = ctx->containers;
    while (cur) {
        next = cur->next;

        if (cur->producer_started)
            pthread_join(cur->producer_thread, NULL);
        free(cur->child_stack);
        free(cur);
        cur = next;
    }

    ctx->containers = NULL;
}

static void handle_start_or_run(supervisor_ctx_t *ctx,
                                int client_fd,
                                const control_request_t *req,
                                int wait_for_exit)
{
    control_response_t resp;
    container_record_t *record = NULL;

    if (launch_container(ctx, req, &resp) != 0) {
        write_full(client_fd, &resp, sizeof(resp));
        return;
    }

    write_full(client_fd, &resp, sizeof(resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_locked(ctx, req->container_id);

    if (wait_for_exit && record) {
        while (!record->finished)
            pthread_cond_wait(&ctx->state_changed, &ctx->metadata_lock);

        if (record->exit_signal != 0) {
            dprintf(client_fd,
                    "container %s exited with signal %d (%s)\n",
                    record->id,
                    record->exit_signal,
                    record->termination_reason);
            pthread_mutex_unlock(&ctx->metadata_lock);
            return;
        }

        dprintf(client_fd,
                "container %s exited with code %d (%s)\n",
                record->id,
                record->exit_code,
                record->termination_reason);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void handle_ps(supervisor_ctx_t *ctx, int client_fd)
{
    control_response_t resp;
    container_record_t *cur;

    fill_response(&resp, 0, "tracked containers");
    write_full(client_fd, &resp, sizeof(resp));

    pthread_mutex_lock(&ctx->metadata_lock);
    if (!ctx->containers) {
        dprintf(client_fd, "no containers tracked\n");
    } else {
        for (cur = ctx->containers; cur; cur = cur->next) {
            char line[1024];

            format_container_line(cur, line, sizeof(line));
            write_full(client_fd, line, strlen(line));
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void handle_logs(supervisor_ctx_t *ctx, int client_fd, const control_request_t *req)
{
    control_response_t resp;
    container_record_t *record;
    char path[PATH_MAX];

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_locked(ctx, req->container_id);
    if (record)
        copy_cstr(path, sizeof(path), record->log_path);
    else
        path[0] = '\0';
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!record) {
        fill_response(&resp, 1, "unknown container id");
        write_full(client_fd, &resp, sizeof(resp));
        return;
    }

    fill_response(&resp, 0, "container logs");
    write_full(client_fd, &resp, sizeof(resp));

    if (stream_file_to_fd(path, client_fd) < 0)
        dprintf(client_fd, "failed to read %s: %s\n", path, strerror(errno));
}

static void handle_stop(supervisor_ctx_t *ctx, int client_fd, const control_request_t *req)
{
    control_response_t resp;
    container_record_t *record;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_locked(ctx, req->container_id);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        fill_response(&resp, 1, "unknown container id");
        write_full(client_fd, &resp, sizeof(resp));
        return;
    }

    if (!container_is_live(record)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        fill_response(&resp, 1, "container is not running");
        write_full(client_fd, &resp, sizeof(resp));
        return;
    }

    record->stop_requested = 1;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (kill(record->host_pid, SIGTERM) < 0) {
        fill_response(&resp, 1, "failed to signal container");
        write_full(client_fd, &resp, sizeof(resp));
        return;
    }

    fill_response(&resp, 0, "stop requested");
    write_full(client_fd, &resp, sizeof(resp));
}

static void *request_thread(void *arg)
{
    request_thread_arg_t *thread_arg = (request_thread_arg_t *)arg;
    supervisor_ctx_t *ctx = thread_arg->ctx;
    int client_fd = thread_arg->client_fd;
    control_request_t req = thread_arg->req;

    switch (req.kind) {
    case CMD_START:
        handle_start_or_run(ctx, client_fd, &req, 0);
        break;
    case CMD_RUN:
        handle_start_or_run(ctx, client_fd, &req, 1);
        break;
    case CMD_PS:
        handle_ps(ctx, client_fd);
        break;
    case CMD_LOGS:
        handle_logs(ctx, client_fd, &req);
        break;
    case CMD_STOP:
        handle_stop(ctx, client_fd, &req);
        break;
    default: {
        control_response_t resp;

        fill_response(&resp, 1, "unsupported command");
        write_full(client_fd, &resp, sizeof(resp));
        break;
    }
    }

    close(client_fd);
    free(thread_arg);
    return NULL;
}

static int setup_control_socket(void)
{
    int fd;
    struct sockaddr_un addr;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    unlink(CONTROL_PATH);

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, 16) < 0) {
        close(fd);
        unlink(CONTROL_PATH);
        return -1;
    }

    return fd;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = pthread_cond_init(&ctx.state_changed, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_cond_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_cond_destroy(&ctx.state_changed);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    g_supervisor_ctx = &ctx;

    if (install_supervisor_signals() < 0)
        perror("sigaction");

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr,
                "warning: could not open /dev/container_monitor: %s\n",
                strerror(errno));
    }

    ctx.server_fd = setup_control_socket();
    if (ctx.server_fd < 0) {
        perror("setup control socket");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.state_changed);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.state_changed);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    fprintf(stdout, "Supervisor listening on %s with base-rootfs %s\n", CONTROL_PATH, rootfs);

    while (!ctx.should_stop && !g_supervisor_stop) {
        fd_set readfds;
        struct timeval tv;
        int sel;

        reap_children(&ctx);

        FD_ZERO(&readfds);
        FD_SET(ctx.server_fd, &readfds);
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        sel = select(ctx.server_fd + 1, &readfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }
        if (sel == 0)
            continue;

        if (FD_ISSET(ctx.server_fd, &readfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0) {
                control_request_t req;
                ssize_t n = read_full(client_fd, &req, sizeof(req));

                if (n == (ssize_t)sizeof(req)) {
                    request_thread_arg_t *thread_arg = calloc(1, sizeof(*thread_arg));
                    pthread_t tid;

                    if (!thread_arg) {
                        control_response_t resp;

                        fill_response(&resp, 1, "out of memory");
                        write_full(client_fd, &resp, sizeof(resp));
                        close(client_fd);
                    } else {
                        thread_arg->ctx = &ctx;
                        thread_arg->client_fd = client_fd;
                        thread_arg->req = req;

                        if (pthread_create(&tid, NULL, request_thread, thread_arg) == 0) {
                            pthread_detach(tid);
                        } else {
                            control_response_t resp;

                            fill_response(&resp, 1, "failed to create request thread");
                            write_full(client_fd, &resp, sizeof(resp));
                            close(client_fd);
                            free(thread_arg);
                        }
                    }
                } else {
                    close(client_fd);
                }
            }
        }
    }

    stop_all_live_containers(&ctx);

    for (;;) {
        int running = 0;
        container_record_t *cur;

        reap_children(&ctx);
        pthread_mutex_lock(&ctx.metadata_lock);
        for (cur = ctx.containers; cur; cur = cur->next) {
            if (container_is_live(cur)) {
                running = 1;
                break;
            }
        }
        pthread_mutex_unlock(&ctx.metadata_lock);

        if (!running)
            break;

        usleep(100000);
    }

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    join_container_threads_and_free(&ctx);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_cond_destroy(&ctx.state_changed);
    pthread_mutex_destroy(&ctx.metadata_lock);
    g_supervisor_ctx = NULL;
    return 0;
}

static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;
    char buf[LOG_CHUNK_SIZE];
    ssize_t n;
    int forwarded = 0;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_cstr(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write_full(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(fd);
        return 1;
    }

    if (read_full(fd, &resp, sizeof(resp)) < (ssize_t)sizeof(resp)) {
        perror("read");
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0') {
        FILE *stream = resp.status == 0 ? stdout : stderr;

        fprintf(stream, "%s\n", resp.message);
    }

    for (;;) {
        n = read(fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR) {
                if (req->kind == CMD_RUN && g_client_forward_stop && !forwarded) {
                    if (send_stop_request_for_id(g_client_run_id) == 0)
                        forwarded = 1;
                    g_client_forward_stop = 0;
                    continue;
                }
                continue;
            }
            perror("read");
            close(fd);
            return 1;
        }
        if (n == 0)
            break;
        if (write_full(STDOUT_FILENO, buf, (size_t)n) < 0) {
            perror("stdout");
            close(fd);
            return 1;
        }
    }

    close(fd);
    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    struct sigaction old_int;
    struct sigaction old_term;
    int rc;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);
    copy_cstr(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_cstr(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    memset(g_client_run_id, 0, sizeof(g_client_run_id));
    copy_cstr(g_client_run_id, sizeof(g_client_run_id), req.container_id);
    g_client_forward_stop = 0;

    if (install_run_client_signals(&old_int, &old_term) < 0) {
        perror("sigaction");
        return 1;
    }

    rc = send_control_request(&req);
    restore_run_client_signals(&old_int, &old_term);
    memset(g_client_run_id, 0, sizeof(g_client_run_id));
    g_client_forward_stop = 0;

    return rc;
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_cstr(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}

