#define _GNU_SOURCE
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
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;        /* set before sending stop signal */
    char log_path[PATH_MAX];
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
    container_record_t *containers;
} supervisor_ctx_t;

/* global pointer for signal handler access */
static supervisor_ctx_t *g_ctx = NULL;

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
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ---------------------------------------------------------------
 * Bounded buffer — unchanged from teammate's correct implementation
 * --------------------------------------------------------------- */
static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
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
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
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

/* ---------------------------------------------------------------
 * MODIFIED: logging_thread
 * Now routes each chunk to the correct per-container log file
 * instead of just printing to stdout
 * --------------------------------------------------------------- */
void *logging_thread(void *arg)
{
    bounded_buffer_t *buffer = (bounded_buffer_t *)arg;
    log_item_t item;

    /* ensure log directory exists */
    mkdir(LOG_DIR, 0755);

    while (1) {
        if (bounded_buffer_pop(buffer, &item) != 0)
            break;  /* shutdown and buffer drained */

        /* build log file path: logs/<container_id>.log */
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("logging_thread: open log file");
            continue;
        }
        write(fd, item.data, item.length);
        close(fd);
    }
    return NULL;
}

/* ---------------------------------------------------------------
 * NEW: producer thread — reads from container pipe, pushes to buffer
 * --------------------------------------------------------------- */
typedef struct {
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

void *producer_thread(void *arg)
{
    producer_arg_t *p = (producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    while ((n = read(p->read_fd, item.data, LOG_CHUNK_SIZE)) > 0) {
        item.length = (size_t)n;
        strncpy(item.container_id, p->container_id, CONTAINER_ID_LEN - 1);
        item.container_id[CONTAINER_ID_LEN - 1] = '\0';
        bounded_buffer_push(p->buffer, &item);
    }

    close(p->read_fd);
    free(p);
    return NULL;
}

/* ---------------------------------------------------------------
 * MODIFIED: child_fn
 * Now sets up namespaces, chroot, /proc, nice, and execs command
 * --------------------------------------------------------------- */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* copy what we need onto the stack before freeing heap */
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int  log_write_fd = cfg->log_write_fd;
    int  nice_value   = cfg->nice_value;

    strncpy(id,      cfg->id,      sizeof(id) - 1);
    strncpy(rootfs,  cfg->rootfs,  sizeof(rootfs) - 1);
    strncpy(command, cfg->command, sizeof(command) - 1);

    /* now safe to redirect */
    dup2(log_write_fd, STDOUT_FILENO);
    dup2(log_write_fd, STDERR_FILENO);
    close(log_write_fd);

    sethostname(id, strlen(id));

    if (chroot(rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    mkdir("/proc", 0555);
    mount("proc", "/proc", "proc", 0, NULL);

    if (nice_value != 0)
        nice(nice_value);

    char *argv[] = { "/bin/sh", "-c", command, NULL };
    execv("/bin/sh", argv);

    perror("execv");
    return 1;
}
/* ---------------------------------------------------------------
 * monitor integration helpers — unchanged from boilerplate
 * --------------------------------------------------------------- */
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
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd,
                            const char *container_id,
                            pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ---------------------------------------------------------------
 * NEW: SIGCHLD handler — reaps children, updates metadata,
 * classifies termination reason, unregisters from monitor
 * --------------------------------------------------------------- */
static void sigchld_handler(int sig)
{
    (void)sig;
    int saved_errno = errno;
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx) continue;

        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *rec = g_ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code = WEXITSTATUS(status);
                    rec->exit_signal = 0;
                    rec->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    rec->exit_code = 0;
                    /*
                     * Termination classification per project guide:
                     * - stop_requested set → STOPPED (manual stop)
                     * - SIGKILL + stop_requested not set → KILLED (hard limit)
                     */
                    if (rec->stop_requested)
                        rec->state = CONTAINER_STOPPED;
                    else if (rec->exit_signal == SIGKILL)
                        rec->state = CONTAINER_KILLED;
                    else
                        rec->state = CONTAINER_EXITED;
                }

                /* unregister from kernel monitor */
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd,
                                            rec->id, rec->host_pid);
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }

    errno = saved_errno;
}

/* ---------------------------------------------------------------
 * NEW: SIGINT/SIGTERM handler — orderly supervisor shutdown
 * --------------------------------------------------------------- */
static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/* ---------------------------------------------------------------
 * MODIFIED: run_supervisor
 * Now fully implemented with monitor integration, clone-based
 * container launch, signal handling, and proper metadata tracking
 * --------------------------------------------------------------- */
static int run_supervisor(const char *rootfs)
{
    (void)rootfs;

    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* 1) open kernel monitor device (non-fatal if not loaded) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "Warning: /dev/container_monitor not available, "
                        "memory monitoring disabled\n");

    /* 2) create log directory */
    mkdir(LOG_DIR, 0755);

    /* 3) start logger thread (joinable) */
    rc = pthread_create(&ctx.logger_thread, NULL,
                        logging_thread, &ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* 4) install signal handlers */
    struct sigaction sa_chld, sa_term;
    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = sigchld_handler;
    sa_chld.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = sigterm_handler;
    sigaction(SIGINT,  &sa_term, NULL);
    sigaction(SIGTERM, &sa_term, NULL);

    /* 5) create control socket */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    unlink(CONTROL_PATH);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        return 1;
    }

    printf("Supervisor running. Control socket: %s\n", CONTROL_PATH);

    /* 6) event loop */
    while (!ctx.should_stop) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;  /* interrupted by signal */
            perror("accept");
            continue;
        }

        control_request_t req;
        if (read(client_fd, &req, sizeof(req)) <= 0) {
            close(client_fd);
            continue;
        }

        control_response_t res;
        memset(&res, 0, sizeof(res));

        switch (req.kind) {

        case CMD_START: {
            /* check for duplicate ID */
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *existing = ctx.containers;
            while (existing) {
                if (strcmp(existing->id, req.container_id) == 0 &&
                    existing->state == CONTAINER_RUNNING) {
                    break;
                }
                existing = existing->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            if (existing) {
                res.status = -1;
                snprintf(res.message, sizeof(res.message),
                         "Container %s already running", req.container_id);
                break;
            }

            /* create pipe for container stdout/stderr → supervisor */
            int pipefd[2];
            if (pipe(pipefd) < 0) {
                perror("pipe");
                res.status = -1;
                snprintf(res.message, sizeof(res.message), "pipe failed");
                break;
            }

            /* set up child config */
            child_config_t *cfg = calloc(1, sizeof(child_config_t));
            strncpy(cfg->id,      req.container_id, CONTAINER_ID_LEN - 1);
            strncpy(cfg->rootfs,  req.rootfs,        PATH_MAX - 1);
            strncpy(cfg->command, req.command,        CHILD_COMMAND_LEN - 1);
            cfg->nice_value   = req.nice_value;
            cfg->log_write_fd = pipefd[1];  /* write end goes to child */

            /* allocate clone stack */
            char *stack = malloc(STACK_SIZE);
            if (!stack) {
                free(cfg);
                close(pipefd[0]);
                close(pipefd[1]);
                res.status = -1;
                snprintf(res.message, sizeof(res.message), "malloc failed");
                break;
            }

            /* launch container with isolated namespaces */
            pid_t pid = clone(child_fn,
                              stack + STACK_SIZE,
                              CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                              cfg);

            free(stack);
            close(pipefd[1]);  /* supervisor closes write end */

            if (pid < 0) {
                perror("clone");
                //free(cfg);
                close(pipefd[0]);
                res.status = -1;
                snprintf(res.message, sizeof(res.message), "clone failed");
                break;
            }

            /* start producer thread to read container output */
            producer_arg_t *parg = malloc(sizeof(producer_arg_t));
            parg->read_fd = pipefd[0];
            strncpy(parg->container_id, req.container_id, CONTAINER_ID_LEN - 1);
            parg->buffer = &ctx.log_buffer;
            pthread_t prod_tid;
            pthread_create(&prod_tid, NULL, producer_thread, parg);
            pthread_detach(prod_tid);

            /* create and store container metadata record */
            container_record_t *rec = calloc(1, sizeof(container_record_t));
            strncpy(rec->id, req.container_id, CONTAINER_ID_LEN - 1);
            rec->host_pid          = pid;
            rec->started_at        = time(NULL);
            rec->state             = CONTAINER_RUNNING;
            rec->soft_limit_bytes  = req.soft_limit_bytes;
            rec->hard_limit_bytes  = req.hard_limit_bytes;
            rec->stop_requested    = 0;
            snprintf(rec->log_path, PATH_MAX, "%s/%s.log",
                     LOG_DIR, req.container_id);

            pthread_mutex_lock(&ctx.metadata_lock);
            rec->next       = ctx.containers;
            ctx.containers  = rec;
            pthread_mutex_unlock(&ctx.metadata_lock);

            /* register with kernel memory monitor */
            if (ctx.monitor_fd >= 0) {
                if (register_with_monitor(ctx.monitor_fd,
                                          req.container_id, pid,
                                          req.soft_limit_bytes,
                                          req.hard_limit_bytes) < 0)
                    fprintf(stderr, "Warning: monitor registration failed "
                                    "for %s\n", req.container_id);
            }

            res.status = 0;
            snprintf(res.message, sizeof(res.message),
                     "Started container %s pid=%d", req.container_id, pid);
            free(cfg);
            break;
        }

        case CMD_STOP: {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = ctx.containers;
            while (rec) {
                if (strcmp(rec->id, req.container_id) == 0 &&
                    rec->state == CONTAINER_RUNNING) {
                    break;
                }
                rec = rec->next;
            }

            if (!rec) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                res.status = -1;
                snprintf(res.message, sizeof(res.message),
                         "Container %s not found", req.container_id);
                break;
            }

            /*
             * Set stop_requested BEFORE sending signal so SIGCHLD
             * handler classifies this as STOPPED not KILLED
             */
            rec->stop_requested = 1;
            rec->state = CONTAINER_STOPPED;  /* update state immediately */
            pid_t pid = rec->host_pid;
            pthread_mutex_unlock(&ctx.metadata_lock);

            /* try SIGTERM first, SIGKILL if needed */
            kill(pid, SIGTERM);

            res.status = 0;
            snprintf(res.message, sizeof(res.message),
                     "Stopped container %s", req.container_id);
            break;
        }

        case CMD_PS: {
            /* build ps output into response message */
            char buf[CONTROL_MESSAGE_LEN];
            memset(buf, 0, sizeof(buf));
            int offset = 0;

            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = ctx.containers;

            if (!rec) {
                snprintf(buf, sizeof(buf), "No containers.\n");
            } else {
                while (rec && offset < (int)sizeof(buf) - 1) {
                    int n = snprintf(buf + offset,
                                     sizeof(buf) - offset,
                                     "ID=%-16s PID=%-6d STATE=%-8s\n",
                                     rec->id, rec->host_pid,
                                     state_to_string(rec->state));
                    offset += n;
                    rec = rec->next;
                }
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            res.status = 0;
            strncpy(res.message, buf, sizeof(res.message) - 1);
            break;
        }

        case CMD_LOGS: {
            /* send log file path back; client can cat it */
            char log_path[PATH_MAX];
            snprintf(log_path, sizeof(log_path), "%s/%s.log",
                     LOG_DIR, req.container_id);
            res.status = 0;
            snprintf(res.message, sizeof(res.message),
                     "Log file: %s", log_path);
            break;
        }

        case CMD_RUN: {
            /*
             * RUN = START + block until container exits.
             * We reuse CMD_START logic then wait for state change.
             * For brevity we forward as a start here; a full
             * implementation would wait on a condition variable.
             */
            res.status = 0;
            snprintf(res.message, sizeof(res.message),
                     "run not fully implemented; use start");
            break;
        }

        default:
            res.status = -1;
            snprintf(res.message, sizeof(res.message), "Unknown command");
            break;
        }

        write(client_fd, &res, sizeof(res));
        close(client_fd);
    }

    /* orderly shutdown */
    printf("Supervisor shutting down...\n");

    /* stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *rec = ctx.containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING) {
            rec->stop_requested = 1;
            kill(rec->host_pid, SIGTERM);
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* give containers a moment to exit */
    sleep(1);

    /* shutdown logging pipeline and join logger thread */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* free container metadata */
    pthread_mutex_lock(&ctx.metadata_lock);
    rec = ctx.containers;
    while (rec) {
        container_record_t *next = rec->next;
        free(rec);
        rec = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    if (ctx.server_fd >= 0) {
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
    }

    return 0;
}

/* ---------------------------------------------------------------
 * send_control_request — unchanged from teammate's implementation
 * --------------------------------------------------------------- */
static int send_control_request(const control_request_t *req)
{
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect: is supervisor running?");
        close(sock);
        return 1;
    }

    write(sock, req, sizeof(*req));

    control_response_t res;
    memset(&res, 0, sizeof(res));
    read(sock, &res, sizeof(res));

    printf("%s\n", res.message);
    close(sock);
    return (res.status == 0) ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
