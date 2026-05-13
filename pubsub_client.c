#include <fcntl.h>
#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#define DEVICE_PATH "/dev/pubsub"
#define LINE_BUF_SIZE 512
#define READ_BUF_SIZE 2048

static void print_help(void)
{
    printf("Commands:\n");
    printf("  write <device command>  Send raw command to /dev/pubsub\n");
    printf("  read                    Read one message from /dev/pubsub\n");
    printf("  help                    Show this help\n");
    printf("  quit | exit             Leave the client\n");
    printf("\nExamples:\n");
    printf("  write subscribe news\n");
    printf("  write fetch news\n");
    printf("  write publish news hello world\n");
    printf("  read\n");
}

static void trim_trailing_newline(char *s)
{
    size_t len;

    if (!s) {
        return;
    }

    len = strlen(s);
    while (len > 0 && (s[len - 1] == '\n' || s[len - 1] == '\r')) {
        s[len - 1] = '\0';
        len--;
    }
}

int main(void)
{
    int fd;
    char line[LINE_BUF_SIZE];
    char read_buf[READ_BUF_SIZE];
    ssize_t n;
    char *cmd;
    const char *payload;
    size_t payload_len;

    fd = open(DEVICE_PATH, O_RDWR);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    printf("pubsub shell connected to %s\n", DEVICE_PATH);
    print_help();

    for (;;) {
        printf("pubsub> ");
        fflush(stdout);

        if (!fgets(line, sizeof(line), stdin)) {
            printf("\n");
            break;
        }

        trim_trailing_newline(line);
        cmd = line;
        while (*cmd && isspace((unsigned char)*cmd)) {
            cmd++;
        }

        if (*cmd == '\0') {
            continue;
        }

        if (strcmp(cmd, "help") == 0) {
            print_help();
            continue;
        }

        if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
            break;
        }

        if (strcmp(cmd, "read") == 0) {
            n = read(fd, read_buf, sizeof(read_buf));
            if (n < 0) {
                printf("read error: %s (errno=%d)\n", strerror(errno), errno);
                continue;
            }

            printf("read %zd bytes: ", n);
            if (n > 0) {
                fwrite(read_buf, 1, (size_t)n, stdout);
            }
            printf("\n");
            continue;
        }

        if (strncmp(cmd, "write", 5) == 0 &&
            (cmd[5] == '\0' || isspace((unsigned char)cmd[5]))) {
            payload = cmd + 5;
            while (*payload && isspace((unsigned char)*payload)) {
                payload++;
            }

            if (*payload == '\0') {
                printf("usage: write <device command>\n");
                continue;
            }

            payload_len = strlen(payload);
            n = write(fd, payload, payload_len);
            if (n < 0) {
                printf("write error: %s (errno=%d)\n", strerror(errno), errno);
                continue;
            }

            printf("wrote %zd bytes\n", n);
            continue;
        }

        printf("unknown command: %s\n", cmd);
        printf("type 'help' for usage\n");
    }

    close(fd);
    printf("bye\n");
    return 0;
}