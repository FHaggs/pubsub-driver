# PubSub Kernel Driver

This project provides a simple Linux kernel module that implements a basic publish/subscribe communication model through a character device and procfs/sysfs interfaces.

It was built for educational purposes, with the goal of exploring Linux kernel development concepts such as character devices, procfs, sysfs, linked lists, synchronization primitives, dynamic memory management, and per-process state handling inside the kernel.

## Overview

The driver creates a publish/subscribe broker inside the kernel. User-space processes interact with it through:

- `/dev/pubsub` for sending commands and reading messages
- `/proc/pubsub` for runtime statistics
- `/sys/pubsub/<topic>/max_subscribers` for per-topic configuration

Processes can subscribe to topics, publish messages, choose which topic to fetch from, and read messages that were published to the topics they subscribed to.

## Main Features

- Dynamic topic creation on first subscription
- Topic removal when the last subscriber unsubscribes
- Per-topic circular buffer for messages
- Per-process subscription tracking
- Maximum number of topics controlled by a module parameter
- Maximum number of subscribers per topic controlled through sysfs
- Message statistics exposed through procfs

## How It Works

### 1. Character Device

The driver creates the device:

- `/dev/pubsub`

This is the main interface used by user-space applications.

Commands are written to the device as text strings. Examples:

- `subscribe news`
- `unsubscribe news`
- `publish news hello world`
- `fetch news`

Messages are read back using the regular `read()` system call on the same device.

### 2. Topics

A topic is created automatically when a process subscribes to a topic that does not exist yet.

Each topic contains:

- a name
- a circular message buffer
- a list of subscribers
- a message counter
- a configurable subscriber limit

When no subscribers remain on a topic, the topic is destroyed and removed from the broker.

### 3. Subscribers and Listeners

Each process that opens `/dev/pubsub` gets its own internal listener state.

That state tracks:

- the process PID
- the list of subscribed topics
- the currently selected topic for reads

A process must first subscribe to a topic before it can publish to it or fetch messages from it.

### 4. Publishing Messages

When a message is published:

- the driver finds the target topic
- it checks whether the process is subscribed to that topic
- it stores the message in the topic buffer
- it increments the topic message counter

Messages are stored in a framed format inside the circular buffer.

### 5. Reading Messages

A process reads from the currently selected topic.

The selected topic is changed using:

- `fetch <topic>`

A subsequent `read()` on `/dev/pubsub` returns the next unread message for that process on that topic.

Each subscriber has its own read offset, so different subscribers can consume messages independently.

## Procfs Interface

The driver creates:

- `/proc/pubsub`

This file shows how many messages have been published to each currently existing topic since the module was loaded.

Example output:

```text
news: 5
alerts: 8
```

Removed topics do not appear in this output.

## Sysfs Interface

The driver creates:

- `/sys/pubsub`

For each active topic, a directory is created:

- `/sys/pubsub/<topic>/`

Inside it, the driver exposes:

- `/sys/pubsub/<topic>/max_subscribers`

This file allows reading or changing the maximum number of subscribers allowed for that topic.

Example:

```sh
cat /sys/pubsub/news/max_subscribers
echo 20 > /sys/pubsub/news/max_subscribers
```

If the limit is reduced below the current number of subscribers, existing subscribers are not removed. The new limit only affects future subscription attempts.

## Module Parameter

The maximum number of topics is controlled by the module parameter:

- `max_topics`

Example when loading the module:

```sh
insmod pubsub.ko max_topics=16
```

If the system tries to create a new topic after reaching this limit, the topic creation is ignored and the subscription fails.

## Educational Purpose

This driver is intended for educational purposes only.

It is not designed as a production-ready message broker. Its main purpose is to help students and developers understand:

- Linux kernel modules
- character device drivers
- procfs and sysfs integration
- kernel synchronization with mutexes and spinlocks
- linked list management in kernel space
- per-process state in device drivers
- memory allocation and cleanup paths

## User-Space Client

A simple client application is available in the project to interact with the driver from user space.

```sh
write subscribe news
write fetch news
write publish news hello world
read
```

Expected behavior:

- the process subscribes to `news`
- it selects `news` as the current topic
- it publishes `hello world`
- it reads the next unread message from `news`

## Notes

This project focuses on clarity and learning rather than performance or hardening. Some parts are intentionally simple to make the code easier to study and extend.
