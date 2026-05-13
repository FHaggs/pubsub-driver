#include <linux/module.h>
#include <linux/kernel.h>
#include <linux/fs.h>
#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/proc_fs.h>
#include <linux/seq_file.h>
#include <linux/uaccess.h>
#include <linux/mutex.h>
#include <linux/string.h>
#include <linux/sched.h>
#include <linux/slab.h>
#include <linux/list.h>
#include <linux/wait.h>

#define DEVICE_NAME "pubsub"
#define PROC_NAME "pubsub"
#define CLASS_NAME "meuclass"

#define TOPIC_NAME_SIZE 64
#define BUFFER_SIZE 1024
#define DEFAULT_MAX_SUBSCRIBERS 10

// Module parameters
static int max_topics = 32;
module_param(max_topics, int, 0444);
MODULE_PARM_DESC(max_topics, "Maximum number of topics (not modifiable at runtime)");

// Use mod probe to load this

// ============================================================

static struct kobject *pubsub_kobj;
static const struct attribute_group topic_attr_group;

// Shared state
// ============================================================

struct broker {
	struct mutex topics_lock;

	struct list_head topics;
	struct list_head listeners;
	
	int topic_count;
};

struct topic;

struct subscriber {
	pid_t pid;

	size_t read_offset;

	struct list_head list;
};

struct listener_subscription {
	struct topic *topic;
	struct subscriber *subscriber;

	struct list_head list;
};

struct listener_state {
	pid_t pid;
	struct topic *current_topic;
	struct mutex lock;

	struct list_head subscriptions;
	struct list_head list;
};

struct message_record {
	size_t payload_len;
};

struct topic {
	char *buffer;
	char name[TOPIC_NAME_SIZE];

	size_t capacity;

	size_t write_pos;
	spinlock_t write_pos_lock;

	struct mutex write_lock;

	wait_queue_head_t readers_wait;

	struct list_head subscribers;
	
	size_t message_count;
	int max_subscribers;
	struct kobject *kobj;

	struct list_head list;
};

static struct broker global_broker;

static struct topic *find_topic_by_name(const char *topic_name)
{
	struct topic *topic;

	list_for_each_entry(topic, &global_broker.topics, list) {
		if (strcmp(topic->name, topic_name) == 0) {
			return topic;
		}
	}

	return NULL;
}

static struct listener_subscription *find_listener_subscription(
	struct listener_state *listener,
	const struct topic *topic)
{
	struct listener_subscription *subscription;

	list_for_each_entry(subscription, &listener->subscriptions, list) {
		if (subscription->topic == topic) {
			return subscription;
		}
	}

	return NULL;
}

static void copy_from_topic_buffer(const struct topic *topic, size_t offset,
				   void *dst, size_t len)
{
	size_t buffer_idx;
	size_t first_chunk;
	size_t second_chunk;

	buffer_idx = offset % topic->capacity;
	first_chunk = min(len, topic->capacity - buffer_idx);
	second_chunk = len - first_chunk;

	memcpy(dst, &topic->buffer[buffer_idx], first_chunk);
	if (second_chunk > 0) {
		memcpy((char *)dst + first_chunk, &topic->buffer[0], second_chunk);
	}
}

static int add_subscription_to_topic(struct listener_state *listener,
				     struct topic *topic)
{
	struct listener_subscription *subscription;
	struct subscriber *subscriber;
	int subscriber_count = 0;
	struct subscriber *s;

	if (find_listener_subscription(listener, topic)) {
		return 0;
	}

	// Check if adding a new subscriber would exceed max_subscribers
	list_for_each_entry(s, &topic->subscribers, list) {
		subscriber_count++;
	}

	if (subscriber_count >= topic->max_subscribers) {
		pr_warn("pubsub: Max subscribers limit reached for topic '%s'\n", topic->name);
		return -ENOSPC;
	}

	subscriber = kmalloc(sizeof(*subscriber), GFP_KERNEL);
	if (!subscriber) {
		return -ENOMEM;
	}

	subscription = kmalloc(sizeof(*subscription), GFP_KERNEL);
	if (!subscription) {
		kfree(subscriber);
		return -ENOMEM;
	}

	subscriber->pid = listener->pid;
	subscriber->read_offset = topic->write_pos;
	list_add_tail(&subscriber->list, &topic->subscribers);

	subscription->topic = topic;
	subscription->subscriber = subscriber;
	list_add_tail(&subscription->list, &listener->subscriptions);

	if (!listener->current_topic) {
		listener->current_topic = topic;
	}

	return 0;
}

static void remove_subscription_locked(struct listener_state *listener,
				       struct listener_subscription *subscription)
{
	if (listener->current_topic == subscription->topic) {
		listener->current_topic = NULL;
	}

	list_del(&subscription->subscriber->list);
	kfree(subscription->subscriber);
	list_del(&subscription->list);
	kfree(subscription);

	if (!listener->current_topic && !list_empty(&listener->subscriptions)) {
		subscription = list_first_entry(&listener->subscriptions,
					       struct listener_subscription, list);
		listener->current_topic = subscription->topic;
	}
}

static int publish_message_to_topic(struct topic *topic, const char *message, size_t message_len)
{
	struct message_record header;
	size_t record_len;
	size_t header_write_idx;
	size_t first_chunk;
	size_t second_chunk;
	char *payload_dst;
	size_t payload_write_idx;
	struct subscriber *s;
	size_t min_read_offset;
	size_t effective_used;
	size_t effective_free;
	size_t current_write_pos;

	// Snapshot write_pos and check buffer space (fast path with spinlock)
	spin_lock(&topic->write_pos_lock);
	current_write_pos = topic->write_pos;
	min_read_offset = current_write_pos;
	list_for_each_entry (s, &topic->subscribers, list) {
		if (s->read_offset < min_read_offset) {
			min_read_offset = s->read_offset;
		}
	}
	spin_unlock(&topic->write_pos_lock);

	// Check if we have space to write the new message without overwriting unread messages
	header.payload_len = message_len;
	record_len = sizeof(header) + message_len;
	if (record_len > topic->capacity) {
		pr_err("Message too large for topic buffer\n");
		return -EINVAL; // Or some error code indicating invalid argument
	}
	// Since offsets are logically monotonic growing
	effective_used = current_write_pos - min_read_offset;
	effective_free = topic->capacity - effective_used;

	if (record_len > effective_free) {
		pr_err("Not enough space to write message without overwriting unread messages\n");
		return -EAGAIN; // Or some error code indicating buffer full
	}

	// Write a framed record as [length][payload] into the circular buffer.
	// Hold write_lock only during actual buffer writes
	mutex_lock(&topic->write_lock);
	header_write_idx = current_write_pos % topic->capacity;
	first_chunk = min(sizeof(header), topic->capacity - header_write_idx);
	second_chunk = sizeof(header) - first_chunk;

	memcpy(&topic->buffer[header_write_idx], &header, first_chunk);
	if (second_chunk > 0) {
		memcpy(&topic->buffer[0], ((char *)&header) + first_chunk, second_chunk);
	}

	payload_dst = topic->buffer;
	payload_write_idx = (current_write_pos + sizeof(header)) % topic->capacity;
	first_chunk = min(message_len, topic->capacity - payload_write_idx);
	second_chunk = message_len - first_chunk;

	memcpy(&payload_dst[payload_write_idx], message, first_chunk);
	if (second_chunk > 0) {
		memcpy(&payload_dst[0], message + first_chunk, second_chunk);
	}
	mutex_unlock(&topic->write_lock);

	// Update write_pos atomically (write_pos grows monotonically, NEVER wraps around)
	spin_lock(&topic->write_pos_lock);
	topic->write_pos += record_len;
	topic->message_count++;
	spin_unlock(&topic->write_pos_lock);

	return 0;
}
// ============================================================
// BROKER MANAGEMENT PATTERNS
// ============================================================
//
// HOW TO ADD A NEW TOPIC:
// 1. Allocate a new struct topic using kmalloc()
// 2. Initialize all fields (buffer, capacity, write_pos, etc.)
// 3. Initialize the mutex: mutex_init(&new_topic->write_lock)
// 4. Initialize wait queue: init_waitqueue_head(&new_topic->readers_wait)
// 5. Initialize subscribers list: INIT_LIST_HEAD(&new_topic->subscribers)
// 6. Lock the broker: mutex_lock(&global_broker.topics_lock)
// 7. Add to broker's list: list_add_tail(&new_topic->list, &global_broker.topics)
// 8. Unlock: mutex_unlock(&global_broker.topics_lock)
//
// HOW TO GET AND FREE LOCKS:
// - Use mutex_lock(&global_broker.topics_lock) before accessing/modifying the topics list
// - Use mutex_unlock(&global_broker.topics_lock) after you're done
// - For individual topics, use mutex_lock(&topic->write_lock) when modifying write_pos/buffer
// - Always unlock in error paths too! Consider using cleanup labels (goto cleanup;)
// - DO NOT hold topics_lock while writing to a topic - could cause deadlock
//
// LOCKING RULES:
// - topics_lock: protects the global topics list and broker state
// - write_lock (per topic): protects individual topic's buffer and write_pos
// - NEVER lock topics_lock while holding write_lock (could deadlock)
// ============================================================

// ============================================================
// Char device
// ============================================================

static dev_t dev_num;
static struct cdev my_cdev;
static struct class *my_class;

// ------------------------------------------------------------
// open
// ------------------------------------------------------------

static int my_open(struct inode *inode, struct file *file)
{
	struct listener_state *listener;

	listener = kzalloc(sizeof(*listener), GFP_KERNEL);
	if (!listener) {
		return -ENOMEM;
	}

	listener->pid = current->pid;
	listener->current_topic = NULL;
	mutex_init(&listener->lock);
	INIT_LIST_HEAD(&listener->subscriptions);
	INIT_LIST_HEAD(&listener->list);
	mutex_lock(&global_broker.topics_lock);
	list_add_tail(&listener->list, &global_broker.listeners);
	mutex_unlock(&global_broker.topics_lock);
	file->private_data = listener;

	//pr_info("pubsub: device opened\n");
	return 0;
}

// ------------------------------------------------------------
// release
// ------------------------------------------------------------

static int my_release(struct inode *inode, struct file *file)
{
	struct listener_state *listener = file->private_data;
	struct listener_subscription *subscription;
	struct listener_subscription *tmp;

	if (listener) {
		mutex_lock(&listener->lock);
		mutex_lock(&global_broker.topics_lock);
		list_for_each_entry_safe(subscription, tmp, &listener->subscriptions, list) {
			remove_subscription_locked(listener, subscription);
		}
		list_del(&listener->list);
		mutex_unlock(&global_broker.topics_lock);
		mutex_unlock(&listener->lock);
		kfree(listener);
		file->private_data = NULL;
	}

	// pr_info("pubsub: device closed\n");
	return 0;
}

// ------------------------------------------------------------
// read
// ------------------------------------------------------------

static ssize_t my_read(struct file *file, char __user *user_buf, size_t count, loff_t *ppos)
{
	struct listener_state *listener = file->private_data;
	struct listener_subscription *subscription;
	struct topic *topic;
	struct subscriber *subscriber;
	struct message_record header;
	char *message;
	size_t next_read_offset;
	size_t current_write_pos;
	ssize_t ret;

	if (!listener) {
		return -EINVAL;
	}

	mutex_lock(&listener->lock);
	if (!listener->current_topic) {
		mutex_unlock(&listener->lock);
		return -ENODATA;
	}

	subscription = find_listener_subscription(listener, listener->current_topic);
	if (!subscription) {
		listener->current_topic = NULL;
		mutex_unlock(&listener->lock);
		return -ENODATA;
	}

	topic = subscription->topic;
	subscriber = subscription->subscriber;

	// Snapshot write_pos (fast spinlock, released immediately)
	spin_lock(&topic->write_pos_lock);
	current_write_pos = topic->write_pos;
	spin_unlock(&topic->write_pos_lock);

	// Check if there's anything to read
	if (subscriber->read_offset >= current_write_pos) {
		mutex_unlock(&listener->lock);
		return -EAGAIN;
	}

	// Read header from topic buffer (no locks held)
	copy_from_topic_buffer(topic, subscriber->read_offset, &header, sizeof(header));
	if (header.payload_len > topic->capacity - sizeof(header)) {
		mutex_unlock(&listener->lock);
		return -EIO;
	}

	if (count < header.payload_len) {
		mutex_unlock(&listener->lock);
		return -EMSGSIZE;
	}

	message = kmalloc(header.payload_len, GFP_KERNEL);
	if (!message) {
		mutex_unlock(&listener->lock);
		return -ENOMEM;
	}

	// Read payload from topic buffer (no locks held)
	copy_from_topic_buffer(topic, subscriber->read_offset + sizeof(header),
			       message, header.payload_len);
	next_read_offset = subscriber->read_offset + sizeof(header) + header.payload_len;

	// Copy to userspace (no locks held - allows concurrent readers)
	if (copy_to_user(user_buf, message, header.payload_len)) {
		mutex_unlock(&listener->lock);
		kfree(message);
		return -EFAULT;
	}

	// Only advance read_offset after successful copy_to_user
	subscriber->read_offset = next_read_offset;
	mutex_unlock(&listener->lock);
	ret = header.payload_len;
	kfree(message);
	return ret;
}

// ------------------------------------------------------------
// write
// ------------------------------------------------------------

static int publish_message(struct file *file, const char *topic_name,
			   const char *message)
{
	struct listener_state *listener = file->private_data;
	struct listener_subscription *subscription;
	struct topic *topic;
	size_t message_len;
	int ret;

	if (!listener) {
		return -EINVAL;
	}

	message_len = strlen(message);
	mutex_lock(&listener->lock);
	mutex_lock(&global_broker.topics_lock);
	topic = find_topic_by_name(topic_name);
	if (!topic) {
		ret = -ENOENT;
		goto out_unlock;
	}

	subscription = find_listener_subscription(listener, topic);
	if (!subscription) {
		ret = -EACCES;
		goto out_unlock;
	}

	mutex_unlock(&global_broker.topics_lock);
	mutex_unlock(&listener->lock);

	ret = publish_message_to_topic(topic, message, message_len);
	if (ret) {
		return ret;
	}

	wake_up_all(&topic->readers_wait);
	return 0;

out_unlock:
	mutex_unlock(&global_broker.topics_lock);
	mutex_unlock(&listener->lock);
	return ret;
}

static int subscribe_listener_to_topic(struct file *file, const char *topic_name)
{
	struct listener_state *listener = file->private_data;
	struct topic *topic;
	int ret;

	if (!listener) {
		return -EINVAL;
	}

	mutex_lock(&listener->lock);
	mutex_lock(&global_broker.topics_lock);
	topic = find_topic_by_name(topic_name);
	if (!topic) {
		// Check if we've reached max topics limit
		if (global_broker.topic_count >= max_topics) {
			pr_warn("pubsub: Max topics limit (%d) reached\n", max_topics);
			ret = -ENOSPC;
			goto out_unlock;
		}

		topic = kzalloc(sizeof(*topic), GFP_KERNEL);
		if (!topic) {
			ret = -ENOMEM;
			goto out_unlock;
		}

		topic->buffer = kmalloc(BUFFER_SIZE, GFP_KERNEL);
		if (!topic->buffer) {
			kfree(topic);
			ret = -ENOMEM;
			goto out_unlock;
		}

		if (strscpy(topic->name, topic_name, sizeof(topic->name)) < 0) {
			kfree(topic->buffer);
			kfree(topic);
			ret = -EINVAL;
			goto out_unlock;
		}

		topic->capacity = BUFFER_SIZE;
		topic->write_pos = 0;
		topic->message_count = 0;
		topic->max_subscribers = DEFAULT_MAX_SUBSCRIBERS;
		spin_lock_init(&topic->write_pos_lock);
		mutex_init(&topic->write_lock);
		init_waitqueue_head(&topic->readers_wait);
		INIT_LIST_HEAD(&topic->subscribers);
		
		/* Create /sys/.../<topic>/max_subscribers */
		topic->kobj = kobject_create_and_add(topic_name, pubsub_kobj);
		if (!topic->kobj) {
			ret = -ENOMEM;
			kfree(topic->buffer);
			kfree(topic);
			goto out_unlock;
		}

		ret = sysfs_create_group(topic->kobj, &topic_attr_group);
		if (ret) {
			kobject_put(topic->kobj);
			kfree(topic->buffer);
			kfree(topic);
			goto out_unlock;
		}

		list_add_tail(&topic->list, &global_broker.topics);
		global_broker.topic_count++;
	}

	ret = add_subscription_to_topic(listener, topic);

out_unlock:
	mutex_unlock(&global_broker.topics_lock);
	mutex_unlock(&listener->lock);
	return ret;
}

static int unsubscribe_listener_from_topic(struct file *file, const char *topic_name)
{
	struct listener_state *listener = file->private_data;
	struct topic *topic;
	struct listener_subscription *subscription;
	int ret;

	if (!listener) {
		return -EINVAL;
	}

	mutex_lock(&listener->lock);
	mutex_lock(&global_broker.topics_lock);
	topic = find_topic_by_name(topic_name);
	if (!topic) {
		ret = -ENOENT;
		goto out_unlock;
	}

	subscription = find_listener_subscription(listener, topic);
	if (!subscription) {
		ret = -ENOENT;
		goto out_unlock;
	}

	remove_subscription_locked(listener, subscription);

	// Check if there are any subscribers left for this topic
	if (list_empty(&topic->subscribers)) {
		// No more subscribers, destroy the topic
		list_del(&topic->list);
		sysfs_remove_group(topic->kobj, &topic_attr_group);
		kobject_put(topic->kobj);
		kfree(topic->buffer);
		kfree(topic);
		global_broker.topic_count--;
	}

	ret = 0;

out_unlock:
	mutex_unlock(&global_broker.topics_lock);
	mutex_unlock(&listener->lock);
	return ret;
}

static int fetch_messages(struct file *file, const char *topic_name)
{
	struct listener_state *listener = file->private_data;
	struct topic *topic;

	if (!listener) {
		return -EINVAL;
	}

	mutex_lock(&listener->lock);
	mutex_lock(&global_broker.topics_lock);
	topic = find_topic_by_name(topic_name);
	if (!topic) {
		mutex_unlock(&global_broker.topics_lock);
		mutex_unlock(&listener->lock);
		return -ENOENT;
	}

	if (!find_listener_subscription(listener, topic)) {
		mutex_unlock(&global_broker.topics_lock);
		mutex_unlock(&listener->lock);
		return -EACCES;
	}

	listener->current_topic = topic;
	mutex_unlock(&global_broker.topics_lock);
	mutex_unlock(&listener->lock);
	return 0;
}

static ssize_t write_cmd(struct file *file, const char __user *user_buf, size_t count)
{
	char cmd_buf[BUFFER_SIZE];
	size_t to_copy = min(count, (size_t)(BUFFER_SIZE - 1));
	int ret;

	if (copy_from_user(cmd_buf, user_buf, to_copy)) {
		return -EFAULT;
	}

	cmd_buf[to_copy] = '\0';

	// For now, we just log the command. In a real implementation,
	// you'd parse the command and perform actions like creating topics,
	// subscribing, publishing, etc.
	char *trimmed = strim(cmd_buf);
	// pr_info("Received command: %s\n", trimmed);
	// Parsing
	// Possible comands
	// subscribe <topic>
	// publish <topic> <message>
	// unsubscribe <topic>
	// fetch <topic>
	char *command;
	char *topic;
	char *message;
	command = strsep(&trimmed, " ");
	topic = strsep(&trimmed, " ");
	message = trimmed; // Whatever is left is the message (can contain spaces)

	if (!command)
		return -EINVAL;

	// 3. Dispatch (Pattern Matching-ish)
	if (strcmp(command, "subscribe") == 0 && topic) {
		// pr_info("Subscribing to: %s\n", topic);
		ret = subscribe_listener_to_topic(file, topic);
	} else if (strcmp(command, "publish") == 0 && topic && message) {
		// pr_info("Publishing to %s: %s\n", topic, message);
		ret = publish_message(file, topic, message);
	} else if (strcmp(command, "unsubscribe") == 0 && topic) {
		// pr_info("Unsubscribing from: %s\n", topic);
		ret = unsubscribe_listener_from_topic(file, topic);
	} else if (strcmp(command, "fetch") == 0 && topic) {
		// pr_info("Fetching from: %s\n", topic);
		ret = fetch_messages(file, topic);
	} else {
		// pr_err("Unknown command or missing arguments\n");
		return -EINVAL;
	}

	if (ret) {
		return ret;
	}

	return to_copy;
}

static ssize_t my_write(struct file *file, const char __user *user_buf, size_t count, loff_t *ppos)
{
	ssize_t cmd = write_cmd(file, user_buf, count);

	return cmd;
}

// ------------------------------------------------------------
// file operations
// ------------------------------------------------------------

static const struct file_operations my_fops = {
	.owner = THIS_MODULE,
	.open = my_open,
	.release = my_release,
	.read = my_read,
	.write = my_write,
};

// ============================================================
// procfs
// ============================================================

static struct proc_dir_entry *proc_entry;

// ------------------------------------------------------------
// /proc show
// ------------------------------------------------------------

static int my_proc_show(struct seq_file *m, void *v)
{
	struct topic *topic;

	mutex_lock(&global_broker.topics_lock);
	list_for_each_entry(topic, &global_broker.topics, list) {
		seq_printf(m, "%s: %zu\n", topic->name, topic->message_count);
	}
	mutex_unlock(&global_broker.topics_lock);

	return 0;
}

// ------------------------------------------------------------
// /proc open
// ------------------------------------------------------------

static int my_proc_open(struct inode *inode, struct file *file)
{
	return single_open(file, my_proc_show, NULL);
}

// ------------------------------------------------------------
// proc operations
// ------------------------------------------------------------

static const struct proc_ops my_proc_ops = {
	.proc_open = my_proc_open,
	.proc_read = seq_read,
	.proc_lseek = seq_lseek,
	.proc_release = single_release,
};

// ============================================================
// sysfs attributes for topics
// ============================================================

static ssize_t max_subscribers_show(struct kobject *kobj, struct kobj_attribute *attr, char *buf)
{
	struct topic *topic;
	ssize_t ret;

	mutex_lock(&global_broker.topics_lock);
	topic = find_topic_by_name(kobject_name(kobj));
	if (!topic) {
		ret = -ENOENT;
	} else {
		ret = sysfs_emit(buf, "%d\n", topic->max_subscribers);
	}
	mutex_unlock(&global_broker.topics_lock);

	return ret;
}

static ssize_t max_subscribers_store(struct kobject *kobj, struct kobj_attribute *attr,
				     const char *buf, size_t count)
{
	struct topic *topic;
	int new_max;

	if (kstrtoint(buf, 10, &new_max) < 0 || new_max <= 0)
		return -EINVAL;

	mutex_lock(&global_broker.topics_lock);
	topic = find_topic_by_name(kobject_name(kobj));
	if (!topic) {
		mutex_unlock(&global_broker.topics_lock);
		return -ENOENT;
	}

	topic->max_subscribers = new_max;
	mutex_unlock(&global_broker.topics_lock);

	return count;
}

static struct kobj_attribute max_subscribers_attr = __ATTR(max_subscribers, 0644,
							   max_subscribers_show,
							   max_subscribers_store);

static struct attribute *topic_attrs[] = {
	&max_subscribers_attr.attr,
	NULL,
};

static const struct attribute_group topic_attr_group = {
	.attrs = topic_attrs,
};

// ============================================================
// module init
// ============================================================

static void init_broker(void)
{
	mutex_init(&global_broker.topics_lock);
	INIT_LIST_HEAD(&global_broker.topics);
	INIT_LIST_HEAD(&global_broker.listeners);
	global_broker.topic_count = 0;
}

static int __init my_init(void)
{
	int ret;

	// --------------------------------------------------------
	// Initialize broker
	// --------------------------------------------------------
	// DONE: For learning - you'll need to initialize:
	// 1. The topics_lock mutex
	// 2. The topics list
	// Hint: Use mutex_init() and INIT_LIST_HEAD()
	// This is the foundation for your pub/sub system

	ret = alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME);
	if (ret < 0) {
		pr_err("alloc_chrdev_region failed\n");
		return ret;
	}

	// --------------------------------------------------------
	// init cdev
	// --------------------------------------------------------

	cdev_init(&my_cdev, &my_fops);

	ret = cdev_add(&my_cdev, dev_num, 1);
	if (ret < 0) {
		pr_err("cdev_add failed\n");
		unregister_chrdev_region(dev_num, 1);
		return ret;
	}

	// --------------------------------------------------------
	// create class
	// --------------------------------------------------------

	my_class = class_create(CLASS_NAME);

	if (IS_ERR(my_class)) {
		pr_err("class_create failed\n");

		cdev_del(&my_cdev);
		unregister_chrdev_region(dev_num, 1);

		return PTR_ERR(my_class);
	}

	// --------------------------------------------------------
	// create /dev entry
	// --------------------------------------------------------

	if (IS_ERR(device_create(my_class, NULL, dev_num, NULL, DEVICE_NAME))) {
		pr_err("device_create failed\n");

		class_destroy(my_class);
		cdev_del(&my_cdev);
		unregister_chrdev_region(dev_num, 1);

		return -ENOMEM;
	}

	// --------------------------------------------------------
	// create /proc entry
	// --------------------------------------------------------

	proc_entry = proc_create(PROC_NAME, 0444, NULL, &my_proc_ops);

	if (!proc_entry) {
		pr_err("proc_create failed\n");

		device_destroy(my_class, dev_num);
		class_destroy(my_class);
		cdev_del(&my_cdev);
		unregister_chrdev_region(dev_num, 1);

		return -ENOMEM;
	}

	// --------------------------------------------------------
	// create sysfs directory
	// --------------------------------------------------------

	pubsub_kobj = kobject_create_and_add("pubsub", NULL);
	if (!pubsub_kobj) {
		pr_err("kobject_create_and_add failed\n");

		proc_remove(proc_entry);
		device_destroy(my_class, dev_num);
		class_destroy(my_class);
		cdev_del(&my_cdev);
		unregister_chrdev_region(dev_num, 1);

		return -ENOMEM;
	}

	init_broker();

	pr_info("pubsub loaded\n");
	pr_info("/dev/%s created\n", DEVICE_NAME);
	pr_info("/proc/%s created\n", PROC_NAME);
	pr_info("max_topics parameter: %d\n", max_topics);

	return 0;
}

// ============================================================
// module exit
// ============================================================

static void __exit my_exit(void)
{
	struct topic *topic;
	struct topic *tmp;

	// Remove all topics from sysfs
	mutex_lock(&global_broker.topics_lock);
	list_for_each_entry_safe(topic, tmp, &global_broker.topics, list) {
		list_del(&topic->list);
		sysfs_remove_group(topic->kobj, &topic_attr_group);
		kobject_put(topic->kobj);
		kfree(topic->buffer);
		kfree(topic);
	}
	mutex_unlock(&global_broker.topics_lock);

	kobject_put(pubsub_kobj);
	proc_remove(proc_entry);

	device_destroy(my_class, dev_num);

	class_destroy(my_class);

	cdev_del(&my_cdev);

	unregister_chrdev_region(dev_num, 1);

	pr_info("pubsub unloaded\n");
}

module_init(my_init);
module_exit(my_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Pedro Fam");
MODULE_DESCRIPTION("PubSub driver with /dev and /proc");