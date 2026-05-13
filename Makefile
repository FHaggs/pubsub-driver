obj-m := pubsub.o

BUILDROOT_DIR := $(abspath ../..)
KDIR := $(BUILDROOT_DIR)/output/build/linux-6.12.27
TARGET_DIR := $(BUILDROOT_DIR)/output/target

CROSS_COMPILE := $(BUILDROOT_DIR)/output/host/bin/i686-buildroot-linux-gnu-
DEPMOD := $(BUILDROOT_DIR)/output/host/sbin/depmod
KERNEL_RELEASE := $(shell cat $(KDIR)/include/config/kernel.release)

all: install

build:
	$(MAKE) -C $(KDIR) M=$(CURDIR) CROSS_COMPILE=$(CROSS_COMPILE) modules

install: build
	$(MAKE) -C $(KDIR) M=$(CURDIR) CROSS_COMPILE=$(CROSS_COMPILE) \
		modules_install INSTALL_MOD_PATH=$(TARGET_DIR)
	$(DEPMOD) -a -b $(TARGET_DIR) $(KERNEL_RELEASE)
