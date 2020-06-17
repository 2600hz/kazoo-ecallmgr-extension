CWD = $(shell pwd -P)
ROOT ?= $(realpath $(CWD)/../..)
PROJECT = ecallmgr_extension

all: compile

include $(ROOT)/make/kz.mk
