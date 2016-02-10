ROOT = ../..
PROJECT = ecallmgr-extension

EBINS = $(wildcard $(ROOT)/core/kazoo_documents/ebin) \
	$(shell find $(ROOT)/deps/rabbitmq_erlang_client-* -name ebin)

all: compile

-include $(ROOT)/make/kz.mk

