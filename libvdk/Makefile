ARCH := $(shell uname -m)
MAKE = make

FINAL_DEST_DIR := build/
FINAL_DEST_HEADER := include/
FINAL_DEST_LIB := lib/
FINAL_DEST_BIN := bin/


TARGETS = vpc vhdx libvdk.a
OBJS_POS = vpc/bin/vpc.o vhdx/bin/header.o vhdx/bin/log.o vhdx/bin/metadata.o vhdx/bin/vhdx.o
OBJS_POS += vhdx/bin/utils.o vhdx/bin/utils_encrypt.o vhdx/bin/utils_file.o

LIB_HEADERS = vpc/vpc.h vhdx/common.h vhdx/header.h vhdx/log.h vhdx/metadata.h vhdx/vhdx.h utils/utils.h

all : $(TARGETS)
.PHONY : $(TARGETS)

vpc:
	cd vpc && if [ ! -d bin ]; then mkdir bin; fi && $(MAKE) clean && $(MAKE)
	cp -f $@/$@ $(FINAL_DEST_DIR)$(FINAL_DEST_BIN)

vhdx:
	cd vhdx && if [ ! -d bin ]; then mkdir bin; fi && $(MAKE) clean && $(MAKE)
	cp -f $@/$@ $(FINAL_DEST_DIR)$(FINAL_DEST_BIN)

libvdk.a:
	$(AR) -r $(FINAL_DEST_DIR)$(FINAL_DEST_LIB)$@ $(OBJS_POS)
	cp -f $(LIB_HEADERS) $(FINAL_DEST_DIR)$(FINAL_DEST_HEADER)


