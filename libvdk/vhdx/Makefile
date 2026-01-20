#comment
CC = gcc
CPP = g++ 
CC_TARGET = vhdx
CC_TARGET_D = vhdxd
CC_TARGET_DEST = bin/
CC_LIB_TARGET = libvdk.a

CC_DEFINE = #-DRW_DEBUG
CC_FLAGS = -g -Wall -fmessage-length=0 -fshort-wchar -std=c11
CPP_FLAGS = -g -Wall -fmessage-length=0 -fshort-wchar -std=c++11
CC_INCLUDES = -I../utils
LK_FLAGS = #-L../../libelk
CC_LIBS = -luuid #-lelk
APP_OBJS = main.o

OBJS = header.o metadata.o log.o utils.o utils_encrypt.o utils_file.o vhdx.o
APP_OBJS = $(OBJS)
APP_OBJS += main.o

OBJS_POS = $(addprefix $(CC_TARGET_DEST),$(OBJS))
APP_OBJS_POS = $(addprefix $(CC_TARGET_DEST),$(APP_OBJS))


all: $(CC_TARGET)

.PHONY: lib
lib: $(CC_LIB_TARGET)
$(CC_LIB_TARGET) : $(OBJS)
	$(AR) -r $(CC_TARGET_DEST)$(CC_LIB_TARGET) $(OBJS_POS)
	cp -f $(CC_TARGET_DEST)$(CC_LIB_TARGET) ./$(CC_LIB_TARGET)

$(CC_TARGET) : $(APP_OBJS)
	$(CPP) $(LK_FLAGS) -o $(CC_TARGET_DEST)$(CC_TARGET) $(APP_OBJS_POS) $(CC_LIBS)
	cp -f $(CC_TARGET_DEST)$(CC_TARGET) ./$(CC_TARGET)

%.o : %.c
	$(CC) $(CC_FLAGS) $(CC_INCLUDES) $(CC_DEFINE) -c $< -o $(CC_TARGET_DEST)$@

%.o : %.cpp
	$(CPP) $(CPP_FLAGS) $(CC_INCLUDES) $(CC_DEFINE) -c $< -o $(CC_TARGET_DEST)$@

vpath %.h src
vpath %.cpp src
vpath %.c src
vpath utils.cpp ../utils
vpath utils_file.cpp ../utils
vpath utils_encrypt.cpp ../utils

.PHONY : clean
clean:
	$(RM) $(CC_TARGET_DEST)*.o $(CC_TARGET_DEST)*.d
	$(RM) $(CC_TARGET_DEST)$(CC_TARGET) 
