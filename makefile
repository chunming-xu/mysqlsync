PATH_INC = .  
PATH_SRC = .
PATH_BIN = .
PATH_OBJ = .
PATH_COM = .

# Setting for compile
CC       := g++
CFLAGS   += -I$(PATH_INC) 
CFLAGS   += -I/Users/stevenxu/Desktop/src/dbsync/include
CFLAGS   += -I./sql/
CFLAGS   += -Wall 
CFLAGS   += -g 
#CFLAGS   += -O2
LIBS     := -L/Users/stevenxu/Desktop/run/mysql5.7.26/lib/ -lmysqlclient

# Target to create
ALL  := $(PATH_BIN)/mysqlsync
#OBJS := $(patsubst $(PATH_SRC)/%.cpp,$(PATH_OBJ)/%.o,$(wildcard $(PATH_SRC)/*.cpp))
OBJS := $(patsubst $(PATH_SRC)/%.cpp,$(PATH_OBJ)/%.o,$(wildcard $(PATH_SRC)/*.cpp))

DEPS = $(patsubst %.o, .%.d, $(OBJS))

all: $(ALL)

$(PATH_OBJ)/%.o :  $(PATH_SRC)/%.cpp 
	$(CC) $(CFLAGS) -c $< -o $@

$(PATH_BIN)/mysqlsync : $(OBJS)
	g++ $^  -o $@ $(CFLAGS) $(LIBS)
	
# Dependency Files
-include $(DEPS)

# Tools
.PHONY : clean 
clean:
	rm -f $(ALL) $(OBJS) $(DEPS) 
