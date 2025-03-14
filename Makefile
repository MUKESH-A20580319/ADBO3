CC = gcc
CFLAGS = -Wall -g

# Source files
SRC = record_mgr.c buffer_mgr.c storage_mgr.c dberror.c expr.c rm_serializer.c test_expr.c test_assign3_1.c

# Header files
HDR = record_mgr.h buffer_mgr.h storage_mgr.h dberror.h expr.h tables.h test_helper.h buffer_mgr_stat.h

# Object files
OBJ = $(SRC:.c=.o)

# Executables
EXE = test_expr test_assign3

# Default rule
all: $(EXE)

# Compile test_expr

test_expr: test_expr.o $(OBJ)
	$(CC) $(CFLAGS) -o test_expr test_expr.o $(OBJ)

# Compile test_assign3

test_assign3: test_assign3_1.o $(OBJ)
	$(CC) $(CFLAGS) -o test_assign3 test_assign3_1.o $(OBJ)

# Compile object files
%.o: %.c $(HDR)
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up
clean:
	rm -f $(OBJ) test_expr test_assign3 *.o
