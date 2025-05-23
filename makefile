# Compilador e flags
CXX = g++

# Flags de compilação
CXXFLAGS = -Wall -Wextra -std=c++17

# Fontes comuns
COMMON_SRCS = \
    etl/dataframe.cpp \
    etl/extrator.cpp \
    etl/handlers.cpp \
    etl/loader.cpp \
    pipeline/pipeline.cpp \
    etl/dashboard.cpp \
    triggers.cpp

COMMON_OBJS = $(COMMON_SRCS:.cpp=.o)

# Programa principal (usa main.cpp)
PROGRAMA_SRCS = main.cpp
PROGRAMA_OBJS = $(PROGRAMA_SRCS:.cpp=.o)
PROGRAMA_TARGET = programa

# Programa de threads (usa threadstime.cpp)
THREADS_SRCS = threadsTime.cpp
THREADS_OBJS = $(THREADS_SRCS:.cpp=.o)
THREADS_TARGET = threadsTime

# Target padrão
all: $(PROGRAMA_TARGET)

# Regras principais
$(PROGRAMA_TARGET): $(COMMON_OBJS) $(PROGRAMA_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lsqlite3

$(THREADS_TARGET): $(COMMON_OBJS) $(THREADS_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ -lsqlite3

# Compilar .cpp em .o
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Rodar os executáveis
run: $(PROGRAMA_TARGET)
	./$(PROGRAMA_TARGET)

run-threads: $(THREADS_TARGET)
	./$(THREADS_TARGET)

# Limpeza
clean:
	rm -f $(COMMON_OBJS) $(PROGRAMA_OBJS) $(THREADS_OBJS) $(PROGRAMA_TARGET) $(THREADS_TARGET)
