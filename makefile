# Nome do executável
TARGET = programa

# Compilador
CXX = g++

# Flags de compilação
CXXFLAGS = -Wall -Wextra -std=c++17

SRCS = \
    etl/dataframe.cpp \
    etl/extrator.cpp \
    etl/handlers.cpp \
	etl/loader.cpp  \
    main.cpp \
	pipeline/pipeline.cpp \
	etl/dashboard.cpp \
	triggers.cpp \

OBJS = $(SRCS:.cpp=.o)

# Regra principal
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS) -lsqlite3

# Regra para compilar arquivos .cpp em .o
%.o: %.cpp %.h
	$(CXX) $(CXXFLAGS) -c $< -o $@ 
# Limpeza dos arquivos compilados
clean:
	rm -f $(OBJS) $(TARGET)