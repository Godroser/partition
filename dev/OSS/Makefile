# Makefile for C++11 projects
CXX = g++
CXXFLAGS = -std=c++11 -Wall -Wextra
TARGET = test
SOURCE = test.cpp

$(TARGET): $(SOURCE)
	$(CXX) $(CXXFLAGS) $(SOURCE) -o $(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: clean 