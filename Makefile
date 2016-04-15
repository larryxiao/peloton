srcdir = src
builddir = build/local
wiredir = $(srcdir)/backend/wire
WIRE_INCLUDES = -I$(wiredir)/*.h

CXX = g++
CXXLD = g++
DEFS = -DHAVE_CONFIG_H
CPPFLAGS =  -I/usr/include
CXXFLAGS = -O0 -DNVML
DEBUG_CXXFLAGS = -O0 -g -ggdb -Wall -Wextra -Werror
INCLUDES = $(WIRE_INCLUDES)
AM_CXXFLAGS = $(DEBUG_CXXFLAGS) -std=c++11 -fPIC -fpermissive \
	-fno-strict-aliasing
SHELL = /bin/bash
LIBTOOL = $(SHELL) build/libtool
AM_LDFLAGS = -static -pthread
LDFLAGS = -L/usr/lib -L/usr/lib

CXXCOMPILE = $(CXX) $(DEFS) $(AM_CXXFLAGS)  \
	 $(CPPFLAGS) $(INCLUDES) $(CXXFLAGS)
CXXLINK = $(LIBTOOL) --tag=CXX \
	--mode=link $(CXXLD) $(AM_CXXFLAGS) \
	$(CXXFLAGS) $(AM_LDFLAGS) $(LDFLAGS)

CPP_FILES := $(wildcard $(wiredir)/*.cpp)
OBJ_FILES := $(addprefix $(builddir)/$(wiredir)/,$(notdir $(CPP_FILES:.cpp=.o)))

wire_server: $(OBJ_FILES)
	$(CXXLINK) -o $(builddir)/$@ $^

$(builddir)/$(wiredir)/%.o: $(wiredir)/%.cpp
	@mkdir -p $(@D)
	$(CXXCOMPILE) -c -o $@ $<

clean:
	@rm -f $(builddir)/src/wire_server
	@rm -rf $(builddir)/$(wiredir)

stylecheck:
	clang-format-3.6 --style=file ./src/postgres/backend/postmaster/postmaster.cpp | diff ./src/postgres/backend/postmaster/postmaster.cpp -
style:
	clang-format-3.6 --style=file -i ./src/postgres/backend/postmaster/postmaster.cpp
	clang-format-3.6 --style=file -i ./src/postgres/backend/tcop/postgres.cpp
