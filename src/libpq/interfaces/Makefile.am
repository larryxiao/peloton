## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# LIBPQ_INTERFACES
######################################################################

lib_LTLIBRARIES = libpq.la

libpq_la_SOURCES = \
				   libpq/interfaces/fe-print.cpp \
				   libpq/interfaces/fe-auth.cpp \
				   libpq/interfaces/fe-exec.cpp \
				   libpq/interfaces/pqexpbuffer.cpp \
				   libpq/interfaces/test/uri-regress.cpp \
				   libpq/interfaces/libpq-events.cpp \
				   libpq/interfaces/fe-secure.cpp \
				   libpq/interfaces/fe-misc.cpp \
				   libpq/interfaces/fe-protocol2.cpp \
				   libpq/interfaces/fe-connect.cpp \
				   libpq/interfaces/fe-protocol3.cpp \
				   libpq/interfaces/fe-lobj.cpp

libpq_la_CPPFLAGS = \
				   -I$(srcdir)/libpq/include \
				   -I$(srcdir)/libpq/interfaces \
				   -I$(srcdir)/libpq/backend \
				   -I$(srcdir)/libpq/port


libpq_la_CXXFLAGS = -Wno-write-strings

libpq_la_LDFLAGS = -lssl
