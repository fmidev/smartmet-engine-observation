PROG = $(patsubst %.cpp,%,$(wildcard *Test.cpp))

MAINFLAGS = -std=c++11 -Wall -W -Wno-unused-parameter -Wno-unknown-pragmas

EXTRAFLAGS = -Werror -pedantic -Wpointer-arith -Wcast-qual \
	-Wcast-align -Wwrite-strings -Wconversion -Winline \
	-Wctor-dtor-privacy -Wnon-virtual-dtor -Wno-pmf-conversions \
	-Wsign-promo -Wchar-subscripts -Wold-style-cast \
	-Wshadow -Woverloaded-virtual

DIFFICULTFLAGS = -Weffc++ -Wunreachable-code -Wredundant-decls

CC = g++
CFLAGS = -DUNIX -O0 -ggdb3 $(MAINFLAGS)

SOURCES = /usr/share/smartmet/engines/geonames.so \
	../observation.so

# Boost 1.69

ifneq "$(wildcard /usr/include/boost169)" ""
  INCLUDES += -I/usr/include/boost169
  LIBS += -L/usr/lib64/boost169
endif

ifneq "$(wildcard /usr/gdal30/include)" ""
  INCLUDES += -isystem /usr/gdal30/include
  LIBS += -L/usr/gdal30/lib
else
  INCLUDES += -isystem /usr/include/gdal
endif

INCLUDES += \
	-I../observation \
	-I/usr/include/smartmet \
	-I/usr/include/oracle/11.2/client64 \
	`pkg-config --cflags libconfig++`

#	-I usr/include/soci \

LIBS += \
	-lsmartmet-spine \
	-lsmartmet-macgyver \
	-lsmartmet-locus \
	-lsmartmet-newbase \
	`pkg-config --libs libconfig++` \
	-lpqxx \
	-lboost_date_time \
	-lboost_thread \
	-lboost_filesystem \
	-lboost_serialization \
	-lboost_regex \
	-lboost_iostreams \
	-lboost_system \
	-lsqlite3 \
	`pkg-config --libs spatialite` \
	-lbz2 -lz -lpthread

#	-lsoci_core -lsoci_sqlite3 \

all: 	$(PROG)
clean:
	rm -f $(PROG) *~

test: $(PROG)
	@echo Running tests:
	@for prog in $(PROG); do \
	./$$prog -s; \
	done

$(PROG) : % : %.cpp ../observation.so /usr/share/smartmet/engines/geonames.so
	$(CC) $(CFLAGS) $(SOURCES) -o $@ $@.cpp $(INCLUDES) $(LIBS)
