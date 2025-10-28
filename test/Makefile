PROG = $(patsubst %.cpp,%,$(wildcard *Test.cpp))

REQUIRES = gdal sqlite3 spatialite

include $(shell echo $${PREFIX-/usr})/share/smartmet/devel/makefile.inc

SOURCES = /usr/share/smartmet/engines/geonames.so \
	../observation.so

INCLUDES += \
	-I../observation \
	-I/usr/include/smartmet

LIBS += \
	-lsmartmet-timeseries \
	-lsmartmet-spine \
	-lsmartmet-macgyver \
	-lsmartmet-locus \
	-lsmartmet-newbase \
	-lboost_thread \
	-lboost_serialization \
	-lboost_regex \
	-lboost_iostreams \
	$(REQUIRED_LIBS) \
	-lbz2 -lz -lpthread

all: 	$(PROG)
clean:
	rm -f $(PROG) *~

test: $(PROG)
	@echo Running tests:
	@for prog in $(PROG); do \
	./$$prog; \
	done

$(PROG) : % : %.cpp ../observation.so /usr/share/smartmet/engines/geonames.so
	$(CXX) $(CFLAGS) $(SOURCES) -o $@ $@.cpp $(INCLUDES) $(LIBS)
