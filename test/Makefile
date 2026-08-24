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

# Build the engine when its sources are newer than the library, so that running
# the tests directly from this directory cannot link a stale observation.so.
../observation.so: $(wildcard ../observation/*.cpp ../observation/*.h)
	$(MAKE) -C .. all

$(PROG) : % : %.cpp ../observation.so /usr/share/smartmet/engines/geonames.so
	$(CXX) $(CFLAGS) $(SOURCES) -o $@ $@.cpp $(INCLUDES) $(LIBS)
