PROG = $(patsubst %.cpp,%,$(wildcard *Test.cpp))

REQUIRES = gdal

include $(shell echo $${PREFIX-/usr})/share/smartmet/devel/makefile.inc

SOURCES = /usr/share/smartmet/engines/geonames.so \
	../observation.so

ifneq ($(wildcard /usr/libspatialite50/include/spatialite.h),)
    INCLUDES += -isystem /usr/libspatialite50/include
    SPATIALITE_LIBS = -L /usr/libspatialite50/lib -lspatialite
else
ifneq ($(wildcard /usr/libspatialite43/include/spatialite.h),)
    INCLUDES += -isystem /usr/libspatialite43/include
    SPATIALITE_LIBS = -L /usr/libspatialite43/lib -lspatialite
else
    SPATIALITE_LIBS = -lspatialite
endif
endif

INCLUDES += \
	-I../observation \
	-I/usr/include/smartmet \
	-I/usr/include/oracle/11.2/client64

LIBS += \
	-lsmartmet-spine \
	-lsmartmet-macgyver \
	-lsmartmet-locus \
	-lsmartmet-newbase \
	-lboost_date_time \
	-lboost_thread \
	-lboost_filesystem \
	-lboost_serialization \
	-lboost_regex \
	-lboost_iostreams \
	-lboost_system \
	$(SPATIALITE_LIBS) \
	-lsqlite3 \
	-lbz2 -lz -lpthread

all: 	$(PROG)
clean:
	rm -f $(PROG) *~

test: $(PROG)
	@echo Running tests:
	@for prog in $(PROG); do \
	./$$prog -s; \
	done

$(PROG) : % : %.cpp ../observation.so /usr/share/smartmet/engines/geonames.so
	$(CXX) $(CFLAGS) $(SOURCES) -o $@ $@.cpp $(INCLUDES) $(LIBS)
