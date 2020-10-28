SUBNAME = observation
SPEC = smartmet-engine-$(SUBNAME)
INCDIR = smartmet//engines/$(SUBNAME)

REQUIRES = gdal

include $(shell echo $${PREFIX-/usr})/share/smartmet/devel/makefile.inc

ifeq ($(origin localstatedir), undefined)
  vardir = /var/smartmet/observation
else
  vardir = $(localstatedir)/smartmet/observation
endif

# Compiler options

DEFINES = -DUNIX -D_REENTRANT

CFLAGS += -Wno-pedantic

INCLUDES += -isystem $(includedir)/mysql

LIBS += -L$(libdir) \
        -lsmartmet-spine \
        -lsmartmet-macgyver \
        -lsmartmet-locus \
	-lsmartmet-gis \
        -lboost_thread \
        -lboost_iostreams \
        -lboost_date_time \
        -lboost_locale \
        -lboost_system \
        -lboost_serialization \
	-lsqlite3 \
	$(shell pkg-config --libs spatialite) \
	$(GDAL_LIBS) \
        -lbz2 -lz \
	-latomic -lpthread

# What to install

LIBFILE = $(SUBNAME).so

# Compilation directories

vpath %.cpp $(SUBNAME)
vpath %.h $(SUBNAME)

# The files to be compiled

SRCS = $(wildcard $(SUBNAME)/*.cpp)
HDRS = $(wildcard $(SUBNAME)/*.h) $(wildcard $(SUBNAME)/*.ipp)
OBJS = $(patsubst %.cpp, obj/%.o, $(notdir $(SRCS)))

INCLUDES := -Iinclude $(INCLUDES)

.PHONY: test rpm

# The rules

all: objdir $(LIBFILE)
debug: all
release: all
profile: all

$(LIBFILE): $(OBJS)
	$(CXX) $(CFLAGS) -shared -rdynamic -o $(LIBFILE) $(OBJS) $(LIBS)
	@echo Checking $(LIBFILE) for unresolved references
	@if ldd -r $(LIBFILE) 2>&1 | c++filt | grep ^undefined\ symbol ; \
		then rm -v $(LIBFILE); \
		exit 1; \
	fi

clean:
	rm -f $(LIBFILE) *~ $(SUBNAME)/*~
	rm -rf obj

format:
	clang-format -i -style=file $(SUBNAME)/*.h $(SUBNAME)/*.cpp

install:
	@mkdir -p $(includedir)/$(INCDIR)
	@list='$(HDRS)'; \
	for hdr in $$list; do \
	  HDR=$$(basename $$hdr); \
	  echo $(INSTALL_DATA) $$hdr $(includedir)/$(INCDIR)/$$HDR; \
	  $(INSTALL_DATA) $$hdr $(includedir)/$(INCDIR)/$$HDR; \
	done
	mkdir -p $(enginedir)
	$(INSTALL_PROG) $(LIBFILE) $(enginedir)/$(LIBFILE)
	mkdir -p $(vardir)
	if [[ ! -e $(vardir)/stations.txt ]]; then $(INSTALL_DATA) cnf/stations.txt $(vardir)/; fi
	if [[ ! -e $(vardir)/stations.sqlite ]]; then $(INSTALL_DATA) cnf/stations.sqlite $(vardir)/; fi

test:
	@test -d test || echo "No test subdirectory, no tests defined"
	@test ! -d test || ( cd test && make test )

objdir:
	@mkdir -p $(objdir)

rpm: clean $(SPEC).spec
	rm -f $(SPEC).tar.gz # Clean a possible leftover from previous attempt
	tar -czvf $(SPEC).tar.gz --exclude test --exclude-vcs --transform "s,^,$(SPEC)/," *
	rpmbuild -ta $(SPEC).tar.gz
	rm -f $(SPEC).tar.gz

.SUFFIXES: $(SUFFIXES) .cpp

obj/%.o: %.cpp
	$(CXX) $(CFLAGS) $(INCLUDES) -c -o $@ $<

ifneq ($(wildcard obj/*.d),)
-include $(wildcard obj/*.d)
endif
