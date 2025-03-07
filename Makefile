SUBNAME = observation
SPEC = smartmet-engine-$(SUBNAME)
INCDIR = smartmet//engines/$(SUBNAME)

REQUIRES = gdal configpp sqlite3 spatialite

include $(shell echo $${PREFIX-/usr})/share/smartmet/devel/makefile.inc

ifeq ($(origin localstatedir), undefined)
  vardir = /var/smartmet/observation
else
  vardir = $(localstatedir)/smartmet/observation
endif

# Compiler options

DEFINES = -DUNIX -D_REENTRANT

# TODO: Should remove -Wno-sign-conversion, FlashTools.cpp warns a lot

CFLAGS += -Wno-pedantic

INCLUDES += -isystem $(includedir)/mysql

LIBS += $(PREFIX_LDFLAGS) \
	$(REQUIRED_LIBS) \
	-lsmartmet-timeseries \
	-lsmartmet-spine \
        -lsmartmet-macgyver \
        -lsmartmet-locus \
	-lsmartmet-gis \
        -lboost_thread \
        -lboost_iostreams \
        -lboost_locale \
        -lboost_system \
        -lboost_serialization \
	-latomic \
        -lbz2 -lz \
	-lpthread

# What to install

LIBFILE = $(SUBNAME).so

# Compilation directories

vpath %.cpp $(SUBNAME)
vpath %.h $(SUBNAME)

# The files to be compiled

SRCS = $(wildcard $(SUBNAME)/*.cpp)
HDRS = $(wildcard $(SUBNAME)/*.h) $(wildcard $(SUBNAME)/*.ipp)
OBJS = $(patsubst %.cpp, obj/%.o, $(notdir $(SRCS)))

.PHONY: test rpm

# The rules

all: objdir $(LIBFILE)
debug: all
release: all
profile: all

$(LIBFILE): $(OBJS)
	$(CXX) $(CFLAGS) -shared -rdynamic -o $(LIBFILE) $(OBJS) $(LIBS)
	@echo Checking $(LIBFILE) for unresolved references
	@if ldd -r $(LIBFILE) 2>&1 | c++filt | grep "^undefined symbol" | grep -v SmartMet::Engine::Geonames |\
                        grep -Pv ':\ __(?:(?:a|t|ub)san_|sanitizer_)'; \
		then rm -v $(LIBFILE); \
		exit 1; \
	fi

clean:
	rm -f $(LIBFILE) *~ $(SUBNAME)/*~
	rm -rf obj
	$(MAKE) -C test $@

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
	@test ! -d test || ( $(MAKE) -C test test )

objdir:
	@mkdir -p $(objdir)

rpm: clean $(SPEC).spec
	rm -f $(SPEC).tar.gz # Clean a possible leftover from previous attempt
	tar -czvf $(SPEC).tar.gz --exclude test --exclude-vcs --transform "s,^,$(SPEC)/," *
	rpmbuild -tb $(SPEC).tar.gz
	rm -f $(SPEC).tar.gz

test-headers:
	ok=true; OPTIMIZE=-O0; for header in $(SUBNAME)/*.h ; do \
	   echo Testing $$header; \
	   echo "#include \"$$header\"" | $(CXX) -S -x c++ $(CFLAGS) $(INCLUDES) - -o /dev/null || ok=false; \
	done; $$ok

.SUFFIXES: $(SUFFIXES) .cpp

obj/%.o: %.cpp
	$(CXX) $(CFLAGS) $(INCLUDES) -c  -MD -MF $(patsubst obj/%.o, obj/%.d, $@) -MT $@ -o $@ $<

ifneq ($(wildcard obj/*.d),)
-include $(wildcard obj/*.d)
endif
