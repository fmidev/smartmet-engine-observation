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

# Older GCC won't accept newer standards in linker options, and we're using CC due to -latomic issues
LFLAGS = $(filter-out -std=c++%, $(CFLAGS))

INCLUDES += -isystem $(includedir)/mysql

LIBS += $(REQUIRED_LIBS) \
	-L$(libdir) \
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
	-latomic \
	$(REQUIRED_LIBS) \
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

# Note: CC instead of CXX since clang++ does not find -latomic

$(LIBFILE): $(OBJS)
	$(CXX) $(LFLAGS) -shared -rdynamic -o $(LIBFILE) $(OBJS) $(LIBS)
	@echo Checking $(LIBFILE) for unresolved references
	@if ldd -r $(LIBFILE) 2>&1 | c++filt | grep "^undefined symbol" | grep -v SmartMet::Engine::Geonames ; \
		then rm -v $(LIBFILE); \
		exit 1; \
	fi

clean:
	rm -f $(LIBFILE) *~ $(SUBNAME)/*~
	rm -rf obj
	make -C test clean

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

.SUFFIXES: $(SUFFIXES) .cpp

obj/%.o: %.cpp
	$(CXX) $(CFLAGS) $(INCLUDES) -c  -MD -MF $(patsubst obj/%.o, obj/%.d, $@) -MT $@ -o $@ $<

ifneq ($(wildcard obj/*.d),)
-include $(wildcard obj/*.d)
endif
