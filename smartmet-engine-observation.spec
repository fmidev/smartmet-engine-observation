%define DIRNAME observation
%define LIBNAME smartmet-%{DIRNAME}
%define SPECNAME smartmet-engine-%{DIRNAME}
Summary: SmartMet Observation Engine
Name: %{SPECNAME}
Version: 17.3.30
Release: 1%{?dist}.fmi
License: FMI
Group: SmartMet/Engines
URL: https://github.com/fmidev/smartmet-engine-observation
Source0: %{name}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires: libconfig-devel
BuildRequires: boost-devel
Requires: libconfig
BuildRequires: smartmet-library-spine-devel >= 17.1.24
BuildRequires: smartmet-engine-geonames-devel >= 17.1.24
BuildRequires: mysql++-devel >= 3.1.0
BuildRequires: libspatialite-devel >= 4.1.1
BuildRequires: sqlite-devel >= 3.11.0
BuildRequires: soci-devel >= 3.2.3
BuildRequires: soci-sqlite3-devel >= 3.2.3
BuildRequires: smartmet-library-locus-devel >= 16.12.20
BuildRequires: smartmet-library-macgyver-devel >= 17.1.18
Requires: smartmet-server >= 17.1.25
Requires: smartmet-engine-geonames >= 17.1.24
Requires: smartmet-library-spine >= 17.1.24
Requires: smartmet-library-locus >= 16.12.20
Requires: smartmet-library-macgyver >= 17.1.18
Requires: libatomic
Requires: unixODBC
Requires: mysql++
Requires: libspatialite >= 4.1.1
Requires: sqlite >= 3.11.0
Requires: soci >= 3.2.3
Requires: soci-sqlite3 >= 3.2.3
Requires: boost-date-time
Requires: boost-iostreams
Requires: boost-locale
Requires: boost-serialization
Requires: boost-system
Requires: boost-thread
Obsoletes: smartmet-brainstorm-obsengine < 16.11.1
Obsoletes: smartmet-brainstorm-obsengine-debuginfo < 16.11.1

%if 0%{rhel} >= 7
BuildRequires: mariadb-devel
Requires: mariadb-libs
%else
BuildRequires: mysql-devel
%endif
Provides: %{SPECNAME}

%description
SmartMet engine for fetching observations from the climate database (cldb).

%package -n %{SPECNAME}-devel
Summary: SmartMet %{SPECNAME} development headers
Group: SmartMet/Development
Provides: %{SPECNAME}-devel
Obsoletes: smartmet-brainstorm-obsengine-devel < 16.11.1
%description -n %{SPECNAME}-devel
SmartMet %{SPECNAME} development headers.

%prep
rm -rf $RPM_BUILD_ROOT

%setup -q -n %{SPECNAME}

%build -q -n %{SPECNAME}
make %{_smp_mflags}

%install
%makeinstall
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/ld.so.conf.d
mkdir -p $RPM_BUILD_ROOT%{_var}/smartmet/observation
install -m 664 cnf/stations.xml $RPM_BUILD_ROOT/var/smartmet/observation/stations.xml
install -m 664 cnf/stations.sqlite.2 $RPM_BUILD_ROOT/var/smartmet/observation/stations.sqlite.2

%post
/sbin/ldconfig

%clean
rm -rf $RPM_BUILD_ROOT

%files -n %{SPECNAME}
%defattr(0775,root,root,0775)
%{_datadir}/smartmet/engines/%{DIRNAME}.so
%defattr(0664,root,root,0775)
%config(noreplace) %{_var}/smartmet/observation/stations.xml
%config(noreplace) %{_var}/smartmet/observation/stations.sqlite.2

%files -n %{SPECNAME}-devel
%defattr(0664,root,root,0775)
%{_includedir}/smartmet/engines/%{DIRNAME}

%changelog
* Wed Mar 30 2017 Anssi Reponen <anssi.reponen@fmi.fi> - 17.3.30-1.fmi
- Configuration parameters for cache added and old renamed (all cache parameters are now inside cache group in configuration file):
 ¤ finCacheUpdateInterval (update interval of FMI data, optional, default value is 0 [no updates])
 ¤ extCacheUpdateInterval  (update interval of non-FMI data, optional, default value is 0 [no updates])
 ¤ flashCacheUpdateInterval (update interval of flash data, optional, default value is 0 [no updates])
 ¤ finCacheDuration (max lifetime of FMI data, mandatory)
 ¤ extCacheDuration (max lifetime of non-FMI data, mandatory)
 ¤ flashCacheDuration (max lifetime of flash data, mandatory)
 ¤ disableAllCacheUpdates (disables all cache updates, default values is false)
 ¤ locationCacheSize (max number of stations in location cache, mandatory)
- jss dependencies removed
- ObservationCacheParameters-struct moved into separate header file

* Thu Mar 28 2017 Anssi Reponen <anssi.reponen@fmi.fi> - 17.3.28-1.fmi
- references to oracle removed from Makefile and spec-file
- 'dbDriverFile' configuration parameter added: 
contains library file name of database driver, if missing or empty DummyDatabaseDriver is created
- Database driver module is loaded dynamically
- MastQuery and VerifiableMessageQuery files moved here from delfoi library
- missing cnf-directory added

* Thu Mar 23 2017 Anssi Reponen <anssi.reponen@fmi.fi> - 17.3.23-1.fmi
- Oracle dependent code moved to delfoi-library
