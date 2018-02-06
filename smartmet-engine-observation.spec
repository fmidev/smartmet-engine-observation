%define DIRNAME observation
%define LIBNAME smartmet-%{DIRNAME}
%define SPECNAME smartmet-engine-%{DIRNAME}
Summary: SmartMet Observation Engine
Name: %{SPECNAME}
Version: 18.2.6
Release: 1%{?dist}.fmi
License: FMI
Group: SmartMet/Engines
URL: https://github.com/fmidev/smartmet-engine-observation
Source0: %{name}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: libconfig-devel
BuildRequires: boost-devel
Requires: libconfig
BuildRequires: smartmet-library-spine-devel >= 18.1.15
BuildRequires: smartmet-engine-geonames-devel >= 18.1.31
BuildRequires: mysql++-devel >= 3.1.0
BuildRequires: libspatialite-devel >= 4.3.0a
BuildRequires: sqlite-devel >= 3.20.1
BuildRequires: smartmet-library-locus-devel >= 18.1.15
BuildRequires: smartmet-library-macgyver-devel >= 18.2.1
BuildRequires: libatomic
BuildRequires: bzip2-devel
Requires: smartmet-server >= 17.11.10
Requires: smartmet-engine-geonames >= 18.1.31
Requires: smartmet-library-spine >= 18.1.15
Requires: smartmet-library-locus >= 18.1.15
Requires: smartmet-library-macgyver >= 18.2.1
Requires: libatomic
Requires: unixODBC
Requires: mysql++
Requires: libspatialite >= 4.3.0a
Requires: sqlite >= 3.20.1
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
Requires: smartmet-library-spine-devel
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

%post
/sbin/ldconfig

%clean
rm -rf $RPM_BUILD_ROOT

%files -n %{SPECNAME}
%defattr(0775,root,root,0775)
%{_datadir}/smartmet/engines/%{DIRNAME}.so
%defattr(0664,root,root,0775)
%config(noreplace) %{_var}/smartmet/observation/stations.txt
%config(noreplace) %{_var}/smartmet/observation/stations.sqlite

%files -n %{SPECNAME}-devel
%defattr(0664,root,root,0775)
%{_includedir}/smartmet/engines/%{DIRNAME}

%changelog
* Tue Feb  6 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.2.6-1.fmi
- Avoid writing same rows to the cache again

* Mon Feb  5 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.2.5-1.fmi
- Readers sleep 1 ms if DB is locked, writers sleep 5 ms after each bulk insert.

* Fri Feb  2 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.2.2-1.fmi
- Automatically retry sqlite requests when the db is locked or otherwise busy

* Mon Jan 29 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.1.29-1.fmi
- Use stations.sqlite instead of stations.sqlite.2

* Thu Jan 18 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.1.18-2.fmi
- Added missing transaction commit to updateStations()

* Thu Jan 18 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.1.18-1.fmi
- Added setting sqlite.auto_vacuum with default value NONE
- Added setting sqlite.cache_size with default value zero (use sqlite default)
- Added setting sqlite.threads with default value zero (no helper threads)

* Wed Jan 17 2018 Anssi Reponen <anssi.reponen@fmi.fi> - 18.1.17-1.fmi
- soci-api replaced by sqlit3pp-api: BRAINSTORM-965

* Mon Jan 15 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.1.15-1.fmi
- Recompiled due to postgresql and libpqxx updates

* Mon Oct 23 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.10.23-2.fmi
- Removed Utils::errorLog as obsolete - leave logging to higher levels

* Mon Oct 23 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.10.23-1.fmi
- Fixed spatialite indexes

* Fri Oct 13 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.10.13-1.fmi
- Changed fincacheduration to be an optional parameter with default value zero
- SpatiaLite min/max times are now always read from the database to enable different drivers

* Wed Oct  4 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.10.4-1.fmi
- Changed the getLatestObservationTime type methods to return not_a_date_time if the respective table is empty

* Mon Aug 28 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.8.28-1.fmi
- Upgrade to boost 1.65

* Mon May 29 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.5.29-1.fmi
- Repackaged due to Delfoi changes

* Fri May  5 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.5.5-1.fmi
- Fixed producer endtime metadata to be in UTC time instead of local time

* Thu Apr 20 2017 Anssi Reponen <anssi.reponen@fmi.fi> - 17.4.20-1.fmi
- Observation metadata feature added

* Wed Apr 19 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.4.19-1.fmi
- Fixed spatialiteFile path to be relative to the configuration file

* Mon Apr 10 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.4.10-1.fmi
- Configuration paths can now be relative

* Sat Apr  8 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.4.8-1.fmi
- Fixed logMessage to always print a space after the timestamp

* Fri Apr  7 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.4.7-2.fmi
- Improved error reporting on serialization failures

* Fri Apr  7 2017 Mika Heiskanen <mika.heiskanen@fmi.fi> - 17.4.7-1.fmi
- Fixed to include stations.txt instead of stations.xml

* Thu Apr 6 2017 Anssi Reponen <anssi.reponen@fmi.fi> - 17.4.6-1.fmi
- Reading cache duration parameters from confgig file moved to database driver

* Wed Apr 5 2017 Anssi Reponen <anssi.reponen@fmi.fi> - 17.4.5-1.fmi
- Thread handling moved to delfoi-library
- Reading of database-specific parameters from configuration file moved to delfoi-library
- Observation cache -specific parameters read in SpatiaLiteCache.cpp

* Thu Mar 30 2017 Anssi Reponen <anssi.reponen@fmi.fi> - 17.3.30-1.fmi
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

* Tue Mar 28 2017 Anssi Reponen <anssi.reponen@fmi.fi> - 17.3.28-1.fmi
- references to oracle removed from Makefile and spec-file
- 'dbDriverFile' configuration parameter added: 
contains library file name of database driver, if missing or empty DummyDatabaseDriver is created
- Database driver module is loaded dynamically
- MastQuery and VerifiableMessageQuery files moved here from delfoi library
- missing cnf-directory added

* Thu Mar 23 2017 Anssi Reponen <anssi.reponen@fmi.fi> - 17.3.23-1.fmi
- Oracle dependent code moved to delfoi-library
