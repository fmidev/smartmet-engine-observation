%define DIRNAME observation
%define LIBNAME smartmet-%{DIRNAME}
%define SPECNAME smartmet-engine-%{DIRNAME}
Summary: SmartMet Observation Engine
Name: %{SPECNAME}
Version: 19.9.12
Release: 1%{?dist}.fmi
License: FMI
Group: SmartMet/Engines
URL: https://github.com/fmidev/smartmet-engine-observation
Source0: %{name}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires: rpm-build
BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: libconfig-devel
BuildRequires: boost-devel
BuildRequires: smartmet-library-spine-devel >= 19.8.28
BuildRequires: smartmet-engine-geonames-devel >= 19.8.28
BuildRequires: libspatialite-devel >= 4.3.0a
BuildRequires: sqlite-devel >= 3.22.0
BuildRequires: smartmet-library-locus-devel >= 19.8.28
BuildRequires: smartmet-library-macgyver-devel >= 19.8.2
BuildRequires: libatomic
BuildRequires: bzip2-devel
BuildRequires: fmt-devel >= 5.2.0
Requires: fmt >= 5.2.0
Requires: libconfig
Requires: smartmet-server >= 19.8.9
Requires: smartmet-engine-geonames >= 19.8.28
Requires: smartmet-library-spine >= 19.8.28
Requires: smartmet-library-locus >= 19.8.28
Requires: smartmet-library-macgyver >= 19.8.2
Requires: libatomic
Requires: unixODBC
Requires: libspatialite >= 4.3.0a
Requires: sqlite >= 3.22.0
Requires: boost-date-time
Requires: boost-iostreams
Requires: boost-locale
Requires: boost-serialization
Requires: boost-system
Requires: boost-thread
Obsoletes: smartmet-brainstorm-obsengine < 16.11.1
Obsoletes: smartmet-brainstorm-obsengine-debuginfo < 16.11.1
#TestRequires: make

Provides: %{SPECNAME}

%description
SmartMet engine for fetching observations from the climate database (cldb).

%package -n %{SPECNAME}-devel
Summary: SmartMet %{SPECNAME} development headers
Group: SmartMet/Development
Provides: %{SPECNAME}-devel
Requires: %{SPECNAME}
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
* Thu Sep 12 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.9.12-1.fmi
- Fixed two base classes to have a virtual destructor

* Thu Sep  5 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.9.5-1.fmi
- Added quick exits on shutdown to the station preload thread

* Wed Aug 28 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.8.28-1.fmi
- Added handling of optional fmisid provided with Location objects

* Fri Aug 23 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.8.23-2.fmi
- Skip protected stations

* Fri Aug 23 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.8.23-1.fmi
- Do not use stations without timezone information
- Optimized memory cache implementation

* Mon Aug  5 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.8.5-1.fmi
- Added a memory cache for narrow table observations

* Wed Jul 31 2019  Anssi Reponen <anssi.reponen@fmi.fi> - 19.7.31-1.fmi
- Fixed PostgreSQL cache update error (BRAINSTORM-1647)

* Tue Jul 30 2019  Anssi Reponen <anssi.reponen@fmi.fi> - 19.7.30-1.fmi
- Fixed incorrect field names in PostgreSQL cache (BRAINSTORM-1646)

* Wed Jul  3 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.7.3-1.fmi
- Added a memory cache for flashes

* Wed Jul  3 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.7.3-1.fmi
- Added a memory cache for flashes

* Fri Jun 28 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.6.28-1.fmi
- Fixed testing whether the given producer is available in the spatialite cache

* Thu Jun 27 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.6.27-2.fmi
- Fixed code to check if observations_fmi or observations_fmi_extaws data is in the SpatiaLite cache

* Thu Jun 27 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.6.27-1.fmi
- Revert to a single database due to speed issues
- Disable shared cache for improved speed

* Mon Jun 24 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.6.24-1.fmi
- Split flash and mobile data into separate spatialite cache files to avoid multiple write locks to the same database

* Thu Jun 20 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.6.20-1.fmi
- Added modified_last handling
- Discard values with data_quality > 5

* Sat Jun  8 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.6.8-1.fmi
- Optimized sqlite timestamp parser not to use boost::lexical case which causes (in GNU) global locale locks

* Thu May 23 2019 Anssi Reponen <anssi.reponen@fmi.fi> - 19.5.23-1.fmi
- Bugfix for mobile observations: area- and sounding_type-parameter taken into consideration when fetched data from cache
- Added log message of Dummy cache in creation
- Name of mobileAndExternalDataFilter changed to dataFilter in observation engine settings since it is used also in sounding-query

* Tue May 21 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.5.21-1.fmi
- Fixed SmartSymbol to be a known meta parameter

* Thu May  9 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.5.9-1.fmi
- Optimized nearest station searches to filter acceptable stations before calculating distances

* Thu May 2 2019 Anssi Reponen <anssi.reponen@fmi.fi> - 19.5.22-1.fmi
- Return missing-values for unknown parameters (BRAINSTORM-1520)

* Tue Apr 23 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.4.23-1.fmi
- It is possible to give period endtime in configuration file (fixedPeriodEndTime parameter). 
This is used in WMS-tests so that GetCapabilities response for observation layers doens't 
change from run to run.
- Dummy cache class added. This is used in WMS tests, where we need no cache-functionality.
- Support for mobile and external producers (curretly RoadCloud, NetAtmo)

* Mon Mar 18 2019 Santeri Oksman <santeri.oksman@fmi.fi> - 19.3.18-1.fmi
- Add support to data independent parameters

* Tue Dec  4 2018 Pertti Kinnia <pertti.kinnia@fmi.fi> - 18.12.4-1.fmi
- Repackaged since Spine::Table size changed

* Wed Nov 21 2018 Mika Heiskanen <heikki.pernu@fmi.fi> - 18.11.21-1.fmi
- Add library binary as the devel package dependency

* Mon Nov 12 2018 Mika Heiskanen <anssi.reponen@fmi.fi> - 18.11.12-1.fmi
- Add data_source-column automatically to observation_data and flash_data tables (BRAINSTORM-1233)

* Fri Nov  9 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.11.9-1.fmi
- Support for data_source-field added (BRAINSTORM-1233)

* Sat Sep 29 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.9.29-1.fmi
- Upgrade to latest fmt

* Fri Sep 28 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.9.28-1.fmi
- Sort nearby stations based on station ID, if the distance is identical to get stable results

* Wed Sep 26 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.9.26-1.fmi
- Fixed finding of nearest stations to check valid time periods correctly

* Tue Sep 18 2018 Anssi Reponen <anssi.reponen@fmi.fi> - 18.9.18-1.fmi
- Defined boost::shared_ptr<ParamterMap> and boost::shared_ptr<EngineParameyers> as const reference whenever used after creation

* Mon Sep  3 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.9.3-1.fmi
- Improved error messages if station info serialization fails

* Wed Aug 29 2018 Anssi Reponen <anssi.reponen@fmi.fi> - 18.8.29-1.fmi
- New ParameterMap class written and the corresponding variable is now accessed via boost::shared_ptr<const ParametrMap> (BRAINSTORM-1156)
- Parameter names in observation.conf made uppercase, so case conversions of these parameters has been removed

* Thu Aug 23 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.8.23-1.fmi
- Use requested time interval when deciding which stations are active

* Thu Aug 16 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.8.16-1.fmi
- Avoid boost::lexical_cast due to locale locks

* Mon Aug 13 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.8.13-1.fmi
- Repackaged since Spine::Location size changed

* Wed Jul 25 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.7.25-1.fmi
- Prefer nullptr over NULL

* Mon Jul 23 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.7.23-2.fmi
- Serialize station info to a temporary file first to prevent corruption during crashes

* Mon Jul 23 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.7.23-1.fmi
- Repackaged since spine ValueFormatter ABI changed

* Thu Jul 19 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.7.19-2.fmi
- Reduce competition between writers by doing one table update at a time

* Thu Jul 19 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.7.19-1.fmi
- Fixed option sqlite.shared_cache to have an effect
- Added option sqlite.read_uncommitted with default value false
- Allow sqlite.cache_size to be negative, which implies number of pages instead of bytes
- Use a single copy of the cache to hold information on rows previously inserted to the observation cache
- Removed unused member from DataItem which messed up cache key calculations

* Mon Jun 18 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.6.18-1.fmi
- Fixed a GROUP BY clause to include all columns required by PostGreSQL

* Fri Jun 15 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.6.15-1.fmi
- Use Fmi::to_iso_string for dates for speed
- Use COALESCE instead of IFNULL in PostGreSQL
- Fixed incorrect throw if location cache could not be updated

* Wed Jun 13 2018 Anssi Reponen <anssi.reponen@fmi.fi> - 18.6.13-1.fmi
- Dockerfile updated, reduntant console output removed
- Speed up insert performance of PostgreSQL-cache:
- All INSERT statemenst are put into one trasaction
- Own write-mutex dedicated for each table (before there was only one write-mutex for all tables)

* Thu Jun 7 2018 Anssi Reponen <anssi.reponen@fmi.fi> - 18.6.7-1.fmi
- Speed up insert performance of PostgreSQL-cache
  - All INSERT statemenst are put into one trasaction
  - Indexes are dropped before insert and re-created after insert
  - Own write-mutex dedicated for each table (before there was only one write-mutex for all tables)

* Mon Jun 4 2018 Anssi Reponen <anssi.reponen@fmi.fi> - 18.6.4-1.fmi
- Docker files for postgresql/postgis updated

* Tue May 29 2018 Anssi Reponen <anssi.reponen@fmi.fi> - 18.5.29-1.fmi
- Corrected duplicate handling during insert in observation_data and weather_data_qc tables

* Sun May 27 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.5.27-1.fmi
- Clean the cache every minute instead of every hour, which causes a major increase in response times

* Thu May 24 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.5.24-2.fmi
- Fixed time interval caching for flash and foreign stations

* Thu May 24 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.5.24-1.fmi
- Added caching of station ID requests to reduce sqlite pool size requirements

* Wed May 23 2018 Anssi Reponen <anssi.reponen@fmi.fi> - 18.5.23-1.fmi
- Docker files for postgresql/postgis added

* Tue May 22 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.5.22-1.fmi
- Added possibility to set sqlite temp_store pragma

* Mon May 21 2018 Anssi Reponen <anssi.reponen@fmi.fi> - 18.5.21-1.fmi
- Added support for PostgreSQL-cache

* Sun May 20 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.5.20-1.fmi
- Report more information if unserializing stations fails

* Fri May 11 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.5.11-1.fmi
- Use DateTimeParser instead of TimeParser for speed

* Thu Apr 19 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.4.19-1.fmi
- Cache available time intervals of cached data to reduce the number of sqlite connection requests

* Sat Apr  7 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.4.7-1.fmi
- Upgrade to boost 1.66

* Tue Apr  3 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.4.3-1.fmi
- Prefer yielding writer threads over sleeping to give more time for sqlite readers

* Tue Mar 20 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.20-2.fmi
- Added explicit initialization of some pointers

* Tue Mar 20 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.20-1.fmi
- Full repackaging of the server

* Mon Mar 19 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.19-2.fmi
- Removed Engine::setGeonames as obsolete

* Mon Mar 19 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.19-1.fmi
- obsengine will now register after all caches have been updated

* Thu Mar 15 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.15-2.fmi
- Replaced uses of boost::lexical_cast with non std::locale locking calls

* Thu Mar 15 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.15-1.fmi
- Use fmt::format instead of ostringstream to avoid std::locale locks

* Mon Mar 12 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.12-1.fmi
- Fixed spatialite min/max time methods to accept NULL SQL responses

* Sat Mar 10 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.10-1.fmi
- Use macgyver time to string conversions to avoid global locale locks

* Thu Mar  8 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.8-1.fmi
- Fixed SmartSymbol to return a missing value instead of throwing if required parameters are missing

* Mon Mar  5 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.5-1.fmi
- Added safety check against a missing measurand_id value

* Sat Mar  3 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.3-1.fmi
- Avoid locale copying when converting SQL timestamps to posix_time

* Thu Mar  1 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.3.1-1.fmi
- Avoid locale copying in case conversions

* Wed Feb 28 2018 Santeri Oksman <santeri.oksman@fmi.fi> - 18.2.28-2.fmi
- Fix wawa parameter handling in SmartSymbol calculation.

* Wed Feb 28 2018 Santeri Oksman <santeri.oksman@fmi.fi> - 18.2.28-1.fmi
- Added night/day calculcation to SmartSymbol

* Tue Feb 27 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.2.27-2.fmi
- Fixed EngineParameters to recognize the new SmartSymbol parameter
- Meta parameter names are now case independent

* Mon Feb 26 2018  <santeri.oksman@fmi.fi> - 18.2.27-1.fmi
- Added SmartSymbol parameter

* Fri Feb 23 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.2.23-1.fmi
- Avoid broken datetime() when retrieving flash data

* Thu Feb 22 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.2.22-1.fmi
- Do not report the engine to be ready before the connection pool is ready
- Improved cleaning logic

* Mon Feb 19 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.2.19-1.fmi
- Fixed shutdown to exit cleanly

* Fri Feb  9 2018 Mika Heiskanen <mika.heiskanen@fmi.fi> - 18.2.9-1.fmi
- Repackaged since base class SmartMetEngine size changed

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
