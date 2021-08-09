%define DIRNAME observation
%define LIBNAME smartmet-%{DIRNAME}
%define SPECNAME smartmet-engine-%{DIRNAME}
Summary: SmartMet Observation Engine
Name: %{SPECNAME}
Version: 21.8.9
Release: 1%{?dist}.fmi
License: FMI
Group: SmartMet/Engines
URL: https://github.com/fmidev/smartmet-engine-observation
Source0: %{name}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildRequires: boost169-devel
BuildRequires: bzip2-devel
BuildRequires: fmt-devel >= 7.1.3
BuildRequires: gcc-c++
BuildRequires: gdal32-devel
BuildRequires: libatomic
BuildRequires: libconfig-devel
BuildRequires: make
BuildRequires: rpm-build
BuildRequires: smartmet-engine-geonames-devel >= 21.7.8
BuildRequires: smartmet-library-locus-devel >= 21.7.8
BuildRequires: smartmet-library-macgyver-devel >= 21.7.27
BuildRequires: smartmet-library-spine-devel >= 21.7.28
BuildRequires: sqlite-devel >= 3.22.0
BuildRequires: zlib-devel
Requires: boost169-date-time
Requires: boost169-iostreams
Requires: boost169-locale
Requires: boost169-serialization
Requires: boost169-system
Requires: boost169-thread
Requires: fmt >= 7.1.3
Requires: gdal32-libs
Requires: libatomic
Requires: libconfig
Requires: smartmet-engine-geonames >= 21.7.8
Requires: smartmet-library-locus >= 21.7.8
Requires: smartmet-library-macgyver >= 21.7.27
Requires: smartmet-library-spine >= 21.7.28
Requires: smartmet-server >= 21.6.3
Requires: sqlite >= 3.22.0
Requires: unixODBC

%if %{defined el7}
Requires: libpqxx < 1:7.0
BuildRequires: libpqxx-devel < 1:7.0
%else
%if %{defined el8}
Requires: libpqxx >= 5.0.1
BuildRequires: libpqxx-devel >= 5.0.1
%else
Requires: libpqxx
BuildRequires: libpqxx-devel
%endif
%endif

Obsoletes: smartmet-brainstorm-obsengine < 16.11.1
Obsoletes: smartmet-brainstorm-obsengine-debuginfo < 16.11.1
#TestRequires: make
#TestRequires: gcc-c++
#TestRequires: gdal32-devel
#TestRequires: sqlite-devel >= 3.22.0
#TestRequires: bzip2-devel
#TestRequires: zlib-devel
#TestRequires: smartmet-engine-geonames >= 21.7.8
#TestRequires: smartmet-library-macgyver >= 21.7.27
#TestRequires: smartmet-library-spine >= 21.7.28
#TestRequires: smartmet-test-data

%if 0%{rhel} >= 8
Requires: libspatialite50
BuildRequires: libspatialite50-devel
#TestRequires: libspatialite50-devel
%else
Requires: libspatialite43
BuildRequires: libspatialite43-devel
#TestRequires: libspatialite43-devel
%endif

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
* Mon Aug 9 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.8.9-1.fmi
- Flash emulator implemented (BRAINSTORM-2126)

* Thu Aug 5 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.8.5-1.fmi
- Added support for new mobile producer 'bk_hydrometa' (BRAINSTORM-2125)

* Sat Jul 31 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.7.31-1.fmi
- Use boost::atomic_shared_ptr for better thread safety

* Wed Jul 28 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.7.28-1.fmi
- Silenced more compiler warnings

* Tue Jul 27 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.7.27-1.fmi
- Silenced several compiler warnings

* Tue Jul 13 2021 Andris Pavēnis <andris.pavenis@fmi.fi> 21.7.13-1.fmi
- Update according to macgyver API changes

* Thu Jul  8 2021 Andris Pavēnis <andris.pavenis@fmi.fi> 21.7.8-1.fmi
- Use libpqxx7 for RHEL8

* Mon Jul  5 2021 Andris Pavēnis <andris.pavenis@fmi.fi> 21.7.5-1.fmi
- Move DataFilter to smartmet-library-spine

* Wed Jun 30 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.6.30-1.fmi
- Simplified ObservationMemoryCache::clean
- Fixed ObservationMemoryCache::read_observations to use std::lower_bound instead of std::upper_bound

* Tue Jun 29 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.6.29-2.fmi
- Fixed FlashMemoryCache::getData to handle the end of the flash data correctly

* Thu Jun 24 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.6.24-1.fmi
- Fixed weather_data_qc cache updates logic : sometimes the data contains dates in the future

* Wed Jun 23 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.6.23-3.fmi
- Fixed sorting of memory cache observations. RHEL7 std::inplace_merge seems to be bugged.

* Wed Jun 23 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.6.23-2.fmi
- Fixed a bug in parsing OR/AND clauses in SQLDataFilter

* Wed Jun 23 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.6.23-1.fmi
- Fixed safety issues in ObservationMemoryCache and FlashMemoryCache

* Wed Jun 16 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.6.16-1.fmi
- Fixed PostgreSQL connection pool to use all members of the pool more efficiently to keep the connections alive

* Mon Jun 14 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.6.14-1.fmi
- Fixed handling of NULL modified_last in weather_data_qc caches

* Thu Jun 10 2021 Andris Pavenis <andris.pavenis@fmi.fi> 21.6.10-1.fmi
- Speed up engine shutdown: interruption points and postgresql operation cancel

* Mon May 31 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.5.31-2.fmi
- Use modified_last as search condition in PG flash_data updates
- sqlite cache weather_data_qc and flash_data modified_last is now not null with default 1970-01-01
- sqlite cache weather_data_qc value column may now be null in case quality control decides to reset the value

* Mon May 31 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.5.31-1.fmi
- Use modified_last as search condition in PG weather_data_qc updates

* Thu May 27 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.5.27-1.fmi
- Sorted cache data item members into descending order by size to avoid unnecessary padding

* Thu May 20 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.5.20-2.fmi
- Repackaged with improved hashing functions

* Thu May 20 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.5.20-1.fmi
- Use Fmi hash functions, boost::hash_combine produces too many collisions

* Wed May 19 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.5.19-1.fmi
- Fixed handling of empty result set (BRAINSTORM-2061)

* Tue May 18 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.5.18-1.fmi
- Fixed data_quality condition in SQL statements (BRAINSTORM-2063)
- Fixed reading of default data_quality condition from configuration file
- Added 'eq' operator in SQLDataFilter

* Mon May 17 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.5.17-1.fmi
- Fixed ObservationCacheAdmihnPostgreSQL to return correct last observation times
- Use only modified_last in PG cache updates, adding an OR for modified_last is very slow

* Tue Apr 20 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.4.20-1.fmi
- Added support for station info request (SMARTMET-2039)

* Tue Apr 6 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.4.6-1.fmi
- Load stations from database and update stations.txt when loadStations=true (BRAINSTORM-2036)

* Wed Mar 31 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.3.31-1.fmi
- Fixed reloadStations to work even if both Oracle and mobile observations are active
- Dropped stations table from the spatialite cache as unnecessary
- Updated default stations.txt to latest information from FMI

* Tue Mar 23 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.3.23-1.fmi
- If WeatherNumber requested from observation engine return null values (BRAINSTORM-1484)

* Thu Mar 18 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.3.18-1.fmi
- Database table name configurable for mobile observations (BRAINSTORM-2022)

* Mon Mar 15 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.3.15-1.fmi
- Fixed handling of missing timesteps (BRAINSTORM-2028)

* Fri Mar 12 2021 Pertti Kinnia <pertti.kinnia@fmi.fi> - 21.3.12-1.fmi
- Crash prevention by ensuring timeseries vector end boundary is not exceeded

* Thu Mar 11 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.3.11-1.fmi
- Map legacy producer 'fmi' to 'observations_fmi'

* Wed Mar 10 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.3.10-1.fmi
- Removed legacy mapping of most producers to observations_fmi in the spatialite/postgresql caches

* Tue Mar  9 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.3.9-1.fmi
- Fixed insertion of NULL values into the sqlite cache

* Mon Mar  8 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.3.8-1.fmi
- Small improvement to error messages

* Tue Mar  2 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.3.2-3.fmi
- Fixed groupok function not to copy the input set of stations

* Tue Mar  2 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.3.2-2.fmi
- Avoid unnecessary LocationDataItems copying

* Tue Mar  2 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.3.2-1.fmi
- Use emplace_back when possible for speed

* Thu Feb 25 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.2.25-1.fmi
- Producer parameter info table in a form more suitable for column sorting and searching

* Thu Feb 11 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.2.11-1.fmi
- Fixed special parameter handling in spatialite queries. No more null values 
for special parameters when timestep is not found in data.

* Wed Feb 10 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.2.10-1.fmi
- Fixed stationlongitude and -latitude to work in direct spatialite queries

* Tue Feb 9 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.2.9-1.fmi
- Timestep-option doesn't work (BRAINSTORM-2003) 

* Thu Feb 4 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.2.4-1.fmi
- Remove possible sensor number from parameter name in observablePropertyQuery (INSPIRE-874)

* Wed Feb 3 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.2.3-1.fmi
- Make it possible to optionally prevent database queries (INSPIRE-914)

* Tue Feb  2 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.2.2-1.fmi
- Re-enabled immutable spatialite caches

* Mon Feb 1 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.2.1-1.fmi
- Sensor number stored as integer in internal data structures (BRAINSTORM-1951)

* Mon Jan 25 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.1.25-2.fmi
- Allow NULL values in the spatialite cache

* Mon Jan 25 2021 Anssi Reponen <anssi.reponen@fmi.fi> - 21.1.25-1.fmi
- Report more info about producers, data, parameters (BRAINSTORM-1981)

* Thu Jan 14 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.1.14-2.fmi
- Fixed caches to ignore the end time of the cache, otherwise flash queries go to the database in the winter

* Thu Jan 14 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.1.14-1.fmi
- Repackaged smartmet to resolve debuginfo issues

* Wed Jan 13 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.1.13-1.fmi
- Updated to latest dependencies

* Tue Jan 12 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.1.12-1.fmi
- Fixed spatialite dependency

* Tue Jan  5 2021 Andris Pavenis <andris.pavenis@fmi.fi> - 21.1.5-2.fmi
- Use libspatialite from pgdg repo

* Tue Jan  5 2021 Mika Heiskanen <mika.heiskanen@fmi.fi> - 21.1.5-1.fmi
- Upgrade to fmt 7.1.3

* Tue Dec 29 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.12.29-1.fmi
- Enabled immutable read only databases for test purposes

* Tue Dec 15 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.12.15-1.fmi
- Upgrade to pgdg12

* Mon Dec  7 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.12.7-2.fmi
- Minor fixes to silence CodeChecker warnings

* Mon Dec  7 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.12.7-1.fmi
- Fixed afterQuery not to throw if the query result is empty

* Thu Nov 26 2020 Anssi Reponen <anssi.reponen@fmi.fi> - 20.11.26-1.fmi
- Stations sorted accodrding to distance (BRAINSTORM-1976)
- Refactoring: moved special parameter related code from delfoi library to engine

* Mon Nov 23 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.11.23-1.fmi
- Sort nearest stations first by distance and then by station name to get deterministic ordering

* Mon Nov 16 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.11.16-1.fmi
- Fixed flash cache updates

* Mon Nov 9 2020 Anssi Reponen <anssi.reponen@fmi.fi> - 20.11.9-1.fmi
- Data type of paramter-field in weather_data_qc table changed to integer
- Fixed handling of stroke_location field when inserting data in weather_data_qc table in sqlite cache

* Wed Nov  4 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.11.4-2.fmi
- Improved cache update locking logic

* Wed Nov  4 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.11.4-1.fmi
- A dummy driver is implicitly created if no active drivers are registered

* Tue Nov 3 2020 Anssi Reponen <anssi.reponen@fmi.fi> - 20.11.3-1.fmi
- PostgreSQL code moved from delfoi-library to engine

* Thu Oct 29 2020 Anssi Reponen <anssi.reponen@fmi.fi> - 20.10.29-1.fmi
- Datetime-fields changed to integer in SpatiaLite cache (BRAINSTORM-1950)
- Recactoring: SpatiaLite code moved from delfoi-library to engine
- Fixed code, so that it is possible to cache data of fmi_iot producer

* Wed Oct 28 2020 Andris Pavenis <andris.pavenis@fmi.fi> - 20.10.28-1.fmi
- Rebuild due to fmt upgrade

* Thu Oct 22 2020 Anssi Reponen <anssi.reponen@fmi.fi> - 20.10.22-1.fmi
- New fetures:
- SpatiaLite database driver added: reads observations only from cache
- Fake cache feature added: loads data from Oracle to spatialite file
- Bugfixes: 
- WHERE-condition corrected when fetching flash data from Oracle to cache (BRAINSTORM-1943) 
- Solar and mareograph producers must use its own parameter number (not of observations_fmi) when queried from cache
- Handling of data_source field NULL-value when fetching data from HAV database
- Order of stations in result set must be identical whether data is fetched from HAV database or cache

* Fri Oct  9 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.10.9-1.fmi
- Use DateTimeParser for speed

* Mon Oct  5 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.10.5-2.fmi
- Added created column to flash_data schema
- Added modified_last index to flash_data schema

* Mon Oct  5 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.10.5-1.fmi
- Added modified_last column to weather_data_qc schema
- Added missing SRID to some flash queries

* Tue Sep 29 2020 Anssi Reponen <anssi.reponen@fmi.fi> - 20.9.29-1.fmi
- PostgreSQL-database driver completed (BRAINSTORM-1783,BRAINSTORM-1678)
- The source database is resolved at runtime based on producer and requested period
- When updating flash data in cache last_modified field is utilized (BRAINSTORM-1910)

* Wed Sep 23 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.9.23-1.fmi
- Use Fmi::Exception instead of Spine::Exception

* Fri Sep 18 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.9.18-1.fmi
- Faster station searches by accepting the first match

* Tue Sep  1 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.9.1-1.fmi
- Cache handling added in PostgreSQL-driver (BRAINSTORM-1783)
- Support for both 'itmf' and 'fmi_iot' producer names (INSPIRE-909)
- Station info is now read into memory at startup and can ce reloaded either 
via admin-plugin (what=reloadstations) or database driver can start reload-loop 
in its own thread (BRAINSTORM-1856)

* Fri Aug 21 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.8.21-1.fmi
- Upgrade to fmt 6.2

* Fri Aug  7 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.8.7-1.fmi
- Fixed sqlite and potsgresql caches to require correct producer_id numbers too

* Tue Jul 28 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.7.28-1.fmi
- Fixed station number searches to also check the station group is correct

* Tue Jul 21 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.7.21-1.fmi
- Added optional fmisid information to locations to avoid unnecessary coordinate searches

* Wed Jun 17 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.6.17-1.fmi
- Fixed observation_data to use sensor_no in the primary key

* Wed Jun 10 2020 Andris Pavēnis <andris.pavenis@fmi.fi> - 20.6.10-1.fmi
- Add possibility to provide debugging options in calls to obsengine

* Mon Jun  8 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.6.8-1.fmi
- Upgraded libpqxx

* Mon Jun 8 2020  Anssi Reponen <anssi.reponen@fmi.fi> - 20.6.8-2.fmi
- PostgresSQL-driver for mirrored Oracle data (BRAINSTORM-1783)
- Support for itmf-producer (INSPIRE-909)

* Wed May 27 2020 Anssi Reponen <anssi.reponen@fmi.fi> - 20.5.27-2.fmi
- Fixed sensor queries from memory cache

* Wed May 27 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.5.27-1.fmi
- Use atomic_load for shared station information

* Mon May 18 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.5.18-3.fmi
- Fixed getLatest/OldestTime type methods to return not_a_date_time on missing tables

* Mon May 18 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.5.18-2.fmi
- Fixed checking whether the given time period is cached or not

* Mon May 18 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.5.18-1.fmi
- Fixed sqlite timestamps to use 'T'

* Wed May 13 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.5.13-1.fmi
- Removed obsolete tables from the default stations.sqlite database
- Added sensor_no column to the default stations.sqlite database

* Tue May 12 2020  Anssi Reponen <anssi.reponen@fmi.fi> - 20.5.12-1.fmi
- Major code refactoring. Big changes in Engine-interface (BRAINSTORM-1678)
- Stations (FMISIDs) are resolved in a seprate function call before actual observation-query
- All relevant information of stations is kept in memory, as a result the following tables 
has been removed from cache: STATIONS, STATION_GROUPS, GROUP_MEMBERS, LOCATIONS
- Support for sensors added, as a result the related tickets has been fixed (BRAINSTORM-1549)
- Fixed non-active station bug (BRAINSTORM-1718,BRAINSTORM-568,BRAINSTORM-569)
- Fixed bug in query with numberofstations-option (BRAINSTORM-1609)
- Added support for data_quality option (BRAINSTORM-1706)

* Sat Apr 18 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.4.18-1.fmi
- Upgraded to Boost 1.69

* Wed Mar 18 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.3.18-1.fmi
- Faster sorting of observations in the memory cache

* Wed Mar 11 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.3.11-1.fmi
- Fixed sorting of observations in the memory cache

* Mon Mar  9 2020 Andris Pavēnis <andris.pavenis@fmi.fi> - 20.3.9-1.fmi
- Remove more functionality now in smartmet-library-spine (is_time_parameter)

* Fri Mar  6 2020 Andris Pavēnis <andris.pavenis@fmi.fi> - 20.3.6-1.fmi
- Remove functionality now in smartmet-library-spine (Engine::isSpecialParameter, Engine::makeParameter)

* Thu Feb 20 2020 Andris Pavēnis <andris.pavenis@fmi.fi> - 20.2.20-1.fmi
- New method Engine::isSpecialParameter

* Mon Feb 10 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.2.10-1.fmi
- Updated default stations.txt and stations.sqlite

* Sun Feb  9 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.2.9-1.fmi
- Fixed handling of missing wmo and lpnn numbers

* Fri Feb  7 2020 Mika Heiskanen <mika.heiskanen@fmi.fi> - 20.2.7-1.fmi
- Spine::Station API changed to use default construction

* Thu Dec 19 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.12.19-2.fmi
- Preprocess station groups using StationInfo instead of using station_groups etc tables

* Thu Dec 19 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.12.19-1.fmi
- Added indexes to station_groups and group_members

* Tue Dec 17 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.12.17-2.fmi
- Add separate observation time indexes to weather_data_qc and flash_data tables for extra speed

* Tue Dec 17 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.12.17-1.fmi
- Added default value for modified_last field when cache is created from scratch

* Mon Dec 16 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.12.16-1.fmi
- Reordered spatialite cache columns for efficiency - group primary key columns together
- Add modified_last index to newly created observation_data caches

* Tue Nov 26 2019  Anssi Reponen <anssi.reponen@fmi.fi> - 19.11.26-1.fmi
- Station group must be checked when reading stations/observations from cache (BRAINSTORM-1722)

* Wed Nov 20 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.11.20-1.fmi
- Rebuilt since Spine::Parameter size changed

* Thu Oct 31 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.10.31-1.fmi
- Rebuilt due to newbase API/ABI changes

* Wed Oct 30 2019  Anssi Reponen <anssi.reponen@fmi.fi> - 19.10.30-1.fmi
- Add spatial indexes at startup to cache database tables flash_data, ext_obsdata_netatmo, ext_obsdata_roadcloud (BRAINSTORM-1716)

* Tue Oct 29 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.10.29-1.fmi
- Fixed data_source handling when reading observations from the cache

* Mon Oct 28 2019  Anssi Reponen <anssi.reponen@fmi.fi> - 19.10.28-1.fmi
- Add missing value for data_source-field when fetching data from cache (BRAINSTORM-1711)

* Mon Oct 21 2019  Anssi Reponen <anssi.reponen@fmi.fi> - 19.10.21-1.fmi
- Added new function to get parameter id as string (related to INSPIRE-889)

* Wed Oct  2 2019  Anssi Reponen <anssi.reponen@fmi.fi> - 19.10.2-1.fmi
- Bugfix in dummy database driver to prevent segfault (BRAINSTORM-1381)

* Thu Sep 26 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.9.26-1.fmi
- Added support for ASAN & TSAN builds
- Explicit initialization of POD types (ASAN)
- Avoid locale locks by not using regex for matching simple numbers

* Tue Sep 17 2019  Anssi Reponen <anssi.reponen@fmi.fi> - 19.9.17-1.fmi
- Modify SQL-queries of mobile data and of queries where data_source-field is present, so that we can get rid of ResultSet class (BRAINSTORM-1673)
- Improve updating of mobile data cache by utilizing 'created' field in the query
- Fix bug in iterator of sqlite3pp-API query_iterator-constructor
- Add missing try-catch blocks and re-write some exception messages to be more accurate

* Fri Sep 13 2019 Mika Heiskanen <mika.heiskanen@fmi.fi> - 19.9.13-1.fmi
- Do not throw in destructors

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
