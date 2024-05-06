#pragma once

#include <boost/date_time/gregorian/gregorian.hpp>
#include <macgyver/DateTime.h>
#include <boost/optional.hpp>
#include <macgyver/ValueFormatter.h>
#include <spine/Location.h>
#include <spine/Parameter.h>
#include <spine/Station.h>
#include <timeseries/TimeSeriesInclude.h>
#include <locale>
#include <ostream>
#include <set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class Settings
{
 public:
  enum DebugOptions
  {
    DUMP_SETTINGS = 1
  };

  Spine::TaggedLocationList taggedLocations;
  Spine::TaggedFMISIDList taggedFMISIDs;
  std::vector<Spine::Parameter> parameters;
  std::vector<int> hours;
  std::vector<int> weekdays;
  std::locale locale{"fi_FI"};
  std::map<std::string, double> boundingBox;  // no default value

  // Filters mobile and external data and sounding data. Filtering is
  // based on given parameters, for example "stations_no" -> "1020,1046"
  // returns data rows only where station_no == 1020 or 1046
  //  std::map<std::string, std::vector<std::string>> dataFilter;
  TS::DataFilter dataFilter;

  std::set<uint> producer_ids;
  std::string cacheKey;
  std::string format = "ascii";
  std::string language = "fi";
  std::string localename = "fi_FI";
  std::string missingtext = "nan";
  std::string stationtype = "fmi";
  std::string stationtype_specifier;    // stationtype 'fmi_iot' may have specifier 'itmf'
  std::set<std::string> stationgroups;  // requested subset of station groups
  std::string timeformat = "timestamp";
  std::string timestring;
  std::string timezone = "localtime";
  std::string wktArea;
  Fmi::DateTime endtime = Fmi::SecondClock::universal_time();  // now
  Fmi::DateTime starttime =
      Fmi::SecondClock::universal_time() - Fmi::Hours(24);

  // starttime...endtime may actuall be a time interval from which we actually only want the
  // observation closest to a specific "wanted" time. The wanted time may actually be equal to the
  // end time if one wants the latest observation.
  boost::optional<Fmi::DateTime> wantedtime;

  double maxdistance = 50000;
  int numberofstations = 1;
  int timestep = 1;
  bool allplaces = false;
  bool starttimeGiven = false;
  bool useCommonQueryMethod = false;  // default is false
  bool useDataCache = true;           // default is true
  bool preventDatabaseQuery = false;
  TS::LocalTimePoolPtr localTimePool{nullptr};
  TS::RequestLimits requestLimits;
  // 0 or more bits from DebugOptions to enable debugging features
  uint32_t debug_options = 0;
};

std::ostream& operator<<(std::ostream& out, const Settings& settings);

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
