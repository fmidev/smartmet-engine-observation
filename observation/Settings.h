#pragma once

#include <spine/Location.h>
#include <spine/Parameter.h>
#include <spine/ValueFormatter.h>

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/date_time/local_time_adjustor.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <locale>
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
  SmartMet::Spine::LocationList locations;
  SmartMet::Spine::TaggedLocationList taggedLocations;
  std::vector<SmartMet::Spine::Parameter> parameters;
  std::vector<int> area_geoids;
  std::vector<int> fmisids;
  std::vector<int> geoids;
  std::vector<int> hours;
  std::vector<int> lpnns;
  std::vector<int> weekdays;
  std::vector<int> wmos;
  std::vector<std::map<std::string, double>> coordinates;
  std::locale locale{"fi_FI"};
  std::map<std::string, double> boundingBox;  // no default value
  std::map<std::string, std::vector<std::string>> dataFilter;
  std::set<std::string> stationgroup_codes;
  std::set<uint> producer_ids;
  std::string cacheKey;
  std::string format = "ascii";
  std::string language = "fi";
  std::string localename = "fi_FI";
  std::string missingtext = "nan";
  std::string stationtype = "fmi";
  std::string timeformat = "timestamp";
  std::string timestring = "";
  std::string timezone = "localtime";
  std::string wktArea;
  boost::posix_time::ptime endtime = boost::posix_time::second_clock::universal_time(); // jow
  boost::posix_time::ptime starttime = boost::posix_time::second_clock::universal_time() - boost::posix_time::hours(24);
  double maxdistance = 50000;
  int numberofstations = 1;
  int timestep = 1;
  bool allplaces = false;
  bool boundingBoxIsGiven = false;
  bool latest = false;
  bool starttimeGiven = false;
  bool useCommonQueryMethod = false;  // default is false
  bool useDataCache = true;          // default is true

  // Filters mobile and external data and sounding data. Filtering is
  // based on given parameters, for example "stations_no" -> "1020,1046"
  // returns data rows only where station_no == 1020 or 1046
  
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
