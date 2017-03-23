#ifndef OBSENGINE_SETTINGS_H
#define OBSENGINE_SETTINGS_H

#include <spine/Parameter.h>
#include <spine/Location.h>
#include <spine/ValueFormatter.h>

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/date_time/local_time_adjustor.hpp>
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
  Settings();

  bool allplaces;
  std::map<std::string, double> boundingBox;  // no default value
  boost::posix_time::ptime endtime;
  std::vector<int> fmisids;
  std::string format;
  std::vector<int> area_geoids;
  std::vector<int> geoids;
  std::vector<int> hours;
  std::string language;
  bool latest;
  std::string localename;
  std::locale locale;
  std::vector<int> lpnns;
  double maxdistance;
  std::string missingtext;
  SmartMet::Spine::LocationList locations;
  SmartMet::Spine::TaggedLocationList taggedLocations;
  std::vector<std::map<std::string, double> > coordinates;
  int numberofstations;
  std::vector<SmartMet::Spine::Parameter> parameters;
  boost::posix_time::ptime starttime;
  bool starttimeGiven;
  std::string stationtype;
  std::string timeformat;
  int timestep;
  std::string timestring;
  std::string timezone;
  std::vector<int> weekdays;
  std::vector<int> wmos;
  bool boundingBoxIsGiven;
  std::string wktArea;

  std::string cacheKey;

  std::set<std::string> stationgroup_codes;
  std::set<uint> producer_ids;
  bool useDataCache;  // default is true
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

#endif
