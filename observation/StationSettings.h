#pragma once
#include <boost/optional.hpp>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct GeoIdSettings
{
  std::vector<int> geoids;
  double maxdistance;
  int numberofstations;
  std::string language;
};

struct NearestStationSettings
{
  double longitude;
  double latitude;
  double maxdistance;
  int numberofstations;
  std::string tag;  // This is put in place parameter (station.tag)
  boost::optional<int> fmisid;

  NearestStationSettings(double lon, double lat, double maxd, int nos, std::string t)
      : longitude(lon),
        latitude(lat),
        maxdistance(maxd),
        numberofstations(nos),
        tag(std::move(t)),
        fmisid(boost::none)
  {
  }

  NearestStationSettings(
      double lon, double lat, double maxd, int nos, std::string t, boost::optional<int> sid)
      : longitude(lon),
        latitude(lat),
        maxdistance(maxd),
        numberofstations(nos),
        tag(std::move(t)),
        fmisid(sid)
  {
  }
};

using BoundingBoxSettings = std::map<std::string, double>;

struct StationSettings
{
  std::vector<int> lpnns;         // Legacy FMI station number
  std::vector<int> wmos;          // Legacy WMO station number
  std::vector<int> fmisids;       // FMI station number
  std::vector<int> rwsids;        // Finnish road weather station numbers
  std::vector<std::string> wsis;  // WIGOS Station identifier
  GeoIdSettings geoid_settings;   // Geonames settings
  std::vector<NearestStationSettings> nearest_station_settings;
  BoundingBoxSettings bounding_box_settings;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
