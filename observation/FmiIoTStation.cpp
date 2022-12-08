#include "FmiIoTStation.h"
#include <gis/OGR.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
bool operator<(const FmiIoTStation& lhs, const FmiIoTStation& rhs)
{
  return ((lhs.station_id < rhs.station_id) || (lhs.valid_from < rhs.valid_from) || (lhs.valid_to < rhs.valid_to));
}

bool operator==(const FmiIoTStation& lhs, const FmiIoTStation& rhs)
{
  return ((lhs.station_id == rhs.station_id) && (lhs.valid_from == rhs.valid_from) && (lhs.valid_to == rhs.valid_to));
}

const FmiIoTStation emptyStation;

void FmiIoTStations::addStation(const FmiIoTStation& s)
{
  itsStations[s.station_id].insert(s);
}

void FmiIoTStations::addStation(const std::string& id,
                                int tgid,
                                double lon,
                                double lat,
                                double elev,
                                const boost::posix_time::ptime& from,
                                const boost::posix_time::ptime& to)
{
  itsStations[id].insert(FmiIoTStation(id, tgid, lon, lat, elev, from, to));
}

const FmiIoTStation& FmiIoTStations::getStation(const std::string& id,
                                                const boost::posix_time::ptime& t) const
{
  if (itsStations.find(id) == itsStations.end())
    return emptyStation;

  const FmiIoTStationSet& station_set = itsStations.at(id);

  for (const auto& s : station_set)
  {
    if (t >= s.valid_from && t <= s.valid_to)
      return s;
  }

  return emptyStation;
}

bool FmiIoTStations::isActive(const std::string& id, const boost::posix_time::ptime& t) const
{
  if (itsStations.find(id) == itsStations.end())
    return false;

  const FmiIoTStationSet& station_set = itsStations.at(id);

  for (const auto& s : station_set)
  {
    if (t >= s.valid_from && t <= s.valid_to)
      return true;
  }

  return false;
}

std::vector<const FmiIoTStation*> FmiIoTStations::getStations(const std::string& wktArea) const
{
  std::vector<const FmiIoTStation*> ret;

  if (wktArea.empty())
    return ret;

  std::set<std::string> processedStations;
  std::unique_ptr<OGRGeometry> stationGeom;
  std::unique_ptr<OGRGeometry> areaGeom;
  areaGeom.reset(Fmi::OGR::createFromWkt(wktArea, 4326));
  for (const auto& stationMap : itsStations)
  {
    if (processedStations.find(stationMap.first) != processedStations.end())
      continue;

    const FmiIoTStationSet& station_set = stationMap.second;

    for (const auto& s : station_set)
    {
      std::string stationPoint =
          ("POINT(" + Fmi::to_string(s.longitude) + " " + Fmi::to_string(s.latitude) + ")");
      stationGeom.reset(Fmi::OGR::createFromWkt(stationPoint, 4326));

      if (areaGeom->Contains(stationGeom.get()))
      {
        ret.push_back(&s);
        break;
      }
    }
    processedStations.insert(stationMap.first);
  }

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
