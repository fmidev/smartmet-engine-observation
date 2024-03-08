#pragma once

#include <macgyver/DateTime.h>
#include <map>
#include <set>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct FmiIoTStation
{
  FmiIoTStation() = default;
  FmiIoTStation(std::string id,
                int tgid,
                double lon,
                double lat,
                double elev,
                const Fmi::DateTime& from,
                const Fmi::DateTime& to)
      : station_id(std::move(id)),
        target_group_id(tgid),
        longitude(lon),
        latitude(lat),
        elevation(elev),
        valid_from(from),
        valid_to(to)
  {
  }

  std::string station_id;
  int target_group_id = 0;
  double longitude = 0;
  double latitude = 0;
  double elevation = 0;
  Fmi::DateTime valid_from{Fmi::DateTime::NOT_A_DATE_TIME};
  Fmi::DateTime valid_to{Fmi::DateTime::NOT_A_DATE_TIME};
};

class FmiIoTStations
{
 public:
  void addStation(const FmiIoTStation& s);
  void addStation(const std::string& id,
                  int tgid,
                  double lon,
                  double lat,
                  double elev,
                  const Fmi::DateTime& from,
                  const Fmi::DateTime& to);
  const FmiIoTStation& getStation(const std::string& id, const Fmi::DateTime& t) const;
  bool isActive(const std::string& id, const Fmi::DateTime& t) const;
  std::vector<const FmiIoTStation*> getStations(const std::string& wktArea) const;

 private:
  using FmiIoTStationSet = std::set<FmiIoTStation>;
  using FmiIoTStationMap = std::map<std::string, FmiIoTStationSet>;
  FmiIoTStationMap itsStations;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
