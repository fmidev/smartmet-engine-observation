#pragma once

#include <boost/date_time/posix_time/posix_time.hpp>
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
  FmiIoTStation() {}
  FmiIoTStation(const FmiIoTStation& s)
      : station_id(s.station_id),
        target_group_id(s.target_group_id),
        longitude(s.longitude),
        latitude(s.latitude),
        elevation(s.elevation),
        valid_from(s.valid_from),
        valid_to(s.valid_to)
  {
  }
  FmiIoTStation(const std::string& id,
                int tgid,
                double lon,
                double lat,
                double elev,
                const boost::posix_time::ptime& from,
                const boost::posix_time::ptime& to)
      : station_id(id),
        target_group_id(tgid),
        longitude(lon),
        latitude(lat),
        elevation(elev),
        valid_from(from),
        valid_to(to)
  {
  }

  std::string station_id{""};
  int target_group_id;
  double longitude;
  double latitude;
  double elevation{0};
  boost::posix_time::ptime valid_from{boost::posix_time::not_a_date_time};
  boost::posix_time::ptime valid_to{boost::posix_time::not_a_date_time};
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
                  const boost::posix_time::ptime& from,
                  const boost::posix_time::ptime& to);
  const FmiIoTStation& getStation(const std::string& id, const boost::posix_time::ptime& t) const;
  bool isActive(const std::string& id, const boost::posix_time::ptime& t) const;
  std::vector<const FmiIoTStation*> getStations(const std::string& wktArea) const;

 private:
  using FmiIoTStationSet = std::set<FmiIoTStation>;
  using FmiIoTStationMap = std::map<std::string, FmiIoTStationSet>;
  FmiIoTStationMap itsStations;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
