#include "ObservationCacheAdminSpatiaLite.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
ObservationCacheAdminSpatiaLite::ObservationCacheAdminSpatiaLite(
    const DatabaseDriverParameters& p,
    SmartMet::Engine::Geonames::Engine* geonames,
    std::atomic<bool>& conn_ok,
    bool timer)
    : ObservationCacheAdminBase(p, geonames, conn_ok, timer)
{
}

std::pair<Fmi::DateTime, Fmi::DateTime>
ObservationCacheAdminSpatiaLite::getLatestWeatherDataQCTime(
    const std::shared_ptr<ObservationCache>& /* cache */) const
{
  return {boost::posix_time::not_a_date_time, boost::posix_time::not_a_date_time};
}

std::pair<Fmi::DateTime, Fmi::DateTime>
ObservationCacheAdminSpatiaLite::getLatestObservationTime(
    const std::shared_ptr<ObservationCache>& /* cache */) const
{
  return {boost::posix_time::not_a_date_time, boost::posix_time::not_a_date_time};
}

std::map<std::string, Fmi::DateTime> ObservationCacheAdminSpatiaLite::getLatestFlashTime(
    const std::shared_ptr<ObservationCache>& /* cache */) const
{
  std::map<std::string, Fmi::DateTime> ret;

  ret["start_time"] = boost::posix_time::not_a_date_time;
  ret["last_stroke_time"] = boost::posix_time::not_a_date_time;
  ret["last_modified_time"] = boost::posix_time::not_a_date_time;

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
