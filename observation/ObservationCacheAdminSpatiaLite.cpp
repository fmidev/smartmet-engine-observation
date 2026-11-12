#include "ObservationCacheAdminSpatiaLite.h"
#include <map>
#include <utility>

namespace SmartMet::Engine::Geonames
{
class Engine;
}  // namespace SmartMet::Engine::Geonames
namespace SmartMet::Engine::Observation
{
struct DatabaseDriverParameters;
}  // namespace SmartMet::Engine::Observation

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

std::pair<Fmi::DateTime, Fmi::DateTime> ObservationCacheAdminSpatiaLite::getLatestWeatherDataQCTime(
    const std::shared_ptr<ObservationCache>& /* cache */) const
{
  return {Fmi::DateTime::NOT_A_DATE_TIME, Fmi::DateTime::NOT_A_DATE_TIME};
}

std::pair<Fmi::DateTime, Fmi::DateTime> ObservationCacheAdminSpatiaLite::getLatestObservationTime(
    const std::shared_ptr<ObservationCache>& /* cache */) const
{
  return {Fmi::DateTime::NOT_A_DATE_TIME, Fmi::DateTime::NOT_A_DATE_TIME};
}

std::map<std::string, Fmi::DateTime> ObservationCacheAdminSpatiaLite::getLatestFlashTime(
    const std::shared_ptr<ObservationCache>& /* cache */) const
{
  std::map<std::string, Fmi::DateTime> ret;

  ret["start_time"] = Fmi::DateTime::NOT_A_DATE_TIME;
  ret["last_stroke_time"] = Fmi::DateTime::NOT_A_DATE_TIME;
  ret["last_modified_time"] = Fmi::DateTime::NOT_A_DATE_TIME;

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
