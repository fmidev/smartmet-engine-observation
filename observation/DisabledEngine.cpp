#include "DisabledEngine.h"
#include <macgyver/Exception.h>

#ifdef DEBUG_ENGINE_DISABLING
#define DISABLE_STACKTRACE
#else
#define DISABLE_STACKTRACE .disableStackTrace()
#endif

#define DISABLED_MSG ": engine is disabled"
#define REPORT_DISABLED throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG) DISABLE_STACKTRACE

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
DisabledEngine::DisabledEngine() = default;

TS::TimeSeriesVectorPtr DisabledEngine::values(Settings& settings)
{
  return {};
}

TS::TimeSeriesVectorPtr DisabledEngine::values(
    Settings& settings, const TS::TimeSeriesGeneratorOptions& timeSeriesOptions)
{
  return {};
}

void DisabledEngine::makeQuery(QueryBase* qb)
{
  (void)qb;
  REPORT_DISABLED;
}

FlashCounts DisabledEngine::getFlashCount(const Fmi::DateTime& /*starttime */,
                                          const Fmi::DateTime& /* endtime */,
                                          const Spine::TaggedLocationList& /*locations */)
{
  return {};
}

std::shared_ptr<std::vector<ObservableProperty>> DisabledEngine::observablePropertyQuery(
    std::vector<std::string>& parameters, const std::string& language)
{
  return {};
}

bool DisabledEngine::ready() const
{
  return true;
}

Geonames::Engine* DisabledEngine::getGeonames() const
{
  return nullptr;
}

std::shared_ptr<DBRegistry> DisabledEngine::dbRegistry() const
{
  return {};
}

void DisabledEngine::reloadStations() {}

void DisabledEngine::getStations(Spine::Stations& /*stations*/, const Settings& /*settings*/) {}

void DisabledEngine::getStationsByArea(Spine::Stations& /*stations*/,
                                       const Settings& /*settings*/,
                                       const std::string& /*areaWkt*/)
{
}

void DisabledEngine::getStationsByBoundingBox(Spine::Stations& /*stations*/,
                                              const Settings& /*settings*/)
{
}

bool DisabledEngine::isParameter(const std::string& /*alias*/,
                                 const std::string& /*stationType*/) const
{
  return false;
}

bool DisabledEngine::isParameterVariant(const std::string& /*name*/) const
{
  return false;
}

uint64_t DisabledEngine::getParameterId(const std::string& /*alias*/,
                                        const std::string& /*stationType*/) const
{
  REPORT_DISABLED;
}

std::string DisabledEngine::getParameterIdAsString(const std::string& /*alias*/,
                                                   const std::string& /*stationType*/) const
{
  REPORT_DISABLED;
}

std::set<std::string> DisabledEngine::getValidStationTypes() const
{
  return {};
}

ContentTable DisabledEngine::getProducerInfo(const std::optional<std::string>& /*producer*/) const
{
  return {};
}

ContentTable DisabledEngine::getParameterInfo(const std::optional<std::string>& /*producer*/) const
{
  return {};
}

ContentTable DisabledEngine::getStationInfo(const StationOptions& /*options*/) const
{
  return {};
}

MetaData DisabledEngine::metaData(const std::string& /*producer*/,
                                  const Settings& /*settings*/) const
{
  return {};
}

Spine::TaggedFMISIDList DisabledEngine::translateToFMISID(
    const Settings& /*settings*/, const StationSettings& /*stationSettings*/) const
{
  return {};
}

const ProducerMeasurandInfo& DisabledEngine::getMeasurandInfo() const
{
  static ProducerMeasurandInfo empty{};
  return empty;
}

Fmi::DateTime DisabledEngine::getLatestDataUpdateTime(const std::string& /* producer */,
                                                      const Fmi::DateTime& /* from */) const
{
  REPORT_DISABLED;
}

void DisabledEngine::init() {}

void DisabledEngine::shutdown() {}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
