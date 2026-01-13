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

TS::TimeSeriesVectorPtr DisabledEngine::values(Settings &settings)
{
  (void)settings;
  REPORT_DISABLED;
}

TS::TimeSeriesVectorPtr DisabledEngine::values(Settings &settings,
                                               const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  (void)settings;
  (void)timeSeriesOptions;
  REPORT_DISABLED;
}

void DisabledEngine::makeQuery(QueryBase *qb)
{
  (void)qb;
  REPORT_DISABLED;
}

FlashCounts DisabledEngine::getFlashCount(const Fmi::DateTime & /*starttime */,
                                          const Fmi::DateTime & /* endtime */,
                                          const Spine::TaggedLocationList & /*locations */)
{
  REPORT_DISABLED;
}

std::shared_ptr<std::vector<ObservableProperty>> DisabledEngine::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string &language)
{
  (void)parameters;
  (void)language;
  REPORT_DISABLED;
}

bool DisabledEngine::ready() const
{
  return true;
}

Geonames::Engine *DisabledEngine::getGeonames() const
{
  REPORT_DISABLED;
}

std::shared_ptr<DBRegistry> DisabledEngine::dbRegistry() const
{
  REPORT_DISABLED;
}

void DisabledEngine::reloadStations() {}

void DisabledEngine::getStations(Spine::Stations & /*stations*/, const Settings & /*settings*/)
{
  REPORT_DISABLED;
}

void DisabledEngine::getStationsByArea(Spine::Stations & /*stations*/,
                                       const Settings & /*settings*/,
                                       const std::string & /*areaWkt*/)
{
  REPORT_DISABLED;
}

void DisabledEngine::getStationsByBoundingBox(Spine::Stations & /*stations*/, const Settings & /*settings*/)
{
  REPORT_DISABLED;
}

bool DisabledEngine::isParameter(const std::string & /*alias*/, const std::string & /*stationType*/) const
{
  REPORT_DISABLED;
}

bool DisabledEngine::isParameterVariant(const std::string & /*name*/) const
{
  REPORT_DISABLED;
}

uint64_t DisabledEngine::getParameterId(const std::string & /*alias*/,
                                        const std::string & /*stationType*/) const
{
  REPORT_DISABLED;
}

std::string DisabledEngine::getParameterIdAsString(const std::string & /*alias*/,
                                                   const std::string & /*stationType*/) const
{
  REPORT_DISABLED;
}

std::set<std::string> DisabledEngine::getValidStationTypes() const
{
  REPORT_DISABLED;
}

ContentTable DisabledEngine::getProducerInfo(const std::optional<std::string> & /*producer*/) const
{
  REPORT_DISABLED;
}

ContentTable DisabledEngine::getParameterInfo(const std::optional<std::string> & /*producer*/) const
{
  REPORT_DISABLED;
}

ContentTable DisabledEngine::getStationInfo(const StationOptions & /*options*/) const
{
  REPORT_DISABLED;
}

MetaData DisabledEngine::metaData(const std::string & /*producer*/, const Settings & /*settings*/) const
{
  REPORT_DISABLED;
}

Spine::TaggedFMISIDList DisabledEngine::translateToFMISID(const Settings & /*settings*/,
                                                          const StationSettings & /*stationSettings*/) const
{
  REPORT_DISABLED;
}

const ProducerMeasurandInfo &DisabledEngine::getMeasurandInfo() const
{
  REPORT_DISABLED;
}

Fmi::DateTime DisabledEngine::getLatestDataUpdateTime(const std::string & /* producer */,
                                                      const Fmi::DateTime & /* from */) const
{
  REPORT_DISABLED;
}

void DisabledEngine::init() {}

void DisabledEngine::shutdown() {}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
