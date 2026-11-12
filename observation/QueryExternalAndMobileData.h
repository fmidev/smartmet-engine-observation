#pragma once

#include "ExternalAndMobileProducerConfig.h"
#include "QueryBase.h"
#include <timeseries/TimeSeries.h>
#include <timeseries/TimeSeriesGeneratorOptions.h>
#include <timeseries/TimeSeriesInclude.h>
#include <memory>

namespace Fmi
{
class TimeZones;
}  // namespace Fmi
namespace SmartMet::Engine::Observation
{
class FmiIoTStations;
class PostgreSQLObsDB;
class Settings;
}  // namespace SmartMet::Engine::Observation

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryExternalAndMobileData : public QueryBase
{
 public:
  QueryExternalAndMobileData(const ExternalAndMobileProducerConfig &producerConfig,
                             const std::shared_ptr<FmiIoTStations> &stations);

  ~QueryExternalAndMobileData() override;
  QueryExternalAndMobileData() = delete;

  TS::TimeSeriesVectorPtr values(PostgreSQLObsDB &db,
                                 const Settings& settings,
                                 const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr values(PostgreSQLObsDB &db,
                                 const Settings& settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
                                 const Fmi::TimeZones &timezones);

 private:
  const ExternalAndMobileProducerConfig &itsProducerConfig;  // producer -> id map
  const std::shared_ptr<FmiIoTStations> &itsStations;

  TS::TimeSeriesVectorPtr executeQuery(PostgreSQLObsDB &db,
                                       const Settings& settings,
                                       const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr executeQuery(PostgreSQLObsDB &db,
                                       const Settings& settings,
                                       const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
                                       const Fmi::TimeZones &timezones);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
