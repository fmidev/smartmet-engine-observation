#pragma once

#include "ExternalAndMobileProducerConfig.h"
#include "FmiIoTStation.h"
#include "PostgreSQLObsDB.h"
#include "QueryBase.h"

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

  TS::TimeSeriesVectorPtr executeQuery(PostgreSQLObsDB &db,
                                       Settings &settings,
                                       const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr executeQuery(PostgreSQLObsDB &db,
                                       Settings &settings,
                                       const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
                                       const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr values(PostgreSQLObsDB &db,
                                 Settings &settings,
                                 const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr values(PostgreSQLObsDB &db,
                                 Settings &settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
                                 const Fmi::TimeZones &timezones);

 private:
  const ExternalAndMobileProducerConfig &itsProducerConfig;  // producer -> id map
  const std::shared_ptr<FmiIoTStations> &itsStations;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
