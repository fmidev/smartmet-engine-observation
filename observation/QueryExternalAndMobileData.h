#pragma once

#include "FmiIoTStation.h"
#include "PostgreSQLObsDB.h"
#include "QueryBase.h"
#include "ExternalAndMobileProducerConfig.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

class QueryExternalAndMobileData : public QueryBase
{
 public:
  QueryExternalAndMobileData(
      const ExternalAndMobileProducerConfig &producerConfig,
      const boost::shared_ptr<FmiIoTStations> &stations);

  virtual ~QueryExternalAndMobileData();

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr executeQuery(
      PostgreSQLObsDB &db,
      Settings &settings,
      const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr executeQuery(
      PostgreSQLObsDB &db,
      Settings &settings,
      const SmartMet::Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr values(
      PostgreSQLObsDB &db,
      Settings &settings,
      const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr values(
      PostgreSQLObsDB &db,
      Settings &settings,
      const SmartMet::Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones);

 private:
  const ExternalAndMobileProducerConfig &itsProducerConfig;  // producer -> id map
  const boost::shared_ptr<FmiIoTStations> &itsStations;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet