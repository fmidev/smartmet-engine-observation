#pragma once

#include "DatabaseDriverParameters.h"
#include "FmiIoTStation.h"
#include <boost/make_shared.hpp>
#include <macgyver/PostgreSQLConnection.h>

class FmiIoTStations;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct PostgreSQLDriverParameters : public DatabaseDriverParameters
{
  PostgreSQLDriverParameters(const std::string& drivername, const EngineParametersPtr& p)
      : DatabaseDriverParameters(drivername, p),
        externalAndMobileProducerConfig(params->externalAndMobileProducerConfig)
  {
    fmiIoTStations = std::make_shared<FmiIoTStations>();
  }

  std::vector<Fmi::Database::PostgreSQLConnectionOptions> connectionOptions;
  const ExternalAndMobileProducerConfig& externalAndMobileProducerConfig;
  std::shared_ptr<FmiIoTStations> fmiIoTStations;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
