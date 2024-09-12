#pragma once

#include "DatabaseDriverParameters.h"
#include "FmiIoTStation.h"
#include <boost/make_shared.hpp>
#include <macgyver/PostgreSQLConnection.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class FmiIoTStations;

struct PostgreSQLDriverParameters : public DatabaseDriverParameters
{
  PostgreSQLDriverParameters(const std::string& drivername, const EngineParametersPtr& p)
      : DatabaseDriverParameters(drivername, p),
        externalAndMobileProducerConfig(params->externalAndMobileProducerConfig),
        fmiIoTStations(std::make_shared<FmiIoTStations>())
  {
  }

  std::vector<Fmi::Database::PostgreSQLConnectionOptions> connectionOptions;
  const ExternalAndMobileProducerConfig& externalAndMobileProducerConfig;
  bool loadFmiIoTStations = true;
  std::shared_ptr<FmiIoTStations> fmiIoTStations;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
