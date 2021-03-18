#pragma once
#include "ExternalAndMobileProducerId.h"
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using Measurands = std::map<std::string, int>;  // Parameter name -> measurand id

class ExternalAndMobileProducerMeasurand
{
 public:
  ExternalAndMobileProducerMeasurand(ProducerId theProducerId, const Measurands& theMeasurands);

  void addMeasurand(const std::string& theParameterName, int theMeasurandId);
  const Measurands& measurands() const { return itsMeasurands; }
  const ProducerId& producerId() const { return itsProducerId; }

 private:
  ProducerId itsProducerId;
  Measurands itsMeasurands;
};

// Maps producer to its configuration
class ExternalAndMobileProducerConfig
    : public std::map<std::string, ExternalAndMobileProducerMeasurand>
{
public:
  void setCached(bool cached);
  void setDatabaseTableName(const std::string& tablename); 
  bool getCached() const { return itsIsCached; } 
  const std::string&  getDatabaseTableName() const { return itsDatabaseTableName; }
 private:
  bool itsIsCached{false};
  std::string itsDatabaseTableName{"ext_obsdata"};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
