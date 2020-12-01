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
  bool cached{false};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
