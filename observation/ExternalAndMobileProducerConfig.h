#pragma once
#include "ExternalAndMobileProducerId.h"
#include <macgyver/StringConversion.h>
#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using Measurands = std::map<std::string, int>;               // Parameter name -> measurand id
using MeasurandIdParameterMap = std::map<int, std::string>;  // Measurand id -> parameter name

class ExternalAndMobileProducerMeasurand
{
 public:
  ExternalAndMobileProducerMeasurand(ProducerId theProducerId, const Measurands& theMeasurands);

  void addMeasurand(const std::string& theParameterName, int theMeasurandId);
  const Measurands& measurands() const { return itsMeasurands; }
  const ProducerId& producerId() const { return itsProducerId; }
  const MeasurandIdParameterMap& measurandParameters() const { return itsMeasurandParameters; }

 private:
  ProducerId itsProducerId;
  Measurands itsMeasurands;
  MeasurandIdParameterMap itsMeasurandParameters;
};

class ExternalAndMobileProducerConfig
    : public std::map<std::string, ExternalAndMobileProducerMeasurand>
{
 public:
  bool cached{false};
};

// using ExternalAndMobileProducerConfig = std::map<std::string, ProducerMeasurand>;  // Producer
// name -> config

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
