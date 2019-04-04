#include "ExternalAndMobileProducerConfig.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
ExternalAndMobileProducerMeasurand::ExternalAndMobileProducerMeasurand(
    ProducerId theProducerId, const Measurands& theMeasurands)
    : itsProducerId(theProducerId), itsMeasurands(theMeasurands)
{
  for (auto item : theMeasurands)
    itsMeasurandParameters.insert(std::make_pair(item.second, item.first));
}

void ExternalAndMobileProducerMeasurand::addMeasurand(const std::string& theParameterName,
                                                      int theMeasurandId)
{
  if (itsMeasurands.find(theParameterName) != itsMeasurands.end())
  {
    itsMeasurands[theParameterName] = theMeasurandId;
    itsMeasurandParameters[theMeasurandId] = theParameterName;
  }
  else
  {
    itsMeasurands.insert(std::make_pair(theParameterName, theMeasurandId));
    itsMeasurandParameters.insert(std::make_pair(theMeasurandId, theParameterName));
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
