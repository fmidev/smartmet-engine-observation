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
}

void ExternalAndMobileProducerMeasurand::addMeasurand(const std::string& theParameterName,
                                                      int theMeasurandId)
{
  if (itsMeasurands.find(theParameterName) != itsMeasurands.end())
  {
    itsMeasurands[theParameterName] = theMeasurandId;
  }
  else
  {
    itsMeasurands.insert(std::make_pair(theParameterName, theMeasurandId));
  }
}

void ExternalAndMobileProducerConfig::setCached(bool cached)
{
  itsIsCached = cached;
}

void ExternalAndMobileProducerConfig::setDatabaseTableName(const std::string& tablename)
{
  itsDatabaseTableName = tablename;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
