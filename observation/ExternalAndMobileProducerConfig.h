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

class ExternalAndMobileProducerConfigItem
{
 public:
  ExternalAndMobileProducerConfigItem(const ProducerId& theProducerId,
                                      const Measurands& theMeasurands,
                                      const std::string& theDatabaseTable)
      : itsProducerId(theProducerId),
        itsMeasurands(theMeasurands),
        itsDatabaseTable(theDatabaseTable)
  {
  }

  const ProducerId& producerId() const { return itsProducerId; }
  const Measurands& measurands() const { return itsMeasurands; }
  const std::string& databaseTable() const { return itsDatabaseTable; }

 private:
  ProducerId itsProducerId;
  Measurands itsMeasurands;
  std::string itsDatabaseTable;
};

using ExternalAndMobileProducerConfig = std::map<std::string, ExternalAndMobileProducerConfigItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
