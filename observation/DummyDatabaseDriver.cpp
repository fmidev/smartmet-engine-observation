#include "DummyDatabaseDriver.h"
#include "ObservableProperty.h"
#include "Utils.h"
#include <spine/Table.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
void DummyDatabaseDriver::init(Engine *obsengine) {}

TS::TimeSeriesVectorPtr DummyDatabaseDriver::values(Settings & /* settings */)
{
  return boost::make_shared<TS::TimeSeriesVector>();
}

TS::TimeSeriesVectorPtr DummyDatabaseDriver::values(Settings & /* settings */,
                                                    const TS::TimeSeriesGeneratorOptions &)
{
  return boost::make_shared<TS::TimeSeriesVector>();
}

Spine::TaggedFMISIDList DummyDatabaseDriver::translateToFMISID(
    const boost::posix_time::ptime & /* starttime */,
    const boost::posix_time::ptime & /* endtime */,
    const std::string & /* stationtype */,
    const StationSettings & /* stationSettings */) const
{
  return {};
}

void DummyDatabaseDriver::getMovingStationsByArea(Spine::Stations & /*stations*/,
                                                  const std::string & /*stationtype*/,
                                                  const boost::posix_time::ptime & /*starttime*/,
                                                  const boost::posix_time::ptime & /*endtime*/,
                                                  const std::string & /*wkt*/) const
{
}

FlashCounts DummyDatabaseDriver::getFlashCount(const boost::posix_time::ptime &,
                                               const boost::posix_time::ptime &,
                                               const Spine::TaggedLocationList &) const
{
  return {};
}

std::shared_ptr<std::vector<ObservableProperty>> DummyDatabaseDriver::observablePropertyQuery(
    std::vector<std::string> &, const std::string)
{
  return std::make_shared<std::vector<ObservableProperty>>();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
