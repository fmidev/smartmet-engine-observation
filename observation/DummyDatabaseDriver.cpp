#include "DummyDatabaseDriver.h"
#include "ObservableProperty.h"
#include "Utils.h"
#include <spine/Table.h>

namespace ts = SmartMet::Spine::TimeSeries;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
void DummyDatabaseDriver::init(Engine *obsengine)
{
  itsParameters->observationCache->initializeConnectionPool();
}

ts::TimeSeriesVectorPtr DummyDatabaseDriver::values(Settings &settings)
{
  return boost::make_shared<ts::TimeSeriesVector>();
}

ts::TimeSeriesVectorPtr DummyDatabaseDriver::values(Settings &settings,
                                                    const Spine::TimeSeriesGeneratorOptions &)
{
  return boost::make_shared<ts::TimeSeriesVector>();
}

boost::shared_ptr<Spine::Table> DummyDatabaseDriver::makeQuery(
    Settings &settings, boost::shared_ptr<Spine::ValueFormatter> &)
{
  return boost::make_shared<Spine::Table>();
}

FlashCounts DummyDatabaseDriver::getFlashCount(const boost::posix_time::ptime &,
                                               const boost::posix_time::ptime &,
                                               const Spine::TaggedLocationList &)
{
  return FlashCounts();
}

boost::shared_ptr<std::vector<ObservableProperty>> DummyDatabaseDriver::observablePropertyQuery(
    std::vector<std::string> &, const std::string)
{
  return boost::make_shared<std::vector<ObservableProperty>>();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
