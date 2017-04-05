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
DummyDatabaseDriver::DummyDatabaseDriver()
{
}

void DummyDatabaseDriver::init(Geonames::Engine *geonames)
{
}

boost::shared_ptr<Spine::Table> DummyDatabaseDriver::makeQuery(
    Settings &settings, boost::shared_ptr<Spine::ValueFormatter> &valueFormatter)
{
  return (boost::shared_ptr<Spine::Table>(new Spine::Table));
}
ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

ts::TimeSeriesVectorPtr DummyDatabaseDriver::values(Settings &settings)
{
  return (ts::TimeSeriesVectorPtr(new ts::TimeSeriesVector));
}

ts::TimeSeriesVectorPtr DummyDatabaseDriver::values(
    Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  return (ts::TimeSeriesVectorPtr(new ts::TimeSeriesVector));
}

FlashCounts DummyDatabaseDriver::getFlashCount(const boost::posix_time::ptime &starttime,
                                               const boost::posix_time::ptime &endtime,
                                               const Spine::TaggedLocationList &locations)
{
  return FlashCounts();
}

boost::shared_ptr<std::vector<ObservableProperty> > DummyDatabaseDriver::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string language)
{
  return (boost::shared_ptr<std::vector<ObservableProperty> >(new std::vector<ObservableProperty>));
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
