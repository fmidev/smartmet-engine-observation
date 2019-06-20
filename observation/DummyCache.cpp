#include "DummyCache.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
DummyCache::DummyCache(const EngineParametersPtr &p) : itsParameters(p) {}

void DummyCache::initializeConnectionPool(int)
{
  logMessage("[Observation Engine] Dummy cache initialized!", itsParameters->quiet);
}

Spine::TimeSeries::TimeSeriesVectorPtr DummyCache::valuesFromCache(Settings &settings)
{
  return Spine::TimeSeries::TimeSeriesVectorPtr();
}

Spine::TimeSeries::TimeSeriesVectorPtr DummyCache::valuesFromCache(
    Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  return Spine::TimeSeries::TimeSeriesVectorPtr();
}

Spine::Stations DummyCache::getStationsByTaggedLocations(
    const Spine::TaggedLocationList &taggedLocations,
    const int numberofstations,
    const std::string &stationtype,
    const int maxdistance,
    const std::set<std::string> &stationgroup_codes,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime)
{
  return Spine::Stations();
}

bool DummyCache::dataAvailableInCache(const Settings &settings) const
{
  return false;
}

bool DummyCache::flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                                       const boost::posix_time::ptime &endtime) const
{
  return false;
}

void DummyCache::getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const
{
}

void DummyCache::updateStationsAndGroups(const StationInfo &info) const {}

Spine::Stations DummyCache::findAllStationsFromGroups(
    const std::set<std::string> stationgroup_codes,
    const StationInfo &info,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime) const
{
  return Spine::Stations();
}

bool DummyCache::getStationById(Spine::Station &station,
                                int station_id,
                                const std::set<std::string> &stationgroup_codes,
                                const boost::posix_time::ptime &starttime,
                                const boost::posix_time::ptime &endtime) const
{
  return false;
}

Spine::Stations DummyCache::findStationsInsideArea(const Settings &settings,
                                                   const std::string &areaWkt,
                                                   const StationInfo &info) const
{
  return Spine::Stations();
}

FlashCounts DummyCache::getFlashCount(const boost::posix_time::ptime &starttime,
                                      const boost::posix_time::ptime &endtime,
                                      const Spine::TaggedLocationList &locations) const
{
  return FlashCounts();
}

boost::posix_time::ptime DummyCache::getLatestFlashTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillFlashDataCache(const std::vector<FlashDataItem> &flashCacheData) const
{
  return 0;
}

void DummyCache::cleanFlashDataCache(const boost::posix_time::time_duration &timetokeep) const {}

boost::posix_time::ptime DummyCache::getLatestObservationModifiedTime() const
{
  return boost::posix_time::not_a_date_time;
}

boost::posix_time::ptime DummyCache::getLatestObservationTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillDataCache(const std::vector<DataItem> &cacheData) const
{
  return 0;
}

void DummyCache::cleanDataCache(const boost::posix_time::time_duration &timetokeep) const {}

boost::posix_time::ptime DummyCache::getLatestWeatherDataQCTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillWeatherDataQCCache(
    const std::vector<WeatherDataQCItem> &cacheData) const
{
  return 0;
}

void DummyCache::cleanWeatherDataQCCache(const boost::posix_time::time_duration &timetokeep) const
{
}

bool DummyCache::roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime) const
{
  return false;
}

boost::posix_time::ptime DummyCache::getLatestRoadCloudDataTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillRoadCloudCache(
    const std::vector<MobileExternalDataItem> &mobileExternalCacheData) const
{
  return 0;
}

void DummyCache::cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const {}

bool DummyCache::netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                                         const boost::posix_time::ptime &endtime) const
{
  return false;
}

boost::posix_time::ptime DummyCache::getLatestNetAtmoDataTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillNetAtmoCache(
    const std::vector<MobileExternalDataItem> &mobileExternalCacheData) const
{
  return 0;
}

void DummyCache::cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const {}

void DummyCache::fillLocationCache(const std::vector<LocationItem> &locations) const {}

boost::shared_ptr<std::vector<ObservableProperty> > DummyCache::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string language) const
{
  return boost::shared_ptr<std::vector<ObservableProperty> >();
}

bool DummyCache::cacheHasStations() const
{
  return false;
}

void DummyCache::shutdown() {}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
