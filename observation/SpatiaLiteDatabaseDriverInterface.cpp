
#include "SpatiaLiteDatabaseDriverInterface.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

SpatiaLiteDatabaseDriverInterface::~SpatiaLiteDatabaseDriverInterface()
{
  if(itsDatabaseDriver)
	delete itsDatabaseDriver;
}

void SpatiaLiteDatabaseDriverInterface::init(Engine *obsengine)
{
  itsDatabaseDriver->init(obsengine);
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteDatabaseDriverInterface::values(Settings &settings) 
{ 
  return itsDatabaseDriver->values(settings); 
}
  
Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteDatabaseDriverInterface::values(Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{ 
  return itsDatabaseDriver->values(settings, timeSeriesOptions); 
}
  
Spine::TaggedFMISIDList SpatiaLiteDatabaseDriverInterface::translateToFMISID(
																			 const boost::posix_time::ptime &starttime,
																			 const boost::posix_time::ptime &endtime,
																			 const std::string &stationtype,
																			 const StationSettings &stationSettings) const
{
  return itsDatabaseDriver->translateToFMISID(starttime, endtime, stationtype, stationSettings);
}

void SpatiaLiteDatabaseDriverInterface::makeQuery(QueryBase *qb) {}

FlashCounts SpatiaLiteDatabaseDriverInterface::getFlashCount(const boost::posix_time::ptime &starttime,
															 const boost::posix_time::ptime &endtime,
															 const Spine::TaggedLocationList &locations) const
{
  return itsDatabaseDriver->getFlashCount(starttime, endtime, locations);
}

boost::shared_ptr<std::vector<ObservableProperty>> SpatiaLiteDatabaseDriverInterface::observablePropertyQuery(std::vector<std::string> &parameters, const std::string language)
{
  return itsDatabaseDriver->observablePropertyQuery(parameters, language);
}

void  SpatiaLiteDatabaseDriverInterface::getStations(Spine::Stations &stations, const Settings &settings) const
{
  itsDatabaseDriver->getStations(stations, settings);
}
  
void SpatiaLiteDatabaseDriverInterface::getStationsByArea(Spine::Stations &stations,
														  const std::string &stationtype,
														  const boost::posix_time::ptime &starttime,
														  const boost::posix_time::ptime &endtime,
														  const std::string &areaWkt) const
{
  itsDatabaseDriver->getStationsByArea(stations, stationtype, starttime, endtime, areaWkt);
}
  
void SpatiaLiteDatabaseDriverInterface::getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const
{
  itsDatabaseDriver->getStationsByBoundingBox(stations, settings);
}

void SpatiaLiteDatabaseDriverInterface::shutdown()
{
  itsDatabaseDriver->shutdown();
}

MetaData SpatiaLiteDatabaseDriverInterface::metaData(const std::string &producer) const
{
  return itsDatabaseDriver->metaData(producer);
}

void SpatiaLiteDatabaseDriverInterface::reloadStations()
{
  itsDatabaseDriver->reloadStations();
}
  
std::string SpatiaLiteDatabaseDriverInterface::id() const
{
  return itsDatabaseDriver->id();
}
  
std::string SpatiaLiteDatabaseDriverInterface::name() const
{
  return itsDatabaseDriver->name();
}
  
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
