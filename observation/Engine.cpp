#include "Engine.h"
#include "DBRegistry.h"
#include "DatabaseDriverFactory.h"
#include "ObservationCacheFactory.h"
#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <boost/algorithm/string/join.hpp>
#include <macgyver/Geometry.h>
#include <spine/Convenience.h>
#include <spine/ParameterTools.h>
#include <spine/Reactor.h>
#include <spine/TimeSeriesOutput.h>

namespace ts = SmartMet::Spine::TimeSeries;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
Engine::Engine(const std::string &configfile)
    : itsConfigFile(configfile), itsDatabaseRegistry(new DBRegistry())
{
}

void Engine::init()
{
  try
  {
    Spine::ConfigBase cfg(itsConfigFile);

    itsEngineParameters.reset(new EngineParameters(cfg));

    //    std::cout << itsEngineParameters->databaseDriverInfo << std::endl;

    itsDatabaseRegistry->loadConfigurations(itsEngineParameters->dbRegistryFolderPath);

    // Initialize the caches
    initializeCache();

    // Read preloaded stations from disk if available
    unserializeStations();

    itsEngineParameters->observationCacheProxy =
        ObservationCacheFactory::create(itsEngineParameters, cfg);
#ifdef TODO_CAUSES_SEGFAULT_AT_EXIT
    itsDatabaseDriver.reset(DatabaseDriverFactory::create(itsEngineParameters, cfg));
#else
    itsDatabaseDriver = DatabaseDriverFactory::create(itsEngineParameters, cfg);
#endif
    if (itsDatabaseDriver != nullptr)
    {
      logMessage("[Observation Engine] Database driver '" + itsDatabaseDriver->name() + "' created",
                 itsEngineParameters->quiet);
      itsDatabaseDriver->init(this);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Observation-engine initialization failed");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown the engine
 */
// ----------------------------------------------------------------------

void Engine::shutdown()
{
  std::cout << "  -- Shutdown requested (Observation)" << std::endl;
  itsDatabaseDriver->shutdown();
}

void Engine::unserializeStations()
{
  boost::filesystem::path path = itsEngineParameters->serializedStationsFile;

  try
  {
    auto stationinfo = boost::make_shared<StationInfo>();
    if (boost::filesystem::exists(path) && !boost::filesystem::is_empty(path))
    {
      stationinfo->unserialize(itsEngineParameters->serializedStationsFile);

      boost::atomic_store(&itsEngineParameters->stationInfo, stationinfo);
      logMessage("[Observation Engine] Unserialized stations successfully from " + path.string(),
                 itsEngineParameters->quiet);
    }
    else
    {
      boost::atomic_store(&itsEngineParameters->stationInfo, stationinfo);
      logMessage("[Observation Engine] No serialized station file found from " + path.string(),
                 itsEngineParameters->quiet);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to unserialize station info!")
        .addParameter("station file", path.string());
  }
}

bool Engine::stationHasRightType(const Spine::Station &station, const Settings &settings)
{
  try
  {
    if ((settings.stationtype == "fmi" || settings.stationtype == "opendata" ||
         settings.stationtype == "opendata_minute" || settings.stationtype == "opendata_daily" ||
         settings.stationtype == "daily" || settings.stationtype == "hourly" ||
         settings.stationtype == "monthly" || settings.stationtype == "lammitystarve" ||
         settings.stationtype == "solar" || settings.stationtype == "minute_rad") &&
        station.isFMIStation)
    {
      return true;
    }
    if (settings.stationtype == "foreign")
    {
      return true;
    }
    if (settings.stationtype == "road" && station.isRoadStation)
    {
      return true;
    }
    if ((settings.stationtype == "mareograph" || settings.stationtype == "opendata_mareograph") &&
        station.isMareographStation)
    {
      return true;
    }
    if ((settings.stationtype == "buoy" || settings.stationtype == "opendata_buoy") &&
        station.isBuoyStation)
    {
      return true;
    }
    if (settings.stationtype == "syke" && station.isSYKEStation)
    {
      return true;
    }
    if (settings.stationtype == "MAST")
    {
      return true;
    }

    return false;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void Engine::getStations(Spine::Stations &stations, Settings &settings)
{
  return itsDatabaseDriver->getStations(stations, settings);
}

void Engine::getStationsByArea(Spine::Stations &stations,
                               const std::string &stationtype,
                               const boost::posix_time::ptime &starttime,
                               const boost::posix_time::ptime &endtime,
                               const std::string &areaWkt)
{
  return itsDatabaseDriver->getStationsByArea(stations, stationtype, starttime, endtime, areaWkt);
}

void Engine::getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings)
{
  return itsDatabaseDriver->getStationsByBoundingBox(stations, settings);
}

void Engine::initializeCache()
{
  try
  {
    itsEngineParameters->queryResultBaseCache.resize(itsEngineParameters->queryResultBaseCacheSize);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool Engine::ready() const
{
  std::cout << "Warning: obsengine::ready called" << std::endl;
  return true;
}

Geonames::Engine *Engine::getGeonames() const
{
  // this will wait until the engine is ready
  auto *engine = itsReactor->getSingleton("Geonames", nullptr);
  return reinterpret_cast<Geonames::Engine *>(engine);
}

FlashCounts Engine::getFlashCount(const boost::posix_time::ptime &starttime,
                                  const boost::posix_time::ptime &endtime,
                                  const Spine::TaggedLocationList &locations)
{
  return itsDatabaseDriver->getFlashCount(starttime, endtime, locations);
}

std::shared_ptr<std::vector<ObservableProperty>> Engine::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string language)
{
  // Remove possible sensor numbers
  std::vector<std::string> parameter_names;
  for(const auto& p : parameters)
	{
	  if(p.find("(:") != std::string::npos)
		parameter_names.push_back(p.substr(0, p.find("(:")));
	  else
		parameter_names.push_back(p);
	}

  return itsDatabaseDriver->observablePropertyQuery(parameter_names, language);
}

ts::TimeSeriesVectorPtr Engine::values(Settings &settings)
{
  // Drop unknown parameters from parameter list and
  // store their indexes
  std::vector<unsigned int> unknownParameterIndexes;
  if (settings.debug_options & Settings::DUMP_SETTINGS)
  {
    std::cout << "SmartMet::Engine::Observation::Settings:\n" << settings << std::endl;
  }
  Settings querySettings = beforeQuery(settings, unknownParameterIndexes);

  ts::TimeSeriesVectorPtr ret = itsDatabaseDriver->values(querySettings);

  // Insert missing values for unknown parameters and
  // arrange data order in result set
  afterQuery(ret, settings, unknownParameterIndexes);

  return ret;
}

void Engine::makeQuery(QueryBase *qb)
{
  itsDatabaseDriver->makeQuery(qb);
}

bool Engine::isParameter(const std::string &alias, const std::string &stationType) const
{
  return itsEngineParameters->isParameter(alias, stationType);
}

bool Engine::isParameterVariant(const std::string &name) const
{
  return itsEngineParameters->isParameterVariant(name);
}

uint64_t Engine::getParameterId(const std::string &alias, const std::string &stationType) const
{
  return itsEngineParameters->getParameterId(alias, stationType);
}

std::string Engine::getParameterIdAsString(const std::string &alias,
                                           const std::string &stationType) const
{
  return itsEngineParameters->getParameterIdAsString(alias, stationType);
}

std::set<std::string> Engine::getValidStationTypes() const
{
  try
  {
    std::set<std::string> stationTypes;

    for (auto const &mapEntry : itsEngineParameters->stationtypeConfig.getGroupCodeSetMap())
    {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry : itsEngineParameters->stationtypeConfig.getDatabaseTableNameMap())
    {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry : itsEngineParameters->stationtypeConfig.getUseCommonQueryMethodMap())
    {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry : itsEngineParameters->stationtypeConfig.getProducerIdSetMap())
    {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry : itsEngineParameters->externalAndMobileProducerConfig)
    {
      stationTypes.insert(mapEntry.first);
    }

    return stationTypes;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

/*
 * \brief Read values for given times only.
 */

Spine::TimeSeries::TimeSeriesVectorPtr Engine::values(
    Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  // Drop unknown parameters from parameter list and
  // store their indexes
  std::vector<unsigned int> unknownParameterIndexes;
  if (settings.debug_options & Settings::DUMP_SETTINGS)
  {
    std::cout << "SmartMet::Engine::Observation::Settings:\n" << settings << std::endl;
    std::cout << "SmartMet::Spine::TimeSeriesGeneratorOptions:\n" << timeSeriesOptions << std::endl;
  }
  Settings querySettings = beforeQuery(settings, unknownParameterIndexes);

  Spine::TimeSeries::TimeSeriesVectorPtr ret =
      itsDatabaseDriver->values(querySettings, timeSeriesOptions);

  // Insert missing values for unknown parameters and
  // arrange data order in result set
  afterQuery(ret, settings, unknownParameterIndexes);

  return ret;
}

MetaData Engine::metaData(const std::string &producer) const
{
  return itsDatabaseDriver->metaData(producer);
}

// Translates WMO, RWID,LPNN to FMISID
Spine::TaggedFMISIDList Engine::translateToFMISID(const boost::posix_time::ptime &starttime,
                                                  const boost::posix_time::ptime &endtime,
                                                  const std::string &stationtype,
                                                  const StationSettings &stationSettings) const
{
  return itsDatabaseDriver->translateToFMISID(starttime, endtime, stationtype, stationSettings);
}

Settings Engine::beforeQuery(const Settings &settings,
                             std::vector<unsigned int> &unknownParameterIndexes) const
{
  // Copy original settings
  Settings ret = settings;
  // Clear parameter list
  ret.parameters.clear();
  // Add known parameters back to list and store indexes of unknown parameters
  for (unsigned int i = 0; i < settings.parameters.size(); i++)
  {
    const auto &p = settings.parameters.at(i);
    std::string pname = Fmi::ascii_tolower_copy(p.name());
    if (!isParameter(pname, settings.stationtype) && !Spine::is_special_parameter(pname))
    {
      unknownParameterIndexes.push_back(i);
      continue;
    }
    ret.parameters.push_back(p);
  }

  return ret;
}

void Engine::afterQuery(Spine::TimeSeries::TimeSeriesVectorPtr &tsvPtr,
                        const Settings &settings,
                        const std::vector<unsigned int> &unknownParameterIndexes) const
{
  if (tsvPtr->empty())
    return;

  if (!unknownParameterIndexes.empty())
  {
    // Take copy of the first time series
    Spine::TimeSeries::TimeSeries ts = tsvPtr->at(0);
    // Set values in all timestesps to Spine::TimeSeries::None()
    for (auto &timedvalue : ts)
      timedvalue.value = Spine::TimeSeries::None();
    // Insert the nullified times series to time series vector
    for (auto index : unknownParameterIndexes)
      tsvPtr->insert(tsvPtr->begin() + index, ts);
  }

  // Arrange resultset in the right order
  // Find out FMISID column
  int fmisid_index = -1;
  for (unsigned int i = 0; i < settings.parameters.size(); i++)
    if (settings.parameters[i].name() == "fmisid")
    {
      fmisid_index = i;
      break;
    }

  if (fmisid_index < 0)
    return;

  const SmartMet::Spine::TimeSeries::TimeSeries &fmisid_vector = tsvPtr->at(fmisid_index);
  std::map<std::string, std::vector<int>> fmisid_mapped_indexes;
  // Sort out data indexes per each FMISID
  for (unsigned int i = 0; i < fmisid_vector.size(); i++)
  {
    const SmartMet::Spine::TimeSeries::Value &value = fmisid_vector.at(i).value;
    std::string fmisid = getStringValue(value);
    fmisid_mapped_indexes[fmisid].push_back(i);
  }

  // Create and initialize data structure for results
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr result =
      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr(
          new SmartMet::Spine::TimeSeries::TimeSeriesVector);
  for (unsigned int i = 0; i < tsvPtr->size(); i++)
    result->push_back(ts::TimeSeries());

  // FMISIDs are in right order in settings.taggedFMISIDs list
  // Iterate the list and copy data from original data structure to result structure
  for (const auto &id : settings.taggedFMISIDs)
  {
    std::string fmisid = Fmi::to_string(id.fmisid);
    if (fmisid_mapped_indexes.find(fmisid) == fmisid_mapped_indexes.end())
      continue;
    const auto &indexes = fmisid_mapped_indexes.at(fmisid);
    if (indexes.empty())
      continue;
    unsigned int firstIndex = indexes.front();
    unsigned int numberOfRows = indexes.size();

    for (unsigned int i = 0; i < tsvPtr->size(); i++)
    {
      const SmartMet::Spine::TimeSeries::TimeSeries &ts = tsvPtr->at(i);
      SmartMet::Spine::TimeSeries::TimeSeries &resultVector = result->at(i);
      resultVector.insert(
          resultVector.end(), ts.begin() + firstIndex, ts.begin() + firstIndex + numberOfRows);
    }
  }

  tsvPtr = result;
}

void Engine::reloadStations()
{
  itsDatabaseDriver->reloadStations();
}

ContentTable Engine::getProducerInfo(boost::optional<std::string> producer) const
{ 
  boost::shared_ptr<Spine::Table> resultTable(new Spine::Table);

  std::set<std::string> station_types = getValidStationTypes();

  if(producer)
	{
	  if(station_types.find(*producer) == station_types.end())
		return std::make_pair(resultTable,  Spine::TableFormatter::Names());

	  station_types.clear();
	  station_types.insert(*producer);
	}

  static Spine::TableFormatter::Names headers{"#","Producer","ProducerId","StationGroups"};

  unsigned int row = 0;
  for(const auto& t : station_types)
	{
      if (t.empty())
        continue;
	  
	  std::string producer_ids;
	  std::string group_codes;

	  if(itsEngineParameters->isExternalOrMobileProducer(t))
		{
		  producer_ids = Fmi::to_string(itsEngineParameters->externalAndMobileProducerConfig.at(t).producerId().asInt());
		}
	  else
		{
		  if(itsEngineParameters->stationtypeConfig.hasProducerIds(t))
			{
			  std::shared_ptr<const std::set<uint>> producers = itsEngineParameters->stationtypeConfig.getProducerIdSetByStationtype(t);
			  std::list<std::string> producer_id_list;
			  for(auto id : *producers)
				producer_id_list.push_back(Fmi::to_string(id));
			  producer_ids =  boost::algorithm::join(producer_id_list, ",");
			}
		  if(itsEngineParameters->stationtypeConfig.hasGroupCodes(t))
			{
			  std::shared_ptr<const std::set<std::string>> group_code_set = itsEngineParameters->stationtypeConfig.getGroupCodeSetByStationtype(t);
			  group_codes = boost::algorithm::join(*group_code_set, ",");
			}
		}

	  int column = 0;

	  // Row number
	  resultTable->set(column, row, Fmi::to_string(row+1));
	  ++column;
	  
	  // Producer
	  resultTable->set(column, row,  t);
	  ++column;
	  
	  // Producer ids
	  resultTable->set(column, row, producer_ids);
	  ++column;
	  
	  // Station groups
	  resultTable->set(column, row,  group_codes);
	  
	  row++;
	}

  return std::make_pair(resultTable, headers);
}

ContentTable Engine::getParameterInfo(boost::optional<std::string> producer) const
{ 
  boost::shared_ptr<Spine::Table> resultTable(new Spine::Table);
  
  if(producer)
	{
	  std::set<std::string> station_types = getValidStationTypes();
	  if(station_types.find(*producer) == station_types.end())
		return std::make_pair(resultTable,  Spine::TableFormatter::Names());
	}

  std::set<std::string> newbase_parameters;
  NFmiEnumConverter converter;
  for(int i = kFmiBadParameter; i < kFmiLastParameter; i++)
	{
	  std::string param_name = converter.ToString(i);
	  if(param_name.empty())
		continue;

	  newbase_parameters.insert(Fmi::ascii_tolower_copy(param_name));
	}

  static Spine::TableFormatter::Names headers{"#","Parameter","Producer","ParameterId"};
  
  unsigned int row = 0;
  unsigned int param_counter = 1;  
  for(const auto& param : *itsEngineParameters->parameterMap)
	{
	  unsigned int column = 0;	

  	  // Param counter
	   resultTable->set(column, row, Fmi::to_string(param_counter));
	   ++column;

	   // Parameter
	   resultTable->set(column, row,  param.first);
	   ++column;

	   unsigned int subrow = row;
	   for(const auto& producer_param : param.second)
		 {
		   if(!producer || (*producer == producer_param.first))
			 {
			   // Producer
			   resultTable->set(column, subrow,  producer_param.first);
			   // Parameter id
			   resultTable->set(column+1, subrow,  producer_param.second);		  
			   subrow++;
			 }
		 }
	   // No producer for parameter
	   if(row == subrow)
		 continue;

	   row++;
	   while(row < subrow)
		 {
		   // Parameter id
		   resultTable->set(0, row, "-");
		   resultTable->set(1, row, "-");
		   row++;
		 }
	   param_counter++;
	}

  return std::make_pair(resultTable, headers);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

// DYNAMIC MODULE CREATION TOOLS

extern "C" void *engine_class_creator(const char *configfile, void * /* user_data */)
{
  return new SmartMet::Engine::Observation::Engine(configfile);
}

extern "C" const char *engine_name()
{
  return "Observation";
}
