#include "Engine.h"
#include "DBRegistry.h"
#include "DatabaseDriverFactory.h"
#include "ObservationCacheFactory.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/make_shared.hpp>
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
namespace
{
  using namespace Utils;

struct CompareLocations
{
  bool operator()(const StationLocation *loc1, const StationLocation *loc2) const
  {
    return (loc1->location_start < loc2->location_start);
  }
};

bool string_found(const std::string &s1, const std::string &s2)
{
  // Checks if s1 contains s2, case insensitive
  std::string str1 = Fmi::ascii_toupper_copy(s1);
  std::string str2 = Fmi::ascii_toupper_copy(s2);

  return (str1.find(str2) != std::string::npos);
}
}  // namespace

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
  if (itsDatabaseDriver)
  {
    itsDatabaseDriver->shutdown();
  }
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

      itsEngineParameters->stationInfo.store(stationinfo);
      logMessage("[Observation Engine] Unserialized stations successfully from " + path.string(),
                 itsEngineParameters->quiet);
    }
    else
    {
      itsEngineParameters->stationInfo.store(stationinfo);
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
  for (const auto &p : parameters)
  {
    if (p.find("(:") != std::string::npos)
      parameter_names.emplace_back(p.substr(0, p.find("(:")));
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
  // LocalTimePool must be created by client plugin, because references to localtimes in the pool
  // are used in the result set and they must be valid as log as result set is processed
  if (settings.localTimePool == nullptr)
    throw Fmi::Exception::Trace(BCP, "Observation::Settings::localTimePool can not be null!!!");

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
    result->emplace_back(ts::TimeSeries(settings.localTimePool));

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

      // Prevent referencing past the end of source data

      if (firstIndex + numberOfRows > ts.size())
      {
        std::cout << "obsengine afterQuery: indexing error: fmisid=" << fmisid
                  << " firstIndex=" << firstIndex << " numberOfRows=" << numberOfRows
                  << " ts.size()=" << ts.size() << " settings=" << settings
                  << " resultVector=" << resultVector << " ts=" << ts << " i=" << i
                  << " tsvPtr->size()=" << tsvPtr->size();

        throw Fmi::Exception::Trace(BCP, "Internal error indexing data");
      }

      SmartMet::Spine::TimeSeries::TimedValueVector::const_iterator it_first = ts.begin();
      for (unsigned int i = 0; i < firstIndex; i++)
        it_first++;
      SmartMet::Spine::TimeSeries::TimedValueVector::const_iterator it_last = it_first;
      for (unsigned int i = 0; i < numberOfRows; i++)
        it_last++;

      //      resultVector.insert(resultVector.end(), ts.begin() + firstIndex, ts.begin() +
      //      firstIndex + numberOfRows);
      resultVector.insert(resultVector.end(), it_first, it_last);
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

  if (producer)
  {
    if (station_types.find(*producer) == station_types.end())
      return std::make_pair(resultTable, Spine::TableFormatter::Names());

    station_types.clear();
    station_types.insert(*producer);
  }

  Spine::TableFormatter::Names headers{"#", "Producer", "ProducerId", "StationGroups"};

  unsigned int row = 0;
  for (const auto &t : station_types)
  {
    if (t.empty())
      continue;

    std::string producer_ids;
    std::string group_codes;

    if (itsEngineParameters->isExternalOrMobileProducer(t))
    {
      producer_ids = Fmi::to_string(
          itsEngineParameters->externalAndMobileProducerConfig.at(t).producerId().asInt());
    }
    else
    {
      if (itsEngineParameters->stationtypeConfig.hasProducerIds(t))
      {
        std::shared_ptr<const std::set<uint>> producers =
            itsEngineParameters->stationtypeConfig.getProducerIdSetByStationtype(t);
        std::list<std::string> producer_id_list;
        for (auto id : *producers)
          producer_id_list.emplace_back(Fmi::to_string(id));
        producer_ids = boost::algorithm::join(producer_id_list, ",");
      }
      if (itsEngineParameters->stationtypeConfig.hasGroupCodes(t))
      {
        std::shared_ptr<const std::set<std::string>> group_code_set =
            itsEngineParameters->stationtypeConfig.getGroupCodeSetByStationtype(t);
        group_codes = boost::algorithm::join(*group_code_set, ",");
      }
    }

    int column = 0;

    // Row number
    resultTable->set(column, row, Fmi::to_string(row + 1));
    ++column;

    // Producer
    resultTable->set(column, row, t);
    ++column;

    // Producer ids
    resultTable->set(column, row, producer_ids);
    ++column;

    // Station groups
    resultTable->set(column, row, group_codes);

    row++;
  }

  return std::make_pair(resultTable, headers);
}

ContentTable Engine::getParameterInfo(boost::optional<std::string> producer) const
{
  boost::shared_ptr<Spine::Table> resultTable(new Spine::Table);

  if (producer)
  {
    std::set<std::string> station_types = getValidStationTypes();
    if (station_types.find(*producer) == station_types.end())
      return std::make_pair(resultTable, Spine::TableFormatter::Names());
  }

  /*
  std::set<std::string> newbase_parameters;
  NFmiEnumConverter converter;
  for (int i = kFmiBadParameter; i < kFmiLastParameter; i++)
  {
    std::string param_name = converter.ToString(i);
    if (param_name.empty())
      continue;

    newbase_parameters.insert(Fmi::ascii_tolower_copy(param_name));
  }
  */

  Spine::TableFormatter::Names headers{"#", "Parameter", "Producer", "ParameterId"};

  unsigned int row = 0;
  unsigned int param_counter = 1;
  for (const auto &param : *itsEngineParameters->parameterMap)
  {
    auto param_counter_str = Fmi::to_string(param_counter);

    for (const auto &producer_param : param.second)
    {
      unsigned int column = 0;

      // Param counter
      resultTable->set(column++, row, param_counter_str);
      // Parameter
      resultTable->set(column++, row, param.first);

      if (!producer || (*producer == producer_param.first))
      {
        // Producer
        resultTable->set(column++, row, producer_param.first);
        // Parameter id
        resultTable->set(column, row, producer_param.second);
      }
      row++;
    }

    param_counter++;
  }

  return std::make_pair(resultTable, headers);
}

ContentTable Engine::getStationInfo(const StationOptions &options) const
{
  try
  {
    boost::shared_ptr<Spine::Table> resultTable(new Spine::Table);

    Spine::TableFormatter::Names headers{"#",
                                         "name",
                                         "type",
                                         "fmisid",
                                         "wmo",
                                         "lpnn",
                                         "rwsid",
                                         "longitude",
                                         "latitude",
                                         "elevation",
                                         "start date",
                                         "end date",
                                         "timezone",
                                         "country",
                                         "region"};

    bool check_fmisid = !options.fmisid.empty();
    bool check_lpnn = !options.lpnn.empty();
    bool check_wmo = !options.wmo.empty();
    bool check_rwsid = !options.rwsid.empty();
    bool check_type = !options.type.empty();
    bool check_name = !options.name.empty();
    bool check_iso2 = !options.iso2.empty();
    bool check_region = !options.region.empty();
    bool check_bbox = (options.bbox != boost::none);
    bool only_starttime =
        (!options.start_time.is_not_a_date_time() && options.end_time.is_not_a_date_time());
    bool only_endtime =
        (options.start_time.is_not_a_date_time() && !options.end_time.is_not_a_date_time());
    bool neither_time =
        (options.start_time.is_not_a_date_time() && options.end_time.is_not_a_date_time());
    bool both_times =
        (!options.start_time.is_not_a_date_time() && !options.end_time.is_not_a_date_time());

    // FMISID -> Location -> station type, sort locations to ascending order according to start time
    std::map<unsigned int, std::map<const StationLocation *, std::string, CompareLocations>>
        station_location_types;

    std::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(options.timeformat));

    boost::posix_time::ptime now = boost::posix_time::second_clock::universal_time();

    unsigned int row = 0;
    auto sinfo = itsEngineParameters->stationInfo.load();
    for (const auto &s : sinfo->stations)
    {
      // Check data against options
      if (check_fmisid && options.fmisid.count(s.fmisid) == 0)
        continue;
      if (check_lpnn && options.lpnn.count(s.lpnn) == 0)
        continue;
      if (check_wmo && options.wmo.count(s.wmo) == 0)
        continue;
      if (check_rwsid && options.rwsid.count(s.rwsid) == 0)
        continue;
      if (check_type && !string_found(s.station_type, options.type))
        continue;
      if (check_name && !string_found(s.station_formal_name, options.name))
        continue;
      if (check_iso2 && !string_found(s.iso2, options.iso2))
        continue;
      if (check_region && !string_found(s.region, options.region))
        continue;

      if (check_bbox)
      {
        if (s.longitude_out < (*options.bbox).xMin || s.longitude_out > (*options.bbox).xMax ||
            s.latitude_out < (*options.bbox).yMin || s.latitude_out > (*options.bbox).yMax)
          continue;
      }

      // Check station time periods
      if (only_starttime && s.station_end < options.start_time)
        continue;
      if (only_endtime && s.station_start > options.end_time)
        continue;
      if (neither_time && (now < s.station_start || now > s.station_end))
        continue;
      if (both_times && (s.station_start > options.end_time || s.station_end < options.start_time))
        continue;

      boost::posix_time::ptime option_starttime;
      boost::posix_time::ptime option_endtime;
      if (only_starttime)
      {
        option_starttime = options.start_time;
        option_endtime = s.station_end;
      }
      else if (only_endtime)
      {
        option_starttime = s.station_start;
        option_endtime = options.end_time;
      }
      else if (both_times)
      {
        option_starttime = options.start_time;
        option_endtime = options.end_time;
      }
      boost::posix_time::time_period option_period(option_starttime, option_endtime);
      boost::posix_time::time_period station_period(s.station_start, s.station_end);
      std::vector<const StationLocation *> station_locations;
      const StationLocationVector &allLocations = sinfo->stationLocations.getAllLocations(s.fmisid);
      // Check location time periods
      for (const auto &loc : allLocations)
      {
        boost::posix_time::time_period location_period(loc.location_start, loc.location_end);
        if (neither_time)
        {
          if (location_period.contains(now))
            station_locations.push_back(&loc);
        }
        else if (option_period.intersects(location_period))
        {
          // Show whole period even if it intersects only partially
          station_locations.push_back(&loc);
        }
      }

      for (const auto &l : station_locations)
      {
        const StationLocation &loc = *l;
        if (station_location_types.find(s.fmisid) == station_location_types.end())
        {
          station_location_types[s.fmisid][&loc] = s.station_type;
        }
        else
        {
          bool add_location = true;
          for (auto &item : station_location_types.at(s.fmisid))
          {
            // If the same location already exists for the station, just update type field
            if (item.first->longitude == loc.longitude && item.first->latitude == loc.latitude &&
                item.first->elevation == loc.elevation &&
                item.first->location_start == loc.location_start &&
                item.first->location_end == loc.location_end)
            {
              item.second += (", " + s.station_type);
              add_location = false;
              break;
            }
          }
          if (add_location)
          {
            station_location_types[s.fmisid][&loc] = s.station_type;
          }
        }
      }
    }

    std::set<std::string> groups;
    for (const auto &station_item : station_location_types)
    {
      const Spine::Station &s = sinfo->getStation(station_item.first, groups);
      for (const auto &location_item : station_item.second)
      {
        unsigned int column = 0;
        const StationLocation &loc = *location_item.first;
        // Row number
        resultTable->set(column++, row, Fmi::to_string(row + 1));
        // Name
        resultTable->set(column++, row, s.station_formal_name);
        // Type
        resultTable->set(column++, row, location_item.second);
        // FMISID
        resultTable->set(column++, row, Fmi::to_string(s.fmisid));
        // WMO
        resultTable->set(column++, row, Fmi::to_string(s.wmo));
        // LPNN
        resultTable->set(column++, row, Fmi::to_string(s.lpnn));
        // RWSID
        resultTable->set(column++, row, Fmi::to_string(s.rwsid));
        // Longitude
        resultTable->set(column++, row, Fmi::to_string(loc.longitude));
        // Latitude
        resultTable->set(column++, row, Fmi::to_string(loc.latitude));
        // Elevation
        resultTable->set(column++, row, Fmi::to_string(loc.elevation));
        // Start date
        resultTable->set(column++, row, timeFormatter->format(loc.location_start));
        // End date
        resultTable->set(column++, row, timeFormatter->format(loc.location_end));
        // Timezone
        resultTable->set(column++, row, s.timezone);
        // Country
        resultTable->set(column++, row, s.iso2);
        // Region
        resultTable->set(column++, row, s.region);
        row++;
      }
    }

    return std::make_pair(resultTable, headers);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Fmi::Cache::CacheStatistics Engine::getCacheStats() const
{
  Fmi::Cache::CacheStatistics ret;

  // Disk and memory caches
  const ObservationCaches &caches = itsEngineParameters->observationCacheProxy->getCachesByName();

  for (const auto &item : caches)
  {
    auto cache_name = item.first;
    auto cache_stats = item.second->getCacheStats();
    for (const auto &item : cache_stats)
    {
      auto key = ("Observation::" + cache_name + "::" + item.first);
      ret.insert(std::make_pair(key, item.second));
    }
  }

  // "query_result_cache" is used by wfs makeQuery function
  ret.insert(std::make_pair("Observation::query_result_cache",
                            itsEngineParameters->queryResultBaseCache.statistics()));

  // Get private caches from drivers (Oracle-driver has some)
  auto private_caches = itsDatabaseDriver->getCacheStats();
  ret.insert(private_caches.begin(), private_caches.end());

  return ret;
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
