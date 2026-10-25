#include "EngineImpl.h"
#include "DBRegistry.h"
#include "DatabaseDriverFactory.h"
#include "DatabaseDriverProxy.h"
#include "ObservationCacheFactory.h"
#include "SpecialParameters.h"
#include "StationOptions.h"
#include "Utils.h"
#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <fmt/format.h>
#include <macgyver/Geometry.h>
#include <macgyver/Join.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TimeFormatter.h>
#include <macgyver/TypeName.h>
#include <spine/ConfigTools.h>
#include <spine/Convenience.h>
#include <spine/Reactor.h>
#include <timeseries/ParameterTools.h>
#include <timeseries/TimeSeriesInclude.h>
#include <algorithm>
#include <memory>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
using namespace Utils;

bool string_found(const std::string &s1, const std::string &s2)
{
  // Checks if s1 contains s2, case insensitive
  std::string str1 = Fmi::ascii_toupper_copy(s1);
  std::string str2 = Fmi::ascii_toupper_copy(s2);

  return (str1.find(str2) != std::string::npos);
}

#if 0
bool stationHasRightType(const Spine::Station &station, const Settings &settings)
{
  try
  {
    if ((settings.stationtype == "fmi" || settings.stationtype == "opendata" ||
         settings.stationtype == "opendata_minute" || settings.stationtype == "opendata_daily" ||
         settings.stationtype == "daily" || settings.stationtype == "hourly" ||
         settings.stationtype == "monthly" || settings.stationtype == "lammitystarve" ||
         settings.stationtype == "solar" || settings.stationtype == "minute_rad") &&
        station.isFmi)
    {
      return true;
    }
    if (settings.stationtype == "foreign")
    {
      return true;
    }
    if (settings.stationtype == "road" && station.isRoad)
    {
      return true;
    }
    if ((settings.stationtype == "mareograph" || settings.stationtype == "opendata_mareograph") &&
        station.isMareograph)
    {
      return true;
    }
    if ((settings.stationtype == "buoy" || settings.stationtype == "opendata_buoy") &&
        station.isBuoy)
    {
      return true;
    }
    if (settings.stationtype == "syke" && station.isSyke)
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
#endif

// Insert nullified time series for unknown parameters
void insertUnknownParameters(TS::TimeSeriesVector &tsv,
                             const std::vector<unsigned int> &unknownParameterIndexes)
{
  // Take copy of the first time series
  TS::TimeSeries ts = tsv.at(0);
  // Set values in all timestesps to TS::None()
  for (auto &timedvalue : ts)
    timedvalue.value = TS::None();
  // Insert the nullified times series to time series vector
  for (auto index : unknownParameterIndexes)
    tsv.insert(tsv.begin() + index, ts);
}

// Find out FMISID column, -1 if there is none
int fmisidIndex(const Settings &settings)
{
  for (unsigned int i = 0; i < settings.parameters.size(); i++)
    if (settings.parameters[i].name() == "fmisid")
      return i;
  return -1;
}

// Copy the rows of a station from the original data structure to the result structure
void copyStationRows(const TS::TimeSeriesVector &tsv,
                     TS::TimeSeriesVector &result,
                     unsigned int firstIndex,
                     unsigned int numberOfRows,
                     const std::string &fmisid,
                     const Settings &settings)
{
  for (unsigned int i = 0; i < tsv.size(); i++)
  {
    const TS::TimeSeries &ts = tsv.at(i);

    TS::TimeSeries &resultVector = result.at(i);

    // Prevent referencing past the end of source data

    if (firstIndex + numberOfRows > ts.size())
    {
      std::cout << "obsengine afterQuery: indexing error: fmisid=" << fmisid
                << " firstIndex=" << firstIndex << " numberOfRows=" << numberOfRows
                << " ts.size()=" << ts.size() << " settings=" << settings
                << " resultVector=" << resultVector << " ts=" << ts << " i=" << i
                << " tsvPtr->size()=" << tsv.size();

      throw Fmi::Exception::Trace(BCP, "Internal error indexing data");
    }

    auto it_first = ts.begin();
    for (unsigned int k = 0; k < firstIndex; k++)
      it_first++;
    auto it_last = it_first;
    for (unsigned int k = 0; k < numberOfRows; k++)
      it_last++;

    //      resultVector.insert(resultVector.end(), ts.begin() + firstIndex, ts.begin() +
    //      firstIndex + numberOfRows);
    resultVector.insert(resultVector.end(), it_first, it_last);
  }
}

void afterQuery(TS::TimeSeriesVectorPtr &tsvPtr,
                const Settings &settings,
                const std::vector<unsigned int> &unknownParameterIndexes)
{
  try
  {
    if (tsvPtr->empty())
      return;

    if (!unknownParameterIndexes.empty())
      insertUnknownParameters(*tsvPtr, unknownParameterIndexes);

    // Arrange resultset in the right order
    // Find out FMISID column
    int fmisid_index = fmisidIndex(settings);

    if (fmisid_index < 0 || settings.taggedFMISIDs.empty())
      return;

    const TS::TimeSeries &fmisid_vector = tsvPtr->at(fmisid_index);
    std::map<std::string, std::vector<int>> fmisid_mapped_indexes;
    // Sort out data indexes per each FMISID
    for (unsigned int i = 0; i < fmisid_vector.size(); i++)
    {
      const TS::Value &value = fmisid_vector.at(i).value;
      std::string fmisid = getStringValue(value);
      fmisid_mapped_indexes[fmisid].push_back(i);
    }

    // Create and initialize data structure for results
    TS::TimeSeriesVectorPtr result = std::make_shared<TS::TimeSeriesVector>();
    for (unsigned int i = 0; i < tsvPtr->size(); i++)
      result->emplace_back();

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

      copyStationRows(*tsvPtr, *result, firstIndex, numberOfRows, fmisid, settings);
    }

    tsvPtr = result;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace

EngineImpl::EngineImpl(std::string configfile)
    : itsConfigFile(std::move(configfile)), itsDatabaseRegistry(new DBRegistry())
{
}

void EngineImpl::init()
{
  try
  {
    Spine::ConfigBase cfg(itsConfigFile);

    itsEngineParameters = std::make_shared<EngineParameters>(cfg);

    //    std::cout << itsEngineParameters->databaseDriverInfo << '\n';

    itsDatabaseRegistry->loadConfigurations(itsEngineParameters->dbRegistryFolderPath);

    itsGeonames = itsReactor->getEngine<Geonames::Engine>("Geonames", nullptr);

    // Initialize the caches
    initializeCache();

    // Read preloaded stations from disk if available
    unserializeStations();

    itsEngineParameters->observationCacheProxy =
        ObservationCacheFactory::create(itsEngineParameters, cfg);

    itsDatabaseDriver.reset(DatabaseDriverFactory::create(itsEngineParameters, cfg));
    if (itsDatabaseDriver)
    {
      logMessage(
          "[Observation EngineImpl] Database driver '" + itsDatabaseDriver->name() + "' created",
          itsEngineParameters->quiet);
      itsDatabaseDriver->init(this);
    }

    SpecialParameters::setGeonames(getGeonames());
    Utils::setParameterTranslations(
        std::make_shared<Spine::ParameterTranslations>(cfg.get_config()));
    itsDatabaseDriver->getProducerGroups(itsEngineParameters->producerGroups);
    itsEngineParameters->producerGroups.replaceProducerIds("observations_fmi", "fmi");

    // Lets get station groups even if they can not be utilized for now
    StationGroups sg;
    itsDatabaseDriver->getStationGroups(sg);
    auto sinfo = itsEngineParameters->stationInfo.load();
    sinfo->setStationGroups(sg);

    // Get measurand info from DB
    initMeasurandInfo();

    Spine::Reactor *reactor = Spine::Reactor::instance;
    if (reactor)
    {
      using AdminRequestAccess = Spine::Reactor::AdminRequestAccess;

      reactor->addAdminTableRequestHandler(
          this,
          "obsproducers",
          AdminRequestAccess::Public,
          std::bind(&EngineImpl::requestProducerInfo, this, std::placeholders::_2),
          "Observation producers");

      reactor->addAdminTableRequestHandler(
          this,
          "obsparameters",
          AdminRequestAccess::Public,
          std::bind(&EngineImpl::requestParameterInfo, this, std::placeholders::_2),
          "Observation parameters");

      reactor->addAdminTableRequestHandler(
          this,
          "stations",
          AdminRequestAccess::Public,
          std::bind(&EngineImpl::requestStationInfo, this, std::placeholders::_2),
          "Observation stations");

      reactor->addAdminTableRequestHandler(
          this,
          "obssensors",
          AdminRequestAccess::Public,
          std::bind(&EngineImpl::requestSensorInfo, this, std::placeholders::_2),
          "Observation sensors of the stations");

      reactor->addAdminBoolRequestHandler(
          this,
          "reloadstations",
          AdminRequestAccess::RequiresAuthentication,
          std::bind(&EngineImpl::requestReloadStations, this, std::placeholders::_2),
          "Reload stations");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Observation-engine initialization failed");
  }
}

namespace
{
void parseIntOption(std::set<int> &output, const std::string &option)
{
  if (option.empty())
    return;

  std::vector<std::string> parts;
  boost::algorithm::split(parts, option, boost::algorithm::is_any_of(","));
  for (const auto &p : parts)
    output.insert(Fmi::stoi(p));
}
}  // anonymous namespace

std::unique_ptr<Spine::Table> EngineImpl::requestProducerInfo(
    const Spine::HTTP::Request &theRequest) const
{
  // Optional producer filter
  auto producer = theRequest.getParameter("producer");

  std::unique_ptr<Spine::Table> obsengineProducerInfo = getProducerInfo(producer);

  return obsengineProducerInfo;
}

std::unique_ptr<Spine::Table> EngineImpl::requestParameterInfo(
    const Spine::HTTP::Request &theRequest) const
{
  // Optional producer filter
  auto producer = theRequest.getParameter("producer");

  std::unique_ptr<Spine::Table> obsengineParameterInfo = getParameterInfo(producer);

  return obsengineParameterInfo;
}

std::unique_ptr<Spine::Table> EngineImpl::requestStationInfo(
    const Spine::HTTP::Request &theRequest) const
{
  StationOptions options;
  parseIntOption(options.fmisid, Spine::optional_string(theRequest.getParameter("fmisid"), ""));
  parseIntOption(options.lpnn, Spine::optional_string(theRequest.getParameter("lpnn"), ""));
  parseIntOption(options.wmo, Spine::optional_string(theRequest.getParameter("wmo"), ""));
  parseIntOption(options.rwsid, Spine::optional_string(theRequest.getParameter("rwsid"), ""));
  options.type = Spine::optional_string(theRequest.getParameter("type"), "");
  options.name = Spine::optional_string(theRequest.getParameter("name"), "");
  options.iso2 = Spine::optional_string(theRequest.getParameter("country"), "");
  options.region = Spine::optional_string(theRequest.getParameter("region"), "");
  options.timeformat = Spine::optional_string(theRequest.getParameter("timeformat"), "sql");
  std::string starttime = Spine::optional_string(theRequest.getParameter("starttime"), "");
  std::string endtime = Spine::optional_string(theRequest.getParameter("endtime"), "");
  if (!starttime.empty())
    options.start_time = Fmi::TimeParser::parse(starttime);
  else
    options.start_time = Fmi::DateTime::NOT_A_DATE_TIME;
  if (!endtime.empty())
    options.end_time = Fmi::TimeParser::parse(endtime);
  else
    options.end_time = Fmi::DateTime::NOT_A_DATE_TIME;

  std::string bbox_string = Spine::optional_string(theRequest.getParameter("bbox"), "");
  if (!bbox_string.empty())
    options.bbox = Spine::BoundingBox(bbox_string);

  std::unique_ptr<Spine::Table> obsengineStationInfo = getStationInfo(options);

  return obsengineStationInfo;
}

// ----------------------------------------------------------------------
/*!
 * \brief The obssensors request: the sensors of the stations of a producer
 *
 * Options: producer (required), fmisid and param (comma separated lists, all when omitted),
 * starttime and endtime (the current time when omitted), timeformat (sql by default).
 */
// ----------------------------------------------------------------------

std::unique_ptr<Spine::Table> EngineImpl::requestSensorInfo(
    const Spine::HTTP::Request &theRequest) const
{
  try
  {
    SensorOptions options;
    options.producer = Spine::optional_string(theRequest.getParameter("producer"), "");
    if (options.producer.empty())
      throw Fmi::Exception(BCP, "The producer option is required");
    parseIntOption(options.fmisids, Spine::optional_string(theRequest.getParameter("fmisid"), ""));
    const std::string params = Spine::optional_string(theRequest.getParameter("param"), "");
    if (!params.empty())
    {
      std::vector<std::string> parts;
      boost::algorithm::split(parts, params, boost::algorithm::is_any_of(","));
      for (const auto &p : parts)
        if (!p.empty())
          options.parameters.insert(p);
    }
    const std::string starttime = Spine::optional_string(theRequest.getParameter("starttime"), "");
    const std::string endtime = Spine::optional_string(theRequest.getParameter("endtime"), "");
    if (!starttime.empty())
      options.starttime = Fmi::TimeParser::parse(starttime);
    if (!endtime.empty())
      options.endtime = Fmi::TimeParser::parse(endtime);
    std::unique_ptr<Fmi::TimeFormatter> formatter(Fmi::TimeFormatter::create(
        Spine::optional_string(theRequest.getParameter("timeformat"), "sql")));

    const auto result = getSensors(options);

    auto table = std::make_unique<Spine::Table>();
    table->setNames({"FMISID",
                     "Parameter",
                     "MeasurandId",
                     "MeasurandCode",
                     "Sensor",
                     "Default",
                     "ConfigurationStart",
                     "ConfigurationEnd",
                     "FirstData",
                     "LastData",
                     "Values"});
    const auto time = [&formatter](const Fmi::DateTime &t)
    { return t.is_not_a_date_time() ? std::string() : formatter->format(t); };
    unsigned int row = 0;
    for (const auto &s : result.sensors)
    {
      unsigned int column = 0;
      table->set(column++, row, Fmi::to_string(s.fmisid));
      table->set(column++, row, Fmi::join(s.parameters, ","));
      table->set(column++, row, Fmi::to_string(s.measurand_id));
      table->set(column++, row, s.measurand_code);
      table->set(column++, row, Fmi::to_string(s.sensor_no));
      table->set(column++, row, s.is_default ? "yes" : "no");
      table->set(column++, row, time(s.configuration_start));
      table->set(column++, row, time(s.configuration_end));
      table->set(column++, row, time(s.first_data));
      table->set(column++, row, time(s.last_data));
      table->set(column, row, s.values > 0 ? Fmi::to_string(s.values) : std::string());
      row++;
    }
    if (!result.available)
      throw Fmi::Exception(BCP, result.note);
    return table;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Listing the observation sensors failed");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief The sensors of the parameters of the stations of a producer
 */
// ----------------------------------------------------------------------

SensorResult EngineImpl::getSensors(const SensorOptions &options) const
{
  try
  {
    if (!isValidStationType(options.producer))
      throw Fmi::Exception(BCP, "Unknown producer").addParameter("producer", options.producer);

    // The parameter names of each measurand of the producer
    std::map<int, std::vector<std::string>> names;
    for (const auto &param : *itsEngineParameters->parameterMap)
      for (const auto &producer_param : param.second)
        if (producer_param.first == options.producer)
          if (auto id = Fmi::stoi_opt(producer_param.second))
            names[*id].push_back(param.first);

    // The measurands of the parameters asked
    std::set<int> measurands;
    for (const auto &name : options.parameters)
    {
      const auto id = Fmi::stoi_opt(
          itsEngineParameters->parameterMap->getParameter(Fmi::ascii_tolower_copy(name),
                                                          options.producer));
      if (!id)
        throw Fmi::Exception(BCP, "Unknown parameter '" + name + "' of producer '" + options.producer + "'")
            .addParameter("parameter", name)
            .addParameter("producer", options.producer);
      measurands.insert(*id);
    }

    SensorOptions query = options;
    const auto now = Fmi::SecondClock::universal_time();
    if (query.starttime.is_not_a_date_time())
      query.starttime = (query.endtime.is_not_a_date_time() ? now : query.endtime);
    if (query.endtime.is_not_a_date_time())
      query.endtime = std::max(now, query.starttime);
    if (query.endtime < query.starttime)
      throw Fmi::Exception(BCP, "The end time of the period is before its start time");

    // The producers of the station type as data requests use them (updateProducers)
    std::set<int> producers;
    const auto &stationtypes = itsEngineParameters->stationtypeConfig;
    if (stationtypes.getUseCommonQueryMethod(options.producer))
    {
      auto ids = stationtypes.getProducerIdSetByStationtype(options.producer);
      if (ids.empty())
        ids = itsEngineParameters->producerGroups.getProducerIds(
            options.producer, query.starttime, query.endtime);
      for (auto id : ids)
        producers.insert(static_cast<int>(id));
    }

    const auto *proxy = dynamic_cast<const DatabaseDriverProxy *>(itsDatabaseDriver.get());
    if (proxy == nullptr)
    {
      SensorResult result;
      result.note = "The database driver cannot list sensors";
      return result;
    }

    auto result = proxy->getSensors(query, measurands, producers);
    // The parameter names of the producer, and the measurand codes when the source has none
    const auto info = itsMeasurandInfo.find(options.producer);
    for (auto &s : result.sensors)
    {
      auto pos = names.find(s.measurand_id);
      if (pos != names.end())
        s.parameters = pos->second;
      if (s.measurand_code.empty() && info != itsMeasurandInfo.end())
      {
        auto m = info->second.find(Fmi::to_string(s.measurand_id));
        if (m != info->second.end())
          s.measurand_code = m->second.measurand_code;
      }
    }
    return result;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool EngineImpl::requestReloadStations(const Spine::HTTP::Request & /* theRequest */)
{
  reloadStations();
  return true;
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown the engine
 */
// ----------------------------------------------------------------------

void EngineImpl::shutdown()
{
  std::cout << "  -- Shutdown requested (Observation)\n";
  if (itsDatabaseDriver)
  {
    itsDatabaseDriver->shutdown();
  }
}

void EngineImpl::unserializeStations()
{
  std::filesystem::path path = itsEngineParameters->serializedStationsFile;

  try
  {
    auto stationinfo = std::make_shared<StationInfo>();
    stationinfo->setCandidateCacheSize(itsEngineParameters->nearestStationsCacheSize);

    if (std::filesystem::exists(path) && !std::filesystem::is_empty(path))
    {
      stationinfo->unserialize(itsEngineParameters->serializedStationsFile);

      itsEngineParameters->stationInfo.store(stationinfo);
      logMessage(
          "[Observation EngineImpl] Unserialized stations successfully from " + path.string(),
          itsEngineParameters->quiet);
    }
    else
    {
      itsEngineParameters->stationInfo.store(stationinfo);
      logMessage("[Observation EngineImpl] No serialized station file found from " + path.string(),
                 itsEngineParameters->quiet);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to unserialize station info!")
        .addParameter("station file", path.string());
  }
}

void EngineImpl::getStations(Spine::Stations &stations, const Settings &settings)
{
  return itsDatabaseDriver->getStations(stations, settings);
}

void EngineImpl::getStationsByArea(Spine::Stations &stations,
                                   const Settings &settings,
                                   const std::string &areaWkt)
{
  itsDatabaseDriver->getStationsByArea(stations, settings, areaWkt);
}

void EngineImpl::getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings)
{
  itsDatabaseDriver->getStationsByBoundingBox(stations, settings);
}

void EngineImpl::initializeCache()
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

bool EngineImpl::ready() const
{
  std::cout << "Warning: obsengine::ready called\n";
  return true;
}

Geonames::Engine *EngineImpl::getGeonames() const
{
  return itsGeonames.get();
}

std::optional<std::uint64_t> EngineImpl::getFlashGeneration(const Fmi::DateTime &starttime,
                                                           const Fmi::DateTime &endtime,
                                                           double minlon,
                                                           double minlat,
                                                           double maxlon,
                                                           double maxlat) const
{
  try
  {
    // Only the flash memory cache can answer, so the database drivers are not involved
    auto cache = itsEngineParameters->observationCacheProxy->getCacheByTableName(FLASH_DATA_TABLE);
    if (!cache)
      return std::nullopt;
    return cache->getFlashGeneration(starttime, endtime, minlon, minlat, maxlon, maxlat);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

FlashCounts EngineImpl::getFlashCount(const Fmi::DateTime &starttime,
                                      const Fmi::DateTime &endtime,
                                      const Spine::TaggedLocationList &locations)
{
  return itsDatabaseDriver->getFlashCount(starttime, endtime, locations);
}

std::shared_ptr<std::vector<ObservableProperty>> EngineImpl::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string &language)
{
  try
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
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr EngineImpl::values(Settings &settings)
{
  try
  {
    // Drop unknown parameters from parameter list and
    // store their indexes
    std::vector<unsigned int> unknownParameterIndexes;
    if (settings.debug_options & Settings::DUMP_SETTINGS)
    {
      std::cout << "EngineImpl::Observation::Settings:\n" << settings << '\n';
    }
    Settings querySettings = beforeQuery(settings, unknownParameterIndexes);

    TS::TimeSeriesVectorPtr ret = itsDatabaseDriver->values(querySettings);

    // Insert missing values for unknown parameters and
    // arrange data order in result set
    afterQuery(ret, settings, unknownParameterIndexes);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void EngineImpl::makeQuery(QueryBase *qb)
{
  itsDatabaseDriver->makeQuery(qb);
}

bool EngineImpl::isParameter(const std::string &alias, const std::string &stationType) const
{
  return itsEngineParameters->isParameter(alias, stationType);
}

bool EngineImpl::isParameterVariant(const std::string &name) const
{
  return itsEngineParameters->isParameterVariant(name);
}

uint64_t EngineImpl::getParameterId(const std::string &alias, const std::string &stationType) const
{
  return itsEngineParameters->getParameterId(alias, stationType);
}

std::string EngineImpl::getParameterIdAsString(const std::string &alias,
                                               const std::string &stationType) const
{
  return itsEngineParameters->getParameterIdAsString(alias, stationType);
}

std::set<std::string> EngineImpl::getValidStationTypes() const
{
  try
  {
    return itsEngineParameters->getValidStationTypes();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool EngineImpl::isValidStationType(const std::string &stationType) const
{
  try
  {
    return itsEngineParameters->isValidStationType(stationType);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

/*
 * \brief Read values for given times only.
 */

TS::TimeSeriesVectorPtr EngineImpl::values(Settings &settings,
                                           const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {  // Drop unknown parameters from parameter list and
    // store their indexes
    std::vector<unsigned int> unknownParameterIndexes;
    if (settings.debug_options & Settings::DUMP_SETTINGS)
    {
      std::cout << "EngineImpl::Observation::Settings:\n" << settings << '\n';
      std::cout << "TS::TimeSeriesGeneratorOptions:\n" << timeSeriesOptions << '\n';
    }
    Settings querySettings = beforeQuery(settings, unknownParameterIndexes);

    TS::TimeSeriesVectorPtr ret = itsDatabaseDriver->values(querySettings, timeSeriesOptions);

    // Insert missing values for unknown parameters and
    // arrange data order in result set
    afterQuery(ret, settings, unknownParameterIndexes);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

MetaData EngineImpl::metaData(const std::string &producer, const Settings &settings) const
{
  try
  {
    auto ret = itsDatabaseDriver->metaData(producer, settings);

    for (const auto &param : *itsEngineParameters->parameterMap)
    {
      for (const auto &producer_param : param.second)
      {
        // An empty mapping means the parameter is not available for the producer, querying
        // it would fail
        if ((producer.empty() || (producer == producer_param.first)) &&
            !producer_param.second.empty())
        {
          ret.parameters.insert(param.first);
        }
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// Translates WMO, RWID,LPNN to FMISID
Spine::TaggedFMISIDList EngineImpl::translateToFMISID(const Settings &settings,
                                                      const StationSettings &stationSettings) const
{
  return itsDatabaseDriver->translateToFMISID(settings, stationSettings);
}

Settings EngineImpl::beforeQuery(const Settings &settings,
                                 std::vector<unsigned int> &unknownParameterIndexes) const
{
  try
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

      // BRAINSTORM-3116 'level' kludge, FIXME !
      //
      if (pname == EDR_OBSERVATION_LEVEL)
      {
        ret.parameters.emplace_back("level", Spine::Parameter::Type::DataIndependent);
        continue;
      }

      if (!isParameter(pname, settings.stationtype) && !TimeSeries::is_special_parameter(pname))
      {
        // A parameter configured for some other station type gives a missing
        // column, so that queries over several producers work. A name which is
        // not configured for any station type is an error, not silently missing.
        if (!isParameterVariant(pname))
          throw Fmi::Exception(BCP, "Unknown observation parameter")
              .addParameter("Parameter", p.name())
              .addParameter("Station type", settings.stationtype)
              .disableStackTrace();
        unknownParameterIndexes.push_back(i);
        continue;
      }
      ret.parameters.push_back(p);
    }

    // Use all groups based on stationtype if there is no desired subgroup, otherwise use set
    // intersection to disable the user from adding new groups to the request

    ret.stationgroups.clear();
    auto allowed_groups =
        itsEngineParameters->stationtypeConfig.getGroupCodeSetByStationtype(settings.stationtype);

    if (settings.stationgroups.empty())
      ret.stationgroups.insert(allowed_groups->begin(), allowed_groups->end());
    else
    {
      // std::set_intersection dos not work here because of typedefs
      for (const auto &desired_group : settings.stationgroups)
        if (allowed_groups->find(desired_group) != allowed_groups->end())
          ret.stationgroups.insert(desired_group);
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void EngineImpl::reloadStations()
{
  itsDatabaseDriver->reloadStations();
}

ContentTable EngineImpl::getProducerInfo(const std::optional<std::string> &producer) const
{
  try
  {
    std::unique_ptr<Spine::Table> resultTable(new Spine::Table);
    Spine::TableFormatter::Names headers{"#", "Producer", "ProducerId", "StationGroups"};

    resultTable->setNames(headers);

    std::set<std::string> types = getValidStationTypes();

    if (producer)
    {
      if (types.find(*producer) == types.end())
        return resultTable;

      types.clear();
      types.insert(*producer);
    }

    unsigned int row = 0;
    for (const auto &t : types)
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
          producer_ids =
              Fmi::join(itsEngineParameters->stationtypeConfig.getProducerIdSetByStationtype(t));
        }
        if (itsEngineParameters->stationtypeConfig.hasGroupCodes(t))
        {
          std::shared_ptr<const std::set<std::string>> group_code_set =
              itsEngineParameters->stationtypeConfig.getGroupCodeSetByStationtype(t);
          group_codes = Fmi::join(*group_code_set);
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

    return resultTable;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

ContentTable EngineImpl::getParameterInfo(const std::optional<std::string> &producer) const
{
  try
  {
    std::unique_ptr<Spine::Table> resultTable(new Spine::Table);
    Spine::TableFormatter::Names headers{"#", "Parameter", "Producer", "ParameterId"};

    resultTable->setNames(headers);

    if (producer)
    {
      std::set<std::string> types = getValidStationTypes();
      if (types.find(*producer) == types.end())
        return resultTable;
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

    unsigned int row = 0;
    for (const auto &param : *itsEngineParameters->parameterMap)
    {
      for (const auto &producer_param : param.second)
      {
        unsigned int column = 0;

        if (!producer || (*producer == producer_param.first))
        {
          // Row number
          resultTable->set(column++, row, Fmi::to_string(row + 1));
          // Parameter
          resultTable->set(column++, row, param.first);
          // Producer
          resultTable->set(column++, row, producer_param.first);
          // Parameter id
          resultTable->set(column, row, producer_param.second);
          row++;
        }

      }

    }

    return resultTable;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

namespace
{
// Stations with similar metadata, as indexes into the station vector
struct StationGroup
{
  std::vector<std::size_t> indexes;
  std::vector<std::string> stationtypes;
};

// True if the station identifiers, names, area and bounding box match the options
bool stationMatchesIdentifiers(const Spine::Station& s, const StationOptions& options)
{
  if (!options.wsi.empty() && options.wsi.count(s.wsi) == 0)
    return false;
  if (!options.lpnn.empty() && options.lpnn.count(s.lpnn) == 0)
    return false;
  if (!options.wmo.empty() && options.wmo.count(s.wmo) == 0)
    return false;
  if (!options.rwsid.empty() && options.rwsid.count(s.rwsid) == 0)
    return false;
  if (!options.type.empty() && !string_found(s.type, options.type))
    return false;
  if (!options.name.empty() && !string_found(s.formal_name_fi, options.name))
    return false;
  if (!options.iso2.empty() && !string_found(s.iso2, options.iso2))
    return false;
  if (!options.region.empty() && !string_found(s.region, options.region))
    return false;

  if (options.bbox != std::nullopt)
  {
    if (s.longitude < (*options.bbox).xMin || s.longitude > (*options.bbox).xMax ||
        s.latitude < (*options.bbox).yMin || s.latitude > (*options.bbox).yMax)
      return false;
  }
  return true;
}

// True if the station time period matches the requested period, or the current time if
// no period was requested
bool stationMatchesTimes(const Spine::Station& s,
                         const StationOptions& options,
                         const Fmi::DateTime& now)
{
  const bool has_start = !options.start_time.is_not_a_date_time();
  const bool has_end = !options.end_time.is_not_a_date_time();

  if (has_start && !has_end)
    return !(s.station_end < options.start_time);
  if (!has_start && has_end)
    return !(s.station_start > options.end_time);
  if (!has_start && !has_end)
    return !(now < s.station_start || now > s.station_end);
  return !(s.station_start > options.end_time || s.station_end < options.start_time);
}

// True if the stations have the same time period, coordinates and elevation
bool sameStationMetadata(const Spine::Station& s1, const Spine::Station& s2)
{
  return (s1.station_start == s2.station_start && s1.station_end == s2.station_end &&
          s1.longitude == s2.longitude && s1.latitude == s2.latitude &&
          s1.elevation == s2.elevation);
}

// Group the stations accepted by the options by station starttime, endtime, coordinates and
// elevation by storing the indexes into the all_locations vector
std::vector<StationGroup> groupStations(const Spine::Stations& all_locations,
                                        const StationOptions& options,
                                        const Fmi::DateTime& now)
{
  std::vector<StationGroup> groups;

  for (auto i = 0UL; i < all_locations.size(); i++)
  {
    const auto& s = all_locations[i];

    // Check station against options and station time periods

    if (!stationMatchesIdentifiers(s, options) || !stationMatchesTimes(s, options, now))
      continue;

    // Station accepted, assign it into a group with similar metadata

    auto match = std::find_if(groups.begin(),
                              groups.end(),
                              [&](const StationGroup& group)
                              {
                                return std::any_of(
                                    group.indexes.begin(),
                                    group.indexes.end(),
                                    [&](std::size_t pos)
                                    { return sameStationMetadata(s, all_locations.at(pos)); });
                              });

    if (match != groups.end())
    {
      match->indexes.push_back(i);
      match->stationtypes.push_back(s.type);
    }
    else
    {
      StationGroup group;
      group.indexes.push_back(i);
      group.stationtypes.push_back(s.type);
      groups.push_back(group);
    }
  }
  return groups;
}

// Print the information for the groups
void addStationGroupRows(Spine::Table& resultTable,
                         unsigned int& row,
                         const Spine::Stations& all_locations,
                         const std::vector<StationGroup>& groups,
                         const Fmi::TimeFormatter& timeFormatter)
{
  for (const auto& group : groups)
  {
    const auto& s = all_locations.at(group.indexes.at(0));  // representative station
    unsigned int column = 0;
    auto grouplist = Fmi::join(group.stationtypes, ", ");
    resultTable.set(column++, row, Fmi::to_string(row + 1));                // Row number
    resultTable.set(column++, row, s.station_formal_name("fi"));            // Name
    resultTable.set(column++, row, grouplist);                              // Type
    resultTable.set(column++, row, Fmi::to_string(s.fmisid));               // FMISID
    resultTable.set(column++, row, s.wsi);                                  // WIGOS ID
    resultTable.set(column++, row, Fmi::to_string(s.wmo));                  // WMO
    resultTable.set(column++, row, Fmi::to_string(s.lpnn));                 // LPNN
    resultTable.set(column++, row, Fmi::to_string(s.rwsid));                // RWSID
    resultTable.set(column++, row, Fmi::to_string(s.longitude));            // Longitude
    resultTable.set(column++, row, Fmi::to_string(s.latitude));             // Latitude
    resultTable.set(column++, row, Fmi::to_string(s.elevation));            // Elevation
    resultTable.set(column++, row, timeFormatter.format(s.station_start));  // Start date
    resultTable.set(column++, row, timeFormatter.format(s.station_end));    // End date
    resultTable.set(column++, row, s.timezone);                             // Timezone
    resultTable.set(column++, row, s.iso2);                                 // Country
    resultTable.set(column++, row, s.region);                               // Region
    row++;
  }
}
}  // namespace

ContentTable EngineImpl::getStationInfo(const StationOptions& options) const
{
  try
  {
    std::unique_ptr<Spine::Table> resultTable(new Spine::Table);

    Spine::TableFormatter::Names headers{"#",
                                         "name",
                                         "type",
                                         "fmisid",
                                         "wsi",
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

    resultTable->setNames(headers);

    auto now = Fmi::SecondClock::universal_time();

    std::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(options.timeformat));

    unsigned int row = 0;
    auto sinfo = itsEngineParameters->stationInfo.load();

    const auto fmisids = sinfo->fmisids();

    for (const auto fmisid : fmisids)
    {
      // Check data against options
      if (!options.fmisid.empty() && options.fmisid.count(fmisid) == 0)
        continue;

      // Get all variants of the fmisid
      std::vector<int> dummy{fmisid};
      auto all_locations = sinfo->findFmisidStations(dummy);

      if (all_locations.empty())  // safety check
        continue;

      auto groups = groupStations(all_locations, options, now);

      if (groups.empty())  // may happen when filtering stations
        continue;

      addStationGroupRows(*resultTable, row, all_locations, groups, *timeFormatter);
    }
    return resultTable;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Fmi::Cache::CacheStatistics EngineImpl::getCacheStats() const
{
  Fmi::Cache::CacheStatistics ret;

  // Disk and memory caches
  const ObservationCaches &caches = itsEngineParameters->observationCacheProxy->getCachesByName();

  for (const auto &item : caches)
  {
    auto cache_name = item.first;
    auto cache_stats = item.second->getCacheStats();
    for (const auto &stats : cache_stats)
    {
      auto key = ("Observation::" + cache_name + "::" + stats.first);
      ret.insert(std::make_pair(key, stats.second));
    }
  }

  // "query_result_cache" is used by wfs makeQuery function
  ret.insert(std::make_pair("Observation::query_result_cache",
                            itsEngineParameters->queryResultBaseCache.statistics()));

  // Station resolution caches shared by all database drivers
  auto info = itsEngineParameters->stationInfo.load();
  if (info)
    ret.insert(std::make_pair("Observation::nearest_station_cache", info->candidateCacheStats()));

  ret.insert(
      std::make_pair("Observation::geoid_cache", itsEngineParameters->geoIdCache.statistics()));

  // Get private caches from drivers (Oracle-driver has some)
  auto private_caches = itsDatabaseDriver->getCacheStats();
  ret.insert(private_caches.begin(), private_caches.end());

  return ret;
}

std::set<uint> EngineImpl::getProducerIds(const std::string &producer) const
{
  try
  {
    std::set<uint> ret;
    auto endtime = Fmi::SecondClock::universal_time();
    auto starttime = (endtime - Fmi::Hours(168));  // 7*24: one week
    // Read from DB
    ret = itsEngineParameters->producerGroups.getProducerIds(producer, starttime, endtime);

    if (ret.empty())
    {
      // Read from config file
      const auto &producerIdSetMap = itsEngineParameters->stationtypeConfig.getProducerIdSetMap();
      if (producerIdSetMap.find(producer) != producerIdSetMap.end())
        ret = producerIdSetMap.at(producer);
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed");
  }
}

// Add the measurands whose measurand or combined code is a parameter of the producer
void EngineImpl::addMeasurands(MeasurandInfo &mi,
                               const std::set<const measurand_info *> &measurands,
                               const std::string &producer) const
{
  for (const auto &m : measurands)
  {
    if (isParameter(m->measurand_code, producer))
    {
      auto parameter_name = m->measurand_code;
      boost::algorithm::to_lower(parameter_name);
      mi[parameter_name] = *m;
    }
    if (isParameter(m->combined_code, producer))
    {
      auto parameter_name = m->combined_code;
      boost::algorithm::to_lower(parameter_name);
      mi[parameter_name] = *m;
    }
  }
}

void EngineImpl::initMeasurandInfo()
{
  try
  {
    auto m_info = itsDatabaseDriver->getMeasurandInfo();

    // producer_id -> measurands
    std::map<uint, std::set<const measurand_info *>> producer_id_measurands;
    for (const auto &item : m_info)
    {
      for (const auto &producer_id : item.second.producers)
      {
        producer_id_measurands[producer_id].insert(&item.second);
      }
    }

    auto producers = getValidStationTypes();

    // Producers
    for (const auto &producer : producers)
    {
      MeasurandInfo mi;
      // Producer ids of a producer
      auto producer_ids = getProducerIds(producer);
      for (auto id : producer_ids)
      {
        if (producer_id_measurands.find(id) != producer_id_measurands.end())
        {
          auto measurands = producer_id_measurands.at(id);
          addMeasurands(mi, measurands, producer);
        }
      }
      if (!mi.empty())
        itsMeasurandInfo[producer] = mi;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

const ProducerMeasurandInfo &EngineImpl::getMeasurandInfo() const
{
  return itsMeasurandInfo;
}

std::vector<std::string> EngineImpl::getDerivedParameters(const std::string &stationtype) const
{
  try
  {
    struct DerivedParameter
    {
      std::vector<std::string> names;
      std::vector<std::string> sources;
      bool qc_supported;  // calculated also for the road and foreign station types
    };

    static const std::vector<DerivedParameter> derived_parameters = {
        {{"windcompass8", "windcompass16", "windcompass32"}, {"winddirection"}, true},
        {{"feelslike"}, {"windspeedms", "relativehumidity", "temperature"}, false},
        {{"smartsymbol", "smartsymboltext"}, {"wawa", "totalcloudcover", "temperature"}, false},
        {{"cloudceiling", "cloudceilingft", "cloudceilinghft"},
         {"cla1_pt1m_acc",
          "cla2_pt1m_acc",
          "cla3_pt1m_acc",
          "cla4_pt1m_acc",
          "cla5_pt1m_acc",
          "clhb1_pt1m_instant",
          "clhb2_pt1m_instant",
          "clhb3_pt1m_instant",
          "clhb4_pt1m_instant",
          "clh5_pt1m_instant"},
         false}};

    // Producer 'fmi' is deprecated, the database queries use 'observations_fmi' instead
    const std::string dbstationtype = (stationtype == "fmi" ? "observations_fmi" : stationtype);
    const bool is_qc_table = (dbstationtype == "foreign" || dbstationtype == "road");

    std::vector<std::string> ret;
    for (const auto &derived : derived_parameters)
    {
      if (is_qc_table && !derived.qc_supported)
        continue;

      const bool available = std::all_of(
          derived.sources.begin(),
          derived.sources.end(),
          [&](const std::string &source)
          { return !getParameterIdAsString(source, dbstationtype).empty(); });
      if (available)
        ret.insert(ret.end(), derived.names.begin(), derived.names.end());
    }

    // roadww and roadwawa are calculated only for road weather stations, from the
    // precipitation intensity (prclst) and form (prst2)
    if (dbstationtype == "road" && !getParameterIdAsString("prclst", dbstationtype).empty() &&
        !getParameterIdAsString("prst2", dbstationtype).empty())
    {
      ret.emplace_back("roadww");
      ret.emplace_back("roadwawa");
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!").addParameter("Station type", stationtype);
  }
}

Fmi::DateTime EngineImpl::getLatestDataUpdateTime(const std::string &producer,
                                                  const Fmi::DateTime &from) const
{
  return itsDatabaseDriver->getLatestDataUpdateTime(producer, from);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
