#include "EngineImpl.h"
#include "DBRegistry.h"
#include "DatabaseDriverFactory.h"
#include "ObservationCacheFactory.h"
#include "SpecialParameters.h"
#include "StationOptions.h"
#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <fmt/format.h>
#include <macgyver/Geometry.h>
#include <macgyver/Join.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TypeName.h>
#include <spine/ConfigTools.h>
#include <spine/Convenience.h>
#include <spine/Reactor.h>
#include <timeseries/ParameterTools.h>
#include <timeseries/TimeSeriesInclude.h>

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

void afterQuery(TS::TimeSeriesVectorPtr &tsvPtr,
                const Settings &settings,
                const std::vector<unsigned int> &unknownParameterIndexes)
{
  try
  {
    if (tsvPtr->empty())
      return;

    if (!unknownParameterIndexes.empty())
    {
      // Take copy of the first time series
      TS::TimeSeries ts = tsvPtr->at(0);
      // Set values in all timestesps to TS::None()
      for (auto &timedvalue : ts)
        timedvalue.value = TS::None();
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
    TS::TimeSeriesVectorPtr result = TS::TimeSeriesVectorPtr(new TS::TimeSeriesVector);
    for (unsigned int i = 0; i < tsvPtr->size(); i++)
      result->emplace_back(TS::TimeSeries());

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
        const TS::TimeSeries &ts = tsvPtr->at(i);

        TS::TimeSeries &resultVector = result->at(i);

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

    tsvPtr = result;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace

EngineImpl::EngineImpl(const std::string &configfile)
    : itsConfigFile(configfile), itsDatabaseRegistry(new DBRegistry())
{
}

void EngineImpl::init()
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

    itsDatabaseDriver.reset(DatabaseDriverFactory::create(itsEngineParameters, cfg));
    if (itsDatabaseDriver)
    {
      logMessage(
          "[Observation EngineImpl] Database driver '" + itsDatabaseDriver->name() + "' created",
          itsEngineParameters->quiet);
      itsDatabaseDriver->init(this);
    }

    SpecialParameters::setGeonames(getGeonames());
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

bool EngineImpl::requestReloadStations(const Spine::HTTP::Request &theRequest)
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
  std::cout << "  -- Shutdown requested (Observation)" << std::endl;
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
  std::cout << "Warning: obsengine::ready called" << std::endl;
  return true;
}

Geonames::Engine *EngineImpl::getGeonames() const
{
  // this will wait until the engine is ready
  auto *engine = itsReactor->getSingleton("Geonames", nullptr);
  return reinterpret_cast<Geonames::Engine *>(engine);
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
      std::cout << "EngineImpl::Observation::Settings:\n" << settings << std::endl;
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

TS::TimeSeriesVectorPtr EngineImpl::values(Settings &settings,
                                           const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {  // Drop unknown parameters from parameter list and
    // store their indexes
    std::vector<unsigned int> unknownParameterIndexes;
    if (settings.debug_options & Settings::DUMP_SETTINGS)
    {
      std::cout << "EngineImpl::Observation::Settings:\n" << settings << std::endl;
      std::cout << "TS::TimeSeriesGeneratorOptions:\n" << timeSeriesOptions << std::endl;
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
        if (producer.empty() || (producer == producer_param.first))
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
        ret.parameters.push_back(
            Spine::Parameter("level", Spine::Parameter::Type::DataIndependent));
        continue;
      }

      if (!isParameter(pname, settings.stationtype) && !TimeSeries::is_special_parameter(pname))
      {
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

    return resultTable;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

ContentTable EngineImpl::getStationInfo(const StationOptions &options) const
{
  struct StationGroup
  {
    std::vector<std::size_t> indexes;
    std::vector<std::string> stationtypes;
  };

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

    const bool check_fmisid = !options.fmisid.empty();
    const bool check_wsi = !options.wsi.empty();
    const bool check_lpnn = !options.lpnn.empty();
    const bool check_wmo = !options.wmo.empty();
    const bool check_rwsid = !options.rwsid.empty();
    const bool check_type = !options.type.empty();
    const bool check_name = !options.name.empty();
    const bool check_iso2 = !options.iso2.empty();
    const bool check_region = !options.region.empty();
    const bool check_bbox = (options.bbox != std::nullopt);
    const bool only_starttime =
        (!options.start_time.is_not_a_date_time() && options.end_time.is_not_a_date_time());
    const bool only_endtime =
        (options.start_time.is_not_a_date_time() && !options.end_time.is_not_a_date_time());
    const bool neither_time =
        (options.start_time.is_not_a_date_time() && options.end_time.is_not_a_date_time());
    const bool both_times =
        (!options.start_time.is_not_a_date_time() && !options.end_time.is_not_a_date_time());

    auto now = Fmi::SecondClock::universal_time();

    std::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(options.timeformat));

    unsigned int row = 0;
    auto sinfo = itsEngineParameters->stationInfo.load();

    const auto fmisids = sinfo->fmisids();

    for (const auto fmisid : fmisids)
    {
      // Check data against options
      if (check_fmisid && options.fmisid.count(fmisid) == 0)
        continue;

      // Get all variants of the fmisid
      std::vector<int> dummy{fmisid};
      auto all_locations = sinfo->findFmisidStations(dummy);

      if (all_locations.empty())  // safety check
        continue;

      // Group stations by station starttime, endtime, coordinates and elevation by storing the
      // indexes into the all_locations vector

      std::vector<StationGroup> groups;

      for (auto i = 0UL; i < all_locations.size(); i++)
      {
        const auto &s = all_locations[i];

        // Check station against options

        if (check_wsi && options.wsi.count(s.wsi) == 0)
          continue;
        if (check_lpnn && options.lpnn.count(s.lpnn) == 0)
          continue;
        if (check_wmo && options.wmo.count(s.wmo) == 0)
          continue;
        if (check_rwsid && options.rwsid.count(s.rwsid) == 0)
          continue;
        if (check_type && !string_found(s.type, options.type))
          continue;
        if (check_name && !string_found(s.formal_name_fi, options.name))
          continue;
        if (check_iso2 && !string_found(s.iso2, options.iso2))
          continue;
        if (check_region && !string_found(s.region, options.region))
          continue;

        if (check_bbox)
        {
          if (s.longitude < (*options.bbox).xMin || s.longitude > (*options.bbox).xMax ||
              s.latitude < (*options.bbox).yMin || s.latitude > (*options.bbox).yMax)
            continue;
        }

        // Check station time periods
        if (only_starttime && s.station_end < options.start_time)
          continue;
        if (only_endtime && s.station_start > options.end_time)
          continue;
        if (neither_time && (now < s.station_start || now > s.station_end))
          continue;
        if (both_times &&
            (s.station_start > options.end_time || s.station_end < options.start_time))
          continue;

        // Station accepted, assign it into a group with similar metadata

        bool matched = false;
        for (auto &group : groups)
        {
          for (auto pos : group.indexes)
          {
            const auto &station2 = all_locations.at(pos);
            matched = (s.station_start == station2.station_start &&
                       s.station_end == station2.station_end && s.longitude == station2.longitude &&
                       s.latitude == station2.latitude && s.elevation == station2.elevation);
            if (matched)
            {
              group.indexes.push_back(i);
              group.stationtypes.push_back(s.type);
              break;
            }
          }
        }
        if (!matched)
        {
          StationGroup group;
          group.indexes.push_back(i);
          group.stationtypes.push_back(s.type);
          groups.push_back(group);
        }
      }

      if (groups.empty())  // may happen when filtering stations
        continue;

      // Print the information for the groups
      for (const auto &group : groups)
      {
        const auto &s = all_locations.at(group.indexes.at(0));  // representative station
        unsigned int column = 0;
        auto grouplist = Fmi::join(group.stationtypes, ", ");
        resultTable->set(column++, row, Fmi::to_string(row + 1));                 // Row number
        resultTable->set(column++, row, s.station_formal_name("fi"));             // Name
        resultTable->set(column++, row, grouplist);                               // Type
        resultTable->set(column++, row, Fmi::to_string(s.fmisid));                // FMISID
        resultTable->set(column++, row, s.wsi);                                   // WIGOS ID
        resultTable->set(column++, row, Fmi::to_string(s.wmo));                   // WMO
        resultTable->set(column++, row, Fmi::to_string(s.lpnn));                  // LPNN
        resultTable->set(column++, row, Fmi::to_string(s.rwsid));                 // RWSID
        resultTable->set(column++, row, Fmi::to_string(s.longitude));             // Longitude
        resultTable->set(column++, row, Fmi::to_string(s.latitude));              // Latitude
        resultTable->set(column++, row, Fmi::to_string(s.elevation));             // Elevation
        resultTable->set(column++, row, timeFormatter->format(s.station_start));  // Start date
        resultTable->set(column++, row, timeFormatter->format(s.station_end));    // End date
        resultTable->set(column++, row, s.timezone);                              // Timezone
        resultTable->set(column++, row, s.iso2);                                  // Country
        resultTable->set(column++, row, s.region);                                // Region
        row++;
      }
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

Fmi::DateTime EngineImpl::getLatestDataUpdateTime(const std::string &producer,
                                                  const Fmi::DateTime &from) const
{
  return itsDatabaseDriver->getLatestDataUpdateTime(producer, from);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
