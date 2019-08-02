#include "ObservationMemoryCache.h"

#include "QueryMapping.h"

#include <boost/atomic.hpp>
#include <boost/make_shared.hpp>
#include <spine/Exception.h>

#include <prettyprint.hpp>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// After the cache has been initialized, we store the time of the
// latest deleted observations instead of the actual last
// observation.

boost::posix_time::ptime ObservationMemoryCache::getStartTime() const
{
  try
  {
    auto t = boost::atomic_load(&itsStartTime);
    if (t)
      return *t;

    return {boost::posix_time::not_a_date_time};
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "ObservationMemoryCache::getStartTime failed");
  }
}

std::size_t ObservationMemoryCache::fill(const DataItems& cacheData) const
{
  try
  {
    // The update is sorted first by fmisid and by time, but can contain duplicates.
    // First we discard the ones whose observation time is less than the latest
    // time in the cache, then we skip the ones which are already in the cache

    // Collect new items

    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;

    for (std::size_t i = 0; i < cacheData.size(); i++)
    {
      const auto& item = cacheData[i];

      auto hash = item.hash_value();

      if (itsHashValues.find(hash) == itsHashValues.end())
      {
        new_items.push_back(i);
        new_hashes.push_back(hash);
      }
    }

    if (!new_items.empty())
    {
      // Load the cache
      auto cache = boost::atomic_load(&itsObservations);

      // Since no-one else can modify the cache at the same time, we can make a safe copy
      // by copying the shared pointers in the unordered map.

      if (cache)
        cache = boost::make_shared<Observations>(*cache);
      else
        cache = boost::make_shared<Observations>();  // first insert to the cache

      // Add the new data to our own copy
      auto& observations = *cache;

      for (std::size_t i = 0; i < new_items.size();)
      {
        const auto& data = cacheData[new_items[i]];

        // Seek the end of the update for this station
        auto fmisid = data.fmisid;

        std::size_t j = i + 1;
        for (; j < new_items.size(); j++)
          if (fmisid != cacheData[new_items[j]].fmisid)
            break;

        // Copy old station observations, or create a new shared empty vector

        auto pos = observations.find(fmisid);
        if (pos == observations.end())
          pos =
              observations.insert(std::make_pair(fmisid, boost::make_shared<StationObservations>()))
                  .first;

        // Shorthand alias for the shared station observations to make code more readable
        auto& shared_obs = pos->second;

        auto newobs = boost::make_shared<StationObservations>(*shared_obs);

        // Indices i...j-1 have the same fmisid

        for (std::size_t k = i; k < j; k++)
          newobs->push_back(cacheData[new_items[k]]);

        // Sort the data based on time

        auto cmp = [](const DataItem& obs1, const DataItem& obs2) -> bool {
          return (obs1.data_time < obs2.data_time);
        };
        std::sort(shared_obs->begin(), shared_obs->end(), cmp);

        // And store the new station data, no need for atomics since we own this shared_ptr
        shared_obs = newobs;

        // Move on to the next station
        i = j;
      }

      // Mark them inserted based on hash value
      for (const auto& hash : new_hashes)
        itsHashValues.insert(hash);

      // Replace old contents
      boost::atomic_store(&itsObservations, cache);
    }

    // Indicate fill has been called once

    auto starttime = boost::atomic_load(&itsStartTime);
    if (!starttime)
    {
      starttime = boost::make_shared<boost::posix_time::ptime>(boost::posix_time::not_a_date_time);
      boost::atomic_store(&itsStartTime, starttime);
    }

    return new_items.size();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "ObservationMemoryCache::fill failed");
  }
}

void ObservationMemoryCache::clean(const boost::posix_time::ptime& newstarttime) const
{
  try
  {
    bool must_clean = false;

    auto cache = boost::atomic_load(&itsObservations);

    if (cache)
    {
      // Make a new copy to be cleaned.
      cache = boost::make_shared<Observations>(*cache);

      for (auto& fmisid_data : *cache)
      {
        // Find first position newer than the given start time

        auto cmp = [](const boost::posix_time::ptime& t, const DataItem& obs) -> bool {
          return (obs.data_time > t);
        };

        auto& obsdata = fmisid_data.second;
        auto pos = std::upper_bound(obsdata->begin(), obsdata->end(), newstarttime, cmp);

        if (pos != obsdata->end())
        {
          must_clean = true;

          // Remove elements from the cache by making a new copy of the elements to be kept

          for (auto it = obsdata->begin(); it != pos; ++it)
            itsHashValues.erase(it->hash_value());

          // And replace our copy of the shared_ptr
          obsdata = boost::make_shared<StationObservations>(pos, obsdata->end());
        }
      }
    }

    // Update new start time for the cache first so no-one can request data before it
    // before the data has been cleaned
    auto starttime = boost::make_shared<boost::posix_time::ptime>(newstarttime);
    boost::atomic_store(&itsStartTime, starttime);

    // And now a quick atomic update to the data too, if we deleted anything. If we actually
    // didn't delete anything after all, we keep the original cache to keep it hot in memory.

    if (must_clean)
      boost::atomic_store(&itsObservations, cache);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "ObservationMemoryCache::clean failed");
  }
}

LocationDataItems ObservationMemoryCache::read_observations(const Spine::Stations& stations,
                                                            const Settings& settings,
                                                            const QueryMapping& qmap) const
{
  LocationDataItems ret;

  auto cache = boost::atomic_load(&itsObservations);

  if (!cache)
    return ret;

  std::cout << "QUERY:\n"
            << "timeseriesPositions = " << qmap.timeseriesPositions << "\n"
            << "timeseriesPositionsString = " << qmap.timeseriesPositionsString << "\n"
            << "parameterNameMap = " << qmap.parameterNameMap << "\n"
            << "paramVector = " << qmap.paramVector << "\n"
            << "specialPositions = " << qmap.specialPositions << "\n"
            << "measurandIds = " << qmap.measurandIds << "\n\n";

  // TODO:
  // 1. loop over stations
  // 2. find desired time interval
  // 3. extract wanted measurand_id's
  // 4. attach latitude, longitude and elevation for each fmisid

  for (const auto& station : stations)
  {
    // Find station specific data
    const auto& pos = cache->find(station.fmisid);
    if (pos == cache->end())
      continue;
    const auto& obsdata = *(pos->second);

    // Find first position >= than the given start time

    auto cmp = [](const boost::posix_time::ptime& t, const DataItem& obs) -> bool {
      return (obs.data_time >= t);
    };

    auto obs = std::upper_bound(obsdata.begin(), obsdata.end(), settings.starttime, cmp);

    // Skip station if there is no data in the interval starttime...endtime
    if (obs == obsdata.end())
      continue;

    // Extract wanted parameters.
    // qmap.paramVector contains the wanted measurand_ids.

    for (; obs < obsdata.end(); ++obs)
    {
    }
  }

#if 0  
  std::string sql =
      "SELECT data.fmisid AS fmisid, data.data_time AS obstime, "
      "loc.latitude, loc.longitude, loc.elevation, "
      "measurand_id, data_value, data_source "
      "FROM observation_data data JOIN locations loc ON (data.fmisid = "
      "loc.fmisid) "
      "WHERE data.fmisid IN (" +
      qstations +
      ") "
      "AND data.data_time >= '" +
      Fmi::to_iso_extended_string(settings.starttime) + "' AND data.data_time <= '" +
      Fmi::to_iso_extended_string(settings.endtime) + "' AND data.measurand_id IN (" + qmap.param +
      ") "
      "AND data.measurand_no = 1 "
      "AND data.data_quality <= 5 "
      "GROUP BY data.fmisid, data.data_time, data.measurand_id, "
      "loc.location_id, "
      "loc.location_end, "
      "loc.latitude, loc.longitude, loc.elevation, data.data_value, data.data_source "
      "ORDER BY fmisid ASC, obstime ASC";


  sqlite3pp::query qry(db, sql.c_str());

  for (auto iter = qry.begin(); iter != qry.end(); ++iter)
  {
    LocationDataItem obs;
    obs.data.fmisid = (*iter).get<int>(0);
    obs.data.data_time = parse_sqlite_time((*iter).get<std::string>(1));
    obs.latitude = (*iter).get<double>(2);
    obs.longitude = (*iter).get<double>(3);
    obs.elevation = (*iter).get<double>(4);
    obs.data.measurand_id = (*iter).get<int>(5);
    if ((*iter).column_type(6) != SQLITE_NULL)
      obs.data.data_value = (*iter).get<double>(6);
    if ((*iter).column_type(7) != SQLITE_NULL)
      obs.data.data_source = (*iter).get<int>(7);

    ret.emplace_back(obs);
  }

#endif

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
