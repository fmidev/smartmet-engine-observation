#include "ObservationMemoryCache.h"
#include "QueryMapping.h"
#include "StationInfo.h"
#include <boost/atomic.hpp>
#include <boost/make_shared.hpp>
#include <macgyver/Exception.h>

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
    throw Fmi::Exception::Trace(BCP, "ObservationMemoryCache::getStartTime failed");
  }
}

// Add new observations to the cache.
//
// In principle, if a new station appears, we must update the master shared_ptr to all the
// observations. Otherwise we need only update one station at a time and keep the master
// shared_ptr valid.
//
// However, the logic needed to keep track if there are any new stations is overly complex
// when compared to simply copying the std::map of station numbers to shared pointers of
// data (usually of the order of 1000). Copying the map is safe, since no other writer
// is assumed to be active.
//
// Hence we always copy the initial map with its shared pointers. If some station
// is updated, we do not need to update the shared pointer atomically, it is a
// new shared pointer whose update does not update the older observations.
// Updating the shared pointer to all observations finally updates all reader
// views of the data which are not then active. The views active at the moment
// of the update will still see the old data in the previous shared_ptr, since
// they loaded it atomically. We do not modify the shared data either, so it
// stays valid for all the readers.

std::size_t ObservationMemoryCache::fill(const DataItems& cacheData) const
{
  try
  {
    // The update is sorted first by fmisid and by time, but can contain duplicates.
    // We discard all observations currently in the cache based on the hash value.
    // If some observation has changed, its hash value has changed, and it will
    // pass into the modification phase.
    // Collect all observations not currently in the cache.

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

    // Create a new cache only if there are updates

    if (!new_items.empty())
    {
      // Make a new cache
      auto new_cache = boost::make_shared<Observations>();

      // Copy pointers to existing observations if there are any
      if (itsObservations)
      {
        auto old_cache = boost::atomic_load(&itsObservations);
        for (auto& id_observations : *old_cache)
        {
          auto id = id_observations.first;
          auto obs = boost::atomic_load(&id_observations.second);
          new_cache->insert(std::make_pair(id, obs));
        }
      }

      // The shared_ptrs now point to the original observations. If we reset
      // the data, we do not disturb readers reading the original data, since these
      // shared_ptrs are our own, and we a free to reset them.

      // Add the new data to our own copy
      auto& observations = *new_cache;

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

        // Copy all old observations
        auto newobs = boost::make_shared<StationObservations>(*shared_obs);

#if 0        
        auto newdata_start = newobs->end();
#endif

        // Indices i...j-1 have the same fmisid

        for (std::size_t k = i; k < j; k++)
          newobs->push_back(cacheData[new_items[k]]);

        // Sort the data based on time

        auto cmp = [](const DataItem& obs1, const DataItem& obs2) -> bool
        { return (obs1.data_time < obs2.data_time); };

        // The two segments are already sorted. However, on RHEL7 the inplace_merge does not
        // produce a sorted container. Could not find a bug by searching the net, but most
        // definitely it does not work as expected.

#if 0
        std::inplace_merge(newobs->begin(), newdata_start, newobs->end(), cmp);
#else
        std::sort(newobs->begin(), newobs->end(), cmp);
#endif

        // And store the new station data
        boost::atomic_store(&shared_obs, newobs);

        // Move on to the next station
        i = j;
      }

      // Mark them inserted based on hash value
      for (const auto& hash : new_hashes)
        itsHashValues.insert(hash);

      // Replace old contents

      boost::atomic_store(&itsObservations, new_cache);
    }

    // Indicate fill has been called once

    auto starttime = boost::atomic_load(&itsStartTime);
    if (!starttime)
    {
      // We intentionally store a not_a_date_time, and let the cache cleaner determine
      // what the oldest observation in the cache is.
      starttime = boost::make_shared<boost::posix_time::ptime>(boost::posix_time::not_a_date_time);
      boost::atomic_store(&itsStartTime, starttime);
    }

    return new_items.size();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "ObservationMemoryCache::fill failed");
  }
}

// Clean the cache from old observations. Only atomics can be used, no locks.
// No new stations are inserted into the shared map, we only need to update
// each station data atomically, not the master map of stations. We do not
// bother removing stations from the map which have stopped observations,
// this is only a RAM cache which will be created afresh at restart anyway.

void ObservationMemoryCache::clean(const boost::posix_time::ptime& newstarttime) const
{
  try
  {
    auto cache_ptr = boost::atomic_load(&itsObservations);
    if (!cache_ptr)
      return;

    // Update new start time for the cache first so no-one can request data before it
    // while the data is being cleaned
    auto starttime = boost::make_shared<boost::posix_time::ptime>(newstarttime);
    boost::atomic_store(&itsStartTime, starttime);

    auto& cache = *cache_ptr;

    for (auto& fmisid_obsdata : cache)
    {
      auto obsdata_ptr = boost::atomic_load(&fmisid_obsdata.second);
      auto& obsdata = *obsdata_ptr;

      // Erase from hash tables all too old observations for this station
      auto it = obsdata.begin();
      for (; it != obsdata.end(); ++it)
      {
        if (it->data_time >= newstarttime)
          break;
        itsHashValues.erase(it->hash_value());
      }
      // Then make a new copy of the remaining data if any deletions were made

      if (it != obsdata.begin())
      {
        // Make a new copy of the data
        auto new_obsdata = boost::make_shared<StationObservations>(it, obsdata.end());

        boost::atomic_store(&fmisid_obsdata.second, new_obsdata);
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "ObservationMemoryCache::clean failed");
  }
}

// Read observations from the cache. Each shared part must be loaded atomically
// to be safe in case write/clean is in progress.

LocationDataItems ObservationMemoryCache::read_observations(
    const Spine::Stations& stations,
    const Settings& settings,
    const StationInfo& stationInfo,
    const std::set<std::string>& stationgroup_codes,
    const QueryMapping& qmap) const
{
  LocationDataItems ret;

  auto cache = boost::atomic_load(&itsObservations);

  if (!cache)
    return ret;

  // 1. loop over stations
  // 2. find desired time interval
  // 3. extract wanted measurand_id's
  // 4. attach latitude, longitude and elevation for each fmisid

  // Valid_sensors, -1 is marker of default sensor
  std::set<int> valid_sensors;
  for (const auto& item : qmap.sensorNumberToMeasurandIds)
    valid_sensors.insert(item.first);

  for (const auto& station : stations)
  {
    // Accept station only if group condition is satisfied
    if (!stationInfo.belongsToGroup(station.fmisid, stationgroup_codes))
      continue;

    // Find station specific data
    const auto& pos = cache->find(station.fmisid);
    if (pos == cache->end())
      continue;

    // Safe shared copy of the station observations right at this moment
    auto obsdata = boost::atomic_load(&pos->second);

    // Find first position >= than the given start time

    auto cmp = [](const DataItem& obs, const boost::posix_time::ptime& t) -> bool
    { return (obs.data_time < t); };

    auto obs = std::lower_bound(obsdata->begin(), obsdata->end(), settings.starttime, cmp);

    // Skip station if there is no data in the interval starttime...endtime
    if (obs == obsdata->end())
      continue;

    // Establish station coordinates

    const auto longitude = station.longitude_out;  // not requestedLon!
    const auto latitude = station.latitude_out;    // not requestedLat!
    const auto elevation = station.station_elevation;

    // Extract wanted parameters.

    for (; obs < obsdata->end(); ++obs)
    {
      // Done if reached desired endtime
      if (obs->data_time > settings.endtime)
        break;

      // Skip unwanted parameters similarly to SpatiaLite.cpp read_observations
      // Check sensor number and data_quality condition. The checks should be
      // ordered based on which skips unwanted data the fastest

      // Wanted parameters

      if (std::find(qmap.measurandIds.begin(), qmap.measurandIds.end(), obs->measurand_id) ==
          qmap.measurandIds.end())
        continue;

      // Wanted sensords
      bool sensorOK = false;
      if ((obs->measurand_no == 1 &&
           (valid_sensors.find(-1) != valid_sensors.end() || valid_sensors.empty())) ||
          valid_sensors.find(obs->sensor_no) != valid_sensors.end())
        sensorOK = true;

      if (!sensorOK)
        continue;

      // Required data quality
      bool dataQualityOK = settings.dataFilter.valueOK("data_quality", obs->data_quality);

      if (!dataQualityOK)
        continue;

      // Construct LocationDataItem from the DataItem

      ret.emplace_back(LocationDataItem{*obs, longitude, latitude, elevation});
    }
  }

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
