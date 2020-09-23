#include "FlashMemoryCache.h"
#include "Utils.h"
#include <macgyver/Geometry.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// If there are tagged locations, we must filter flash data based on
// 1) a radius from a point
// 2) a bounding box

bool is_within_search_limits(const FlashDataItem& flash, const Settings& settings)
{
  for (const auto& tloc : settings.taggedLocations)
  {
    if (tloc.loc->type == Spine::Location::CoordinatePoint)
    {
      if (Fmi::Geometry::GeoDistance(
              tloc.loc->longitude, tloc.loc->latitude, flash.longitude, flash.latitude) >
          tloc.loc->radius * 1000)
        return false;
    }
    else if (tloc.loc->type == Spine::Location::BoundingBox)
    {
      Spine::BoundingBox bbox(tloc.loc->name);
      if (flash.longitude < bbox.xMin || flash.longitude > bbox.xMax ||
          flash.latitude < bbox.yMin || flash.latitude > bbox.yMax)
        return false;
    }
  }
  return true;
}

// After the cache has been initialized, we store the time of the
// latest deleted observations instead of the actual last
// observation. For example, there may not be lightning for several days,
// yet the cache should know that the empty state is correct.

boost::posix_time::ptime FlashMemoryCache::getStartTime() const
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
    throw Fmi::Exception::Trace(BCP, "FlashMemoryCache::getStartTime failed");
  }
}

std::size_t FlashMemoryCache::fill(const FlashDataItems& flashCacheData) const
{
  try
  {
    // The update is sorted by stroke_time, but be contain duplicates.
    // First we discard the ones whose stroke_time is less than the latest stroke
    // time in the cache, then we skip the ones which are already in the cache

    // Collect new items

    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;

    for (std::size_t i = 0; i < flashCacheData.size(); i++)
    {
      const auto& item = flashCacheData[i];

      auto hash = item.hash_value();

      if (itsHashValues.find(hash) == itsHashValues.end())
      {
        new_items.push_back(i);
        new_hashes.push_back(hash);
      }
    }

    if (!new_items.empty())
    {
      // Insert new items to the cache

      auto cache = boost::atomic_load(&itsFlashData);

      // Copy the old data
      if (cache)
        cache = boost::make_shared<FlashDataVector>(*cache);
      else
        cache = boost::make_shared<FlashDataVector>();  // first insert to the cache

      // Append new data
      auto& flashvector = *cache;
      for (std::size_t i = 0; i < new_items.size(); i++)
        flashvector.push_back(flashCacheData[new_items[i]]);

      // Mark them inserted based on hash value
      for (const auto& hash : new_hashes)
        itsHashValues.insert(hash);

      // Replace old contents
      boost::atomic_store(&itsFlashData, cache);
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
    throw Fmi::Exception::Trace(BCP, "FlashMemoryCache::fill failed");
  }
}

void FlashMemoryCache::clean(const boost::posix_time::ptime& newstarttime) const
{
  try
  {
    bool must_clean = false;

    auto cache = boost::atomic_load(&itsFlashData);

    if (cache)
    {
      // Find first position newer than the given start time

      auto cmp = [](const boost::posix_time::ptime& t, const FlashDataItem& flash) -> bool {
        return (flash.stroke_time > t);
      };

      auto pos = std::upper_bound(cache->begin(), cache->end(), newstarttime, cmp);

      must_clean = (pos != cache->begin());

      // Remove elements from the cache by making a new copy of the elements to be kept
      if (must_clean)
      {
        for (auto it = cache->begin(); it != pos; ++it)
          itsHashValues.erase(it->hash_value());

        cache = boost::make_shared<FlashDataVector>(pos, cache->end());
      }
    }

    // Update new start time for the cache first so no-one can request data before it
    // before the data has been cleaned
    auto starttime = boost::make_shared<boost::posix_time::ptime>(newstarttime);
    boost::atomic_store(&itsStartTime, starttime);

    // And now a quick atomic update to the data too, if we deleted anything
    if (must_clean)
      boost::atomic_store(&itsFlashData, cache);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "FlashMemoryCache::clean failed");
  }
}

/**
 * @brief Retrieve flash data
 * @param settings Time interval, bbox etc
 * @parameterMap Parameters to retrieve
 * @timezones Global timezone information
 */

Spine::TimeSeries::TimeSeriesVectorPtr FlashMemoryCache::getData(
    const Settings& settings,
    const ParameterMapPtr& parameterMap,
    const Fmi::TimeZones& timezones) const
{
  namespace ts = Spine::TimeSeries;

  try
  {
    auto result = initializeResultVector(settings.parameters);

    auto cache = boost::atomic_load(&itsFlashData);

    // Safety check
    if (!cache)
      return result;

    // Find time interval from the cache data

    auto lcmp = [](const FlashDataItem& flash, const boost::posix_time::ptime& t) -> bool {
      return (flash.stroke_time < t);
    };

    auto pos1 = std::lower_bound(cache->begin(), cache->end(), settings.starttime, lcmp);

    // Nothing to do if there is nothing with a time lower than the starttime, or if there is
    // nothing after it
    if (pos1 == cache->end() || ++pos1 == cache->end())
      return result;

    auto ucmp = [](const boost::posix_time::ptime& t, const FlashDataItem& flash) -> bool {
      return (flash.stroke_time > t);
    };

    auto pos2 = std::upper_bound(cache->begin(), cache->end(), settings.endtime, ucmp);

    // pos1...pos2 is now the inclusive range to be checked against other search conditions

    // Collect normalized parameter names
    std::vector<std::string> column_names;
    for (const Spine::Parameter& p : settings.parameters)
    {
      std::string name = p.name();
      boost::to_lower(name, std::locale::classic());
      if (!not_special(p))
        column_names.push_back(name);
      else
      {
        std::string pname = parameterMap->getParameter(name, "flash");
        if (!pname.empty())
        {
          boost::to_lower(pname, std::locale::classic());
          column_names.push_back(pname);
        }
      }
    }

    // Collect the results

    auto localtz = timezones.time_zone_from_string(settings.timezone);

    for (auto pos = pos1; pos <= pos2; ++pos)
    {
      const auto& flash = *pos;

      if (!is_within_search_limits(flash, settings))
        continue;

      // Append to output

      boost::local_time::local_date_time localtime(flash.stroke_time, localtz);

      for (std::size_t i = 0; i < column_names.size(); i++)
      {
        const auto& name = column_names[i];

        ts::Value val;  // missing value

        // strcmp is slow, but reordering the loops would look ugly
        if (name == "longitude")
          val = flash.longitude;
        else if (name == "latitude")
          val = flash.latitude;
        else if (name == "multiplicity")
          val = flash.multiplicity;
        else if (name == "peak_current")
          val = flash.peak_current;
        else if (name == "cloud_indicator")
          val = flash.cloud_indicator;
        else if (name == "angle_indicator")
          val = flash.angle_indicator;
        else if (name == "signal_indicator")
          val = flash.signal_indicator;
        else if (name == "timing_indicator")
          val = flash.timing_indicator;
        else if (name == "stroke_status")
          val = flash.stroke_status;
        else if (name == "data_source")
          val = flash.data_source;
        else if (name == "sensors")
          val = flash.sensors;
        else if (name == "flash_id")
          val = static_cast<int>(flash.flash_id);
        else if (name == "freedom_degree")
          val = flash.freedom_degree;
        else if (name == "ellipse_angle")
          val = flash.ellipse_angle;
        else if (name == "ellipse_major")
          val = flash.ellipse_major;
        else if (name == "ellipse_minor")
          val = flash.ellipse_minor;
        else if (name == "chi_square")
          val = flash.chi_square;
        else if (name == "rise_time")
          val = flash.rise_time;
        else if (name == "ptz_time")
          val = flash.ptz_time;

        result->at(i).push_back(ts::TimedValue(localtime, val));
      }
    }

    return result;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "FlashMemoryCache::getData failed");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
