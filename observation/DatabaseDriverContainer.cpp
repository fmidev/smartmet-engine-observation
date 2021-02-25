#include "DatabaseDriverContainer.h"
#include <macgyver/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
bool sort_function(const DatabaseDriverDays &first, const DatabaseDriverDays &second)
{
  return (first.max_days < second.max_days);
}

void DatabaseDriverContainer::addDriver(const std::string &tablename,
                                        int max_days,
                                        DatabaseDriverBase *driver)
{
  itsDatabaseDrivers[tablename].emplace_back(max_days, driver);
}

DatabaseDriverBase *DatabaseDriverContainer::resolveDriver(
    const std::string &tablename,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime) const
{
  try
  {
    if (itsDatabaseDrivers.find(tablename) == itsDatabaseDrivers.end())
    {
      // If no active drivers defined, dummy driver for all table names '*' is created
      if (itsDatabaseDrivers.find("*") != itsDatabaseDrivers.end())
        return itsDatabaseDrivers.at("*").at(0).driver;

      throw Fmi::Exception::Trace(
          BCP, "Error! No database driver found for reqested table: '" + tablename + "'");
    }

    boost::posix_time::time_period requested_period(starttime, endtime);

    std::vector<DatabaseDriverDays> driver_days = itsDatabaseDrivers.at(tablename);
    // Sort to ascending order and return the first driver that matches 'max_days' criterion
    std::sort(driver_days.begin(), driver_days.end(), sort_function);

    if (driver_days.size() > 0)
    {
      // If no starttime / endtime given, return the first driver
      if (starttime.is_not_a_date_time() || endtime.is_not_a_date_time())
        return driver_days.at(0).driver;

      auto now = boost::posix_time::second_clock::universal_time();

      for (const auto item : driver_days)
      {
        if (item.max_days == INT_MAX)
        {
          return item.driver;
        }

        boost::posix_time::ptime driver_data_start_time =
            (now - boost::gregorian::days(item.max_days));

        if (starttime >= driver_data_start_time)
        {
          return item.driver;
        }
      }
    }

    throw Fmi::Exception::Trace(
        BCP,
        "Error! No database driver found for requested table and period: " + tablename + " -> " +
            boost::posix_time::to_simple_string(requested_period));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverContainer::resolveDriver function failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
