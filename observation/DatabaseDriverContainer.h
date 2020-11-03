#pragma once

#include "DatabaseDriverBase.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

struct DatabaseDriverDays
{
  DatabaseDriverDays(int max, DatabaseDriverBase *d) : max_days(max), driver(d) {}
  int max_days;
  DatabaseDriverBase *driver;
};

class DatabaseDriverContainer
{
 public:
  void addDriver(const std::string &tablename, int max_days, DatabaseDriverBase *driver);
  DatabaseDriverBase *resolveDriver(const std::string &tablename,
                                    const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime) const;

 private:
  std::map<std::string, std::vector<DatabaseDriverDays>>
      itsDatabaseDrivers;  // Tablename -> vector of drivers with period
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
