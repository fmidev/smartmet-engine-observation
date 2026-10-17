#pragma once

#include <macgyver/DateTime.h>
#include <cstddef>
#include <set>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// ----------------------------------------------------------------------
/*!
 * \brief What to list the sensors of
 *
 * The sensor numbers of a parameter are not consistent between stations: the same number may be
 * the default sensor at one station and an additional one at another. The sensors are hence
 * listed per station, parameter and sensor. The input is validated, no part of it is passed to
 * the databases as such.
 */
// ----------------------------------------------------------------------

struct SensorOptions
{
  std::string producer;              // The station type, for example observations_fmi
  std::set<int> fmisids;             // The stations, all of them when empty
  std::set<std::string> parameters;  // Parameter names of the producer, all when empty
  Fmi::DateTime starttime;           // The period of the configurations or the data,
  Fmi::DateTime endtime;             // the current time when not a date time
};

// ----------------------------------------------------------------------
/*!
 * \brief A sensor of a parameter at a station
 *
 * The configuration period comes from the metadata of the database when it has one, the data
 * period and the number of values from the metadata or from the data itself. Unknown times are
 * not a date time, an unknown number of values is zero.
 */
// ----------------------------------------------------------------------

struct SensorInfo
{
  int fmisid = 0;
  int measurand_id = 0;
  std::string measurand_code;          // The code of the parameter in the database, e.g. TA
  std::vector<std::string> parameters;  // The parameter names of the producer
  int sensor_no = 0;
  bool is_default = false;  // The values of a parameter requested without a sensor number
  Fmi::DateTime configuration_start;
  Fmi::DateTime configuration_end;
  Fmi::DateTime first_data;
  Fmi::DateTime last_data;
  std::size_t values = 0;
};

// ----------------------------------------------------------------------
/*!
 * \brief The sensors found, or why the producer has none listed
 */
// ----------------------------------------------------------------------

struct SensorResult
{
  bool available = false;  // Whether the source of the producer can list its sensors
  std::string note;        // Why not, or how the list was made
  std::vector<SensorInfo> sensors;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
