#include "Settings.h"
#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
Settings::Settings()
{
  try
  {
    boost::posix_time::ptime now = boost::posix_time::second_clock::universal_time();
    boost::posix_time::ptime defaultTime =
        boost::posix_time::second_clock::universal_time() - boost::posix_time::hours(24);

    // Set defaults for data members
    // These are taken from obsplugin's queryparser. Most of these should be read from a config file
    allplaces = false;
    endtime = now;
    format = "ascii";
    language = "fi";
    latest = false;
    localename = "fi_FI";
    locale = std::locale(localename.c_str());
    maxdistance = 50000;
    missingtext = "nan";
    numberofstations = 1;
    starttime = defaultTime;
    stationtype = "fmi";
    timeformat = "timestamp";
    timestep = 1;
    timestring = "";
    timezone = "localtime";
    boundingBoxIsGiven = false;
    useDataCache = true;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
