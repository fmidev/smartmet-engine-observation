#pragma once

#include "Settings.h"
#include <boost/optional.hpp>
// #include <macgyver/Astronomy.h>
#include <macgyver/TimeFormatter.h>
#include <macgyver/TimeZoneFactory.h>
#include <spine/Location.h>
#include <spine/Station.h>
#include <timeseries/TimeSeriesInclude.h>
#include <functional>
#include <memory>

namespace SmartMet
{
namespace Engine
{
namespace Geonames
{
class Engine;
}

namespace Observation
{
class SpecialParameters
{
 public:
  struct Args
  {
    const Spine::Station& station;
    const std::string& stationType;
    const boost::local_time::local_date_time& obstime;
    const boost::local_time::local_date_time& origintime;
    const std::string& timeZone;
    const Settings* settings;

    Args(const Spine::Station& station,
         const std::string& stationType,
         const boost::local_time::local_date_time& obstime,
         const boost::local_time::local_date_time& origintime,
         const std::string& timeZone,
         const Settings* settings = nullptr)

        : station(station),
          stationType(stationType),
          obstime(obstime),
          origintime(origintime),
          timeZone(timeZone)
    {
      // Does not seem to be directly available in smartmet-library-delfoi
      // Use fallback settings when not available
      static const Settings fallback_settings;
      this->settings = settings ? settings : &fallback_settings;
    }

    virtual ~Args() = default;

    Spine::LocationPtr get_location(Geonames::Engine* engine) const;
    const std::string& get_tz_name() const
    {
      return timeZone == "localtime" ? station.timezone : timeZone;
    }

   private:
    mutable boost::optional<Spine::LocationPtr> location_ptr;
  };

 private:
  SpecialParameters();

 public:
  virtual ~SpecialParameters() = default;

  static void setGeonames(SmartMet::Engine::Geonames::Engine* itsGeonames);

  TS::Value getValue(const std::string& param_name, const Args& args) const;

  bool is_supported(const std::string& param_name) const;

  TS::TimedValue getTimedValue(const std::string& param_name, const Args& args) const;

  TS::TimedValue getTimedValue(const Spine::Station& station,
                               const std::string& stationType,
                               const std::string& parameter,
                               const boost::local_time::local_date_time& obstime,
                               const boost::local_time::local_date_time& origintime,
                               const std::string& timeZone,
                               const Settings* settings = nullptr) const;

  static const SpecialParameters& instance() { return mutable_instance(); }

 private:
  static SpecialParameters& mutable_instance();

 private:
  using parameter_handler_t = std::function<TS::Value(const Args&)>;

  std::map<std::string, parameter_handler_t> handler_map;
  Geonames::Engine* itsGeonames;

  std::unique_ptr<Fmi::TimeFormatter> tf;
  boost::local_time::time_zone_ptr utc_tz;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
