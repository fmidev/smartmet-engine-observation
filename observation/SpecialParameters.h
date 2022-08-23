#pragma once

#include "Settings.h"
#include <boost/optional.hpp>
#include <macgyver/Astronomy.h>
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

    const Fmi::Astronomy::solar_position_t& get_solar_position() const;
    const Fmi::Astronomy::solar_time_t& get_solar_time() const;
    const Fmi::Astronomy::lunar_time_t& get_lunar_time() const;
    Spine::LocationPtr get_location(Geonames::Engine* engine) const;
    const std::string& get_tz_name() const
    {
      return timeZone == "localtime" ? station.timezone : timeZone;
    }
    boost::local_time::time_zone_ptr get_tz() const;

   private:
    mutable std::unique_ptr<Fmi::Astronomy::solar_position_t> solar_position;
    mutable std::unique_ptr<Fmi::Astronomy::solar_time_t> solar_time;
    mutable std::unique_ptr<Fmi::Astronomy::lunar_time_t> lunar_time;
    mutable boost::optional<Spine::LocationPtr> location_ptr;
    mutable boost::local_time::time_zone_ptr tz;
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
  typedef std::function<TS::Value(const Args&)> parameter_handler_t;

  std::map<std::string, parameter_handler_t> handler_map;
  Geonames::Engine* itsGeonames;

  std::unique_ptr<Fmi::TimeFormatter> tf;
  boost::local_time::time_zone_ptr utc_tz;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
