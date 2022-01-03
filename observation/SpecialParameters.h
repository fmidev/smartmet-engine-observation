#pragma once

#include "Settings.h"
#include <functional>
#include <memory>
#include <spine/Location.h>
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <macgyver/Astronomy.h>
#include <macgyver/TimeFormatter.h>
#include <boost/optional.hpp>

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
        const Spine::Station &station;
        const std::string &stationType;
        const boost::local_time::local_date_time &obstime;
        const boost::local_time::local_date_time &origintime;
        const std::string &timeZone;
        const Settings* settings;

        Args(
            const Spine::Station &station,
            const std::string &stationType,
            const boost::local_time::local_date_time &obstime,
            const boost::local_time::local_date_time &origintime,
            const std::string &timeZone,
            const Settings* settings = nullptr)

            : station(station)
            , stationType(stationType)
            , obstime(obstime)
            , origintime(origintime)
            , timeZone(timeZone)
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
        SmartMet::Spine::LocationPtr get_location(Geonames::Engine* engine) const;

  private:
        mutable std::unique_ptr<Fmi::Astronomy::solar_position_t> solar_position;
        mutable std::unique_ptr<Fmi::Astronomy::solar_time_t> solar_time;
        mutable std::unique_ptr<Fmi::Astronomy::lunar_time_t> lunar_time;
        mutable boost::optional<SmartMet::Spine::LocationPtr> location_ptr;
    };

 private:
    SpecialParameters();

 public:
    virtual ~SpecialParameters() = default;

    static void setGeonames(::SmartMet::Engine::Geonames::Engine* itsGeonames);

    Spine::TimeSeries::Value getValue(const std::string& param_name,
				      const Args& args) const;

    Spine::TimeSeries::TimedValue getTimedValue(const std::string& param_name,
						const Args& args) const;

    Spine::TimeSeries::TimedValue getTimedValue(
        const Spine::Station &station,
        const std::string &stationType,
        const std::string &parameter,
        const boost::local_time::local_date_time &obstime,
        const boost::local_time::local_date_time &origintime,
        const std::string &timeZone,
	const Settings* settings = nullptr) const;

    static const SpecialParameters& instance() { return mutable_instance(); }

 private:
    static SpecialParameters& mutable_instance();

 private:
    typedef std::function <Spine::TimeSeries::Value(const Args&)> parameter_handler_t;

    std::map<std::string, parameter_handler_t> handler_map;
    Geonames::Engine* itsGeonames;

    std::unique_ptr<Fmi::TimeFormatter> tf;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

