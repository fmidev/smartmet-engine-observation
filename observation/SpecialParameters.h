#pragma once

#include <functional>
#include <memory>
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <macgyver/Astronomy.h>
#include <macgyver/TimeFormatter.h>

namespace SmartMet
{
namespace Engine
{
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

        Args(
            const Spine::Station &station,
            const std::string &stationType,
            const boost::local_time::local_date_time &obstime,
            const boost::local_time::local_date_time &origintime,
            const std::string &timeZone)

            : station(station)
            , stationType(stationType)
            , obstime(obstime)
            , origintime(origintime)
            , timeZone(timeZone)
        {
        }

        virtual ~Args() = default;

        const Fmi::Astronomy::solar_position_t& get_solar_position() const;
        const Fmi::Astronomy::solar_time_t& get_solar_time() const;
        const Fmi::Astronomy::lunar_time_t& get_lunar_time() const;

  private:
        mutable std::unique_ptr<Fmi::Astronomy::solar_position_t> solar_position;
        mutable std::unique_ptr<Fmi::Astronomy::solar_time_t> solar_time;
        mutable std::unique_ptr<Fmi::Astronomy::lunar_time_t> lunar_time;
  };

 public:
    SpecialParameters();

    virtual ~SpecialParameters() = default;

    Spine::TimeSeries::Value getValue(const std::string& param_name, const Args& args) const;

    Spine::TimeSeries::TimedValue getTimedValue(const std::string& param_name, const Args& args) const;

    Spine::TimeSeries::TimedValue getTimedValue(
        const Spine::Station &station,
        const std::string &stationType,
        const std::string &parameter,
        const boost::local_time::local_date_time &obstime,
        const boost::local_time::local_date_time &origintime,
        const std::string &timeZone) const;


 private:
    typedef std::function <Spine::TimeSeries::Value(const Args&)> parameter_handler_t;

    std::map<std::string, parameter_handler_t> handler_map;

    std::unique_ptr<Fmi::TimeFormatter> tf;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

