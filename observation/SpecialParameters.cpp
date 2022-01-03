#include "SpecialParameters.h"
#include <macgyver/Astronomy.h>
#include <macgyver/CharsetTools.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <spine/ParameterKeywords.h>
#include <spine/ParameterTools.h>
#include <spine/ValueFormatter.h>
#include <locus/Query.h>
#include <engines/geonames/Engine.h>

using namespace SmartMet;
using SmartMet::Engine::Observation::SpecialParameters;

SpecialParameters& SpecialParameters::mutable_instance()
{
    static SpecialParameters special_parameters;
    return special_parameters;
}

void SpecialParameters::setGeonames(SmartMet::Engine::Geonames::Engine* itsGeonames)
{
     mutable_instance().itsGeonames = itsGeonames;
}

Spine::TimeSeries::Value
SpecialParameters::getValue(
    const std::string& param_name,
    const SpecialParameters::Args& args) const
{
    assert(itsGeonames);
    try {
        auto it = handler_map.find(param_name);
        if (it == handler_map.end() || !it->second) {
            std::string msg = "Unsupported special parameter '" + param_name + "'";
            Fmi::Exception exception(BCP, "Operation processing failed!");
            exception.addDetail(msg);
            throw exception;
        } else {
            return it->second(args);
        }
    } catch (...) {
        throw Fmi::Exception::Trace(BCP, "Operation failed!");
    }
}

Spine::TimeSeries::TimedValue
SpecialParameters::getTimedValue(
    const std::string& param_name,
    const Args& args) const
{
    try {
        Spine::TimeSeries::Value value = getValue(param_name, args);
        return Spine::TimeSeries::TimedValue(args.obstime, value);
    } catch (...) {
            throw Fmi::Exception::Trace(BCP, "Operation failed!");
    }
}

Spine::TimeSeries::TimedValue
SpecialParameters::getTimedValue(
    const Spine::Station &station,
    const std::string &stationType,
    const std::string &parameter,
    const boost::local_time::local_date_time &obstime,
    const boost::local_time::local_date_time &origintime,
    const std::string &timeZone,
    const Settings* settings) const
{
  try
  {
      const SpecialParameters::Args args(station, stationType, obstime, origintime, timeZone, settings);
      Spine::TimeSeries::Value value = getValue(parameter, args);
      return Spine::TimeSeries::TimedValue(obstime, value);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

namespace {

  std::string format_date(const boost::local_time::local_date_time& ldt,
			  const std::locale& llocale,
			  const std::string& fmt)
  {
    try
      {
	using tfacet = boost::date_time::time_facet<boost::local_time::local_date_time, char>;
	std::ostringstream os;
	os.imbue(std::locale(llocale, new tfacet(fmt.c_str())));
	    os << ldt;
	    return Fmi::latin1_to_utf8(os.str());
      }
    catch (...)
      {
	throw Fmi::Exception::Trace(BCP, "Operation failed!");
	  }
  };

}

SpecialParameters::SpecialParameters()
    : tf(Fmi::TimeFormatter::create("iso"))
{
    // FIXME: is locale dependent (this dependency does not seem to work)
    handler_map[COUNTRY_PARAM] =
        [this](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            auto loc = d.get_location(itsGeonames);
            return loc ? loc->country : d.station.country;
        };

    handler_map[DARK_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Fmi::Astronomy::solar_position_t sp =
                Fmi::Astronomy::solar_position(d.obstime, d.station.longitude_out, d.station.latitude_out);
            return sp.dark();
        };

    handler_map[DAYLENGTH_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Fmi::Astronomy::solar_time_t st =
                Fmi::Astronomy::solar_time(d.obstime, d.station.longitude_out, d.station.latitude_out);
            auto seconds = st.daylength().total_seconds();
            int minutes = boost::numeric_cast<int>(round(static_cast<double>(seconds) / 60.0));
            return minutes;
        };

    // FIXME: läsnä ParameterKeywords.h. Ei kuitenkaan toteutusta tähän asti
    handler_map[DEM_PARAM] = parameter_handler_t();

    handler_map[DIRECTION_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Spine::TimeSeries::Value value = Spine::TimeSeries::None();
            if (d.station.stationDirection >= 0)
            {
                Spine::ValueFormatterParam vfp;
                Spine::ValueFormatter valueFormatter(vfp);
                value = valueFormatter.format(d.station.stationDirection, 1);
            }
            return value;
        };

    // FIXME: miksi Station::distance on std::string?
    handler_map[DISTANCE_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Spine::TimeSeries::Value value = Spine::TimeSeries::None();
            if (!d.station.distance.empty())
            {
                Spine::ValueFormatterParam vfp;
                Spine::ValueFormatter valueFormatter(vfp);
                value = valueFormatter.format(Fmi::stod(d.station.distance), 1);
            }
            return d.station.distance;
        };

    handler_map[ELEVATION_PARAM] =
    handler_map[STATION_ELEVATION_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.station_elevation;
        };

    // FIXME: extra .0 added by formatter
    handler_map[EPOCHTIME_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            boost::posix_time::ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));
            boost::posix_time::time_duration diff = d.obstime.utc_time() - time_t_epoch;
            return int(diff.total_seconds());
        };

    // FIXME: on Location::feature mutta ei kuitenkaan sopiva arvo täällä saattavissa
    handler_map[FEATURE_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return Spine::TimeSeries::None();
        };

    // FIXME: läsnä ParameterKeywords.h. Ei kuitenkaan toteutusta tähän asti
    handler_map[FLASH_PRODUCER] = parameter_handler_t();

    handler_map[FMISID_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Spine::TimeSeries::Value value = Spine::TimeSeries::None();
            if (d.station.fmisid > 0) {
                value = d.station.fmisid;
            }
            return value;
        };

    handler_map[GEOID_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.geoid;
        };

    handler_map[HOUR_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return int(d.obstime.local_time().time_of_day().hours());
        };

    // FIXME: iso2 seems to be empty in d.station.iso2
    handler_map[ISO2_PARAM] =
        [this](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            auto loc = d.get_location(itsGeonames);
            return loc ? loc->iso2 : d.station.iso2;
        };

    handler_map[ISOTIME_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return Fmi::to_iso_string(d.obstime.local_time());
        };


    handler_map[LATITUDE_PARAM] =
    handler_map[LAT_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.latitude_out;
        };

    // FIXME: how to handle accurracy
    handler_map[LATLON_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Spine::ValueFormatterParam vfp;
            Spine::ValueFormatter valueformatter(vfp);
            return (valueformatter.format(d.station.latitude_out, 5) + ", " +
                valueformatter.format(d.station.longitude_out, 5));
        };

    // FIXME: ei toteutusta aikaisemmin. On määrrätty ParameterKeywords.h
    //        on toteutus smartmet-library-delfoi
    handler_map[LEVEL_PARAM] = parameter_handler_t();

    handler_map[LOCALTIME_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.obstime;
        };

    handler_map[LOCALTZ_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.timezone;
        };

    handler_map[LONGITUDE_PARAM] =
    handler_map[LON_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.longitude_out;
        };

    handler_map[LONLAT_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
        Spine::ValueFormatterParam vfp;
        Spine::ValueFormatter valueformatter(vfp);
            return valueformatter.format(d.station.longitude_out, 5) + ", " +
                valueformatter.format(d.station.latitude_out, 5);
        };

    handler_map[LPNN_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Spine::TimeSeries::Value value = Spine::TimeSeries::None();
            if (d.station.lpnn > 0) {
                value = d.station.lpnn;
            }
            return value;
        };

    handler_map[MODEL_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.stationType;
        };

    // modtime is only for timeseries compatibility
    handler_map["modtime"] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return "";
        };

    handler_map[MONTH_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
	    return format_date(d.obstime, d.settings->locale, "%B");
        };

    handler_map[MON_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
	    return format_date(d.obstime, d.settings->locale, "%b");
        };

    handler_map[MOONPHASE_PARAM] =
        [] (const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return Fmi::Astronomy::moonphase(d.obstime.utc_time());
        };

    handler_map[MOONRISETODAY_PARAM] =
        [] (const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_lunar_time().moonrise_today();
        };

    handler_map[MOONRISE_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_lunar_time().moonrise;
        };

    handler_map[MOONSET2TODAY_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_lunar_time().moonset2_today();
        };

    handler_map[MOONSET2_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_lunar_time().moonset2;
        };

    handler_map[MOONSETTODAY_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_lunar_time().moonset_today();
        };

    handler_map[MOONSET_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_lunar_time().moonset;
        };

    handler_map[MOONUP24H_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_lunar_time().above_horizont_24h();
        };

    handler_map[NAME_PARAM] =
        [this](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            auto loc = d.get_location(itsGeonames);
            if (loc) {
	      return loc->name;
	    } else {
	      if (d.station.requestedName.length() > 0)
                return d.station.requestedName;
	      else
                return d.station.station_formal_name;
	    }
	};

    handler_map[NEARLATLON_PARAM] = parameter_handler_t();

    handler_map[NEARLONLAT_PARAM] = parameter_handler_t();

    // FIXME: tarvitsemmeko formatoida täällä?
    handler_map[NOON_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Fmi::Astronomy::solar_time_t st =
                Fmi::Astronomy::solar_time(d.obstime, d.station.longitude_out, d.station.latitude_out);
            return st.noon;
        };

    handler_map[ORIGINTIME_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.origintime;
        };

    handler_map[PLACE_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.tag;
        };

    handler_map[POPULATION_PARAM] = parameter_handler_t();

    handler_map[PRODUCER_PARAM] = parameter_handler_t();

    handler_map[REGION_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.region;
        };

    handler_map[RWSID_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Spine::TimeSeries::Value value = Spine::TimeSeries::None();
            if (d.station.rwsid > 0) {
                value = d.station.rwsid;
            }
            return value;
        };

    // FIXME: is this correct?
    handler_map[SENSOR_NO_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return 1;
        };

    // FIXME: Station::stationary on std::string. Pitäisikö olla bool vai int?
    handler_map[STATIONARY_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.stationary;
        };

    handler_map[STATIONLATITUDE_PARAM] =
    handler_map[STATIONLAT_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.latitude_out;
        };

    handler_map[STATIONLONGITUDE_PARAM] =
    handler_map[STATIONLON_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.longitude_out;
        };

    handler_map[STATIONNAME_PARAM] =
    handler_map[STATION_NAME_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.station.station_formal_name;;
        };

    handler_map[STATIONTYPE_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.stationType;
        };

    handler_map[SUNAZIMUTH_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_solar_position().azimuth;
        };

    handler_map[SUNDECLINATION_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_solar_position().declination;
        };

    handler_map[SUNELEVATION_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_solar_position().elevation;
        };

    handler_map[SUNRISETODAY_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_solar_time().sunrise_today();
        };

    handler_map[SUNRISE_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_solar_time().sunrise;
        };

    handler_map[SUNSETTODAY_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_solar_time().sunset_today();
        };

    handler_map[SUNSET_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.get_solar_time().sunset;
        };

    handler_map[SYKE_PRODUCER] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return int(d.station.isSYKEStation);
        };

    // FIXME: Spine::ParameterTools contains TZ conversion. Do we need it here?
    handler_map[TIME_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return d.obstime;
        };

    // FIXME: implement
    handler_map[TIMESTRING_PARAM] =
        //handler_map[TIMESTRING_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            //return format_date(ldt, outlocale, timestring);
            return "";
        };

    handler_map[TZ_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            if (d.timeZone == "localtime")
                return d.station.timezone;
            else
                return d.timeZone;
        };

    handler_map[UTCTIME_PARAM] =
        [this](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return tf->format(d.obstime.utc_time());
        };


    handler_map[UTC_PARAM] =
        [this](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return tf->format(d.obstime.utc_time());
        };

    handler_map[WDAY_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
	    return format_date(d.obstime, d.settings->locale, "%a");
        };

    handler_map[WEEKDAY_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
	    return format_date(d.obstime, d.settings->locale, "%A");
        };

    handler_map[WMO_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            Spine::TimeSeries::Value value = Spine::TimeSeries::None();
            if (d.station.wmo > 0)
                value = d.station.wmo;
            return value;
        };

    handler_map[XMLTIME_PARAM] =
        [](const SpecialParameters::Args& d) -> Spine::TimeSeries::Value
        {
            return Fmi::to_iso_extended_string(d.obstime.local_time());
        };

    // FIXME: implement: requires initial coordinate system
    handler_map[X_PARAM] = parameter_handler_t();

    // FIXME: implement: requires initial coordinate system
    handler_map[Y_PARAM] = parameter_handler_t();
}

const Fmi::Astronomy::solar_position_t&
SpecialParameters::Args::get_solar_position() const
{
    if (!solar_position) {
        solar_position.reset(
            new Fmi::Astronomy::solar_position_t(
                Fmi::Astronomy::solar_position(obstime, station.longitude_out, station.latitude_out)));
    }
    return *solar_position;
}

const Fmi::Astronomy::solar_time_t&
SpecialParameters::Args::get_solar_time() const
{
    if (!solar_time) {
        solar_time.reset(
            new Fmi::Astronomy::solar_time_t(
                Fmi::Astronomy::solar_time(obstime, station.longitude_out, station.latitude_out)));
    }
    return *solar_time;
}

const Fmi::Astronomy::lunar_time_t&
SpecialParameters::Args::get_lunar_time() const
{
    if (!lunar_time) {
        lunar_time.reset(
            new Fmi::Astronomy::lunar_time_t(
                Fmi::Astronomy::lunar_time(obstime, station.longitude_out, station.latitude_out)));
    }
    return *lunar_time;
}

SmartMet::Spine::LocationPtr SpecialParameters::Args::get_location(Geonames::Engine* itsGeonames) const
{
    SmartMet::Spine::LocationPtr ptr;
    if (!location_ptr && settings && itsGeonames) {
        Locus::QueryOptions opts;
	opts.SetLanguage(settings->language);
	opts.SetResultLimit(1);
	opts.SetCountries("");
	opts.SetSearchVariants(true);
	auto places = itsGeonames->idSearch(opts, station.geoid);
	if (!places.empty()) {
            ptr = *places.begin();
	}
        location_ptr = ptr;
    } else if (location_ptr) {
        ptr = *location_ptr;
    }
    return ptr;
}
