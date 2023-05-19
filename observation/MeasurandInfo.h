#pragma once

#include <map>
#include <set>
#include <boost/optional.hpp>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

struct measurand_text
{
  measurand_text() : measurand_name(""), measurand_desc(""), measurand_label("") {} 
  std::string measurand_name;
  std::string measurand_desc;
  std::string measurand_label;
};

struct measurand_info
{
  measurand_info() 
	: measurand_id(""),
	  measurand_code(""),
	  aggregate_period(""),
	  aggregate_function(""),
	  combined_code(""),
	  instant_value(true),
	  base_phenomenon(""),
	  measurand_period(""),
	  measurand_layer(""),
	  standard_level(boost::none),
	  measurand_unit("")
  {}
  measurand_info(const measurand_info& mi) = default;
  measurand_info& operator = (const measurand_info& mi) = default;

  std::string measurand_id; // Can be integer of string, e.g 137,'ILMA'
  std::string measurand_code;
  std::string aggregate_period;
  std::string aggregate_function;
  std::string combined_code;
  bool instant_value;
  std::map<std::string, measurand_text> translations; // language code -> texts
  std::string base_phenomenon;
  std::string measurand_period;
  std::string measurand_layer;
  boost::optional<double> standard_level;
  std::string measurand_unit;
  std::set<int> producers; // Valid producers for this parameter
  const std::string& get_name(const std::string& language_code) const;
  const std::string& get_description(const std::string& language_code) const;
  const std::string& get_label(const std::string& language_code) const;
};

// measurand_id -> info
using MeasurandInfo = std::map<std::string, measurand_info>;
  // producer -> MeasurandInfo
using ProducerMeasurandInfo = std::map<std::string, MeasurandInfo>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
