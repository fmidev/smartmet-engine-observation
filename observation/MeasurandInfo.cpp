#include "MeasurandInfo.h"
#include <macgyver/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
static std::string EMPTY_STRING;

const std::string& measurand_info::get_name(const std::string& language_code) const
{
  try
  {
    if (translations.find(language_code) != translations.end())
      return translations.at(language_code).measurand_name;

    return EMPTY_STRING;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed");
  }
}

const std::string& measurand_info::get_description(const std::string& language_code) const
{
  try
  {
    if (translations.find(language_code) != translations.end())
      return translations.at(language_code).measurand_desc;

    return EMPTY_STRING;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed");
  }
}

const std::string& measurand_info::get_label(const std::string& language_code) const
{
  try
  {
    if (translations.find(language_code) != translations.end())
      return translations.at(language_code).measurand_label;

    return EMPTY_STRING;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
