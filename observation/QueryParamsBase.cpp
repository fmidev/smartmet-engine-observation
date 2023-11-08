#include "QueryParamsBase.h"
#include <boost/date_time/gregorian/formatters.hpp>
#include <macgyver/DateTime.h>
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
std::string formattedTime(const Fmi::DateTime& t, const std::string& format)
{
  try
  {
    std::string fT;
    if (format == "YYYY-MM-DD HH24:MI:SS")
    {
      fT = Fmi::to_simple_string(t);
      if ((fT != "not-a-date-time") && (fT != "+infinity") && (fT != "-infinity") && (!fT.empty()))
        return fT;
    }

    throw Fmi::Exception(BCP, "Operation processing failed!")
        .addDetail(fmt::format("Time format conversion failure - '{}'", fT));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}
}  // namespace

QueryParamsBase::~QueryParamsBase() = default;

std::string QueryParamsBase::getBeginTime(const std::string& format) const
{
  try
  {
    if (m_usingTimeRange)
      return formattedTime(m_beginTime, format);

    return "";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string QueryParamsBase::getEndTime(const std::string& format) const
{
  try
  {
    if (m_usingTimeRange)
      return formattedTime(m_endTime, format);
    return "";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void QueryParamsBase::setTimeRange(const Fmi::DateTime& beginTime, const Fmi::DateTime& endTime)
{
  try
  {
    if (beginTime > endTime)
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("Invalid time interval {} - {}",
                                 Fmi::to_simple_string(beginTime),
                                 Fmi::to_simple_string(endTime)));

    m_beginTime = beginTime;
    m_endTime = endTime;
    m_usingTimeRange = true;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void QueryParamsBase::setBoundingBox(const double& xMin,
                                     const double& yMin,
                                     const double& xMax,
                                     const double& yMax)
{
  try
  {
    std::string msg;
    if (xMin > xMax)
      msg = fmt::format("xMin '{}' is greater than xMax '{}'", xMin, xMax);
    else if (yMin > yMax)
      msg = fmt::format("yMin '{}' is greater than yMax '{}'", yMin, yMax);
    else if (xMin < -180)
      msg = fmt::format("xMin '{}' is less than -180.0", xMin);
    else if (xMax > 180.0000)
      msg = fmt::format("xMax '{}' is greater than 180.0", xMax);
    else if (yMin < -90.0000)
      msg = fmt::format("yMin '{}' is less than -90.0000", yMin);
    else if (yMax > 90.0000)
      msg = fmt::format("yMax '{}' is greater than 90.0000", yMax);

    if (!msg.empty())
      throw Fmi::Exception(BCP, "Invalid bounding box!").addDetail(msg);

    m_bbox.xMin = xMin;
    m_bbox.yMin = yMin;
    m_bbox.xMax = xMax;
    m_bbox.yMax = yMax;
    m_bbox.crs = "EPSG:4236";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
