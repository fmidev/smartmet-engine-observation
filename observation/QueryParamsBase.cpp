#include "QueryParamsBase.h"
#include <sstream>
#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void QueryParamsBase::setTimeRange(const pt::ptime& beginTime, const pt::ptime& endTime)
{
  try
  {
    if (beginTime > endTime)
    {
      std::ostringstream msg;
      msg << "Invalid time interval " << pt::to_simple_string(beginTime) << " - "
          << pt::to_simple_string(endTime);

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }

    m_beginTime = beginTime;
    m_endTime = endTime;
    m_usingTimeRange = true;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void QueryParamsBase::setBoundingBox(const double& xMin,
                                     const double& yMin,
                                     const double& xMax,
                                     const double& yMax)
{
  try
  {
    std::ostringstream msg;
    if (xMin > xMax)
      msg << "xMin '" << xMin << "' is greater than xMax '" << xMax << "'";
    else if (yMin > yMax)
      msg << "yMin '" << yMin << "' is greater than yMAx '" << yMax << "'";
    else if (xMin < -180.0000)
      msg << "xMin '" << xMin << "' is less than -180.0";
    else if (xMax > 180.0000)
      msg << "xMax '" << xMax << "' is greater than 180.0";
    else if (yMin < -90.0000)
      msg << "yMin '" << yMin << "' is less than -90.0000";
    else if (yMax > 90.0000)
      msg << "yMax '" << yMax << "' is greater than 90.0000";

    if (msg.str().length() > 0)
    {
      std::ostringstream err;
      err << "Invalid bounding box "
          << " - " << msg;

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(err.str());
      throw exception;
    }

    m_bbox.xMin = xMin;
    m_bbox.yMin = yMin;
    m_bbox.xMax = xMax;
    m_bbox.yMax = yMax;
    m_bbox.crs = "EPSG:4236";
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

std::string QueryParamsBase::formattedTime(const pt::ptime& t, const std::string& format) const
{
  try
  {
    std::string fT;
    if (format == "YYYY-MM-DD HH24:MI:SS")
    {
      fT = pt::to_simple_string(t);
      if ((fT != "not-a-date-time") and (fT != "+infinity") and (fT != "-infinity") and (fT != ""))
        return fT;
    }

    std::ostringstream msg;
    msg << "Time format conversion failure"
        << " - '" << fT << "'.";

    SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
    // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
    exception.addDetail(msg.str());
    throw exception;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
