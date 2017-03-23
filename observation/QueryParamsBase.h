#ifndef QUERY_PARAMS_BASE_H
#define QUERY_PARAMS_BASE_H

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <spine/Value.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace pt = boost::posix_time;

class QueryParamsBase
{
 public:
  explicit QueryParamsBase() : m_usingTimeRange(false) {}
  virtual ~QueryParamsBase() {}
  /**
   *  \brief Get formatted time string.
   *
   *  Supported formats:
   *  - 'YYYY-MM-DD HH24:MI:SS' (default), output e.g. '2012-Feb-01 17:18:19'
   *
   *  \param format Output time format e.g..
   *  \return formatted time string or empty string if not set.
   *  \exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *             if format conversion fail.
   */
  std::string getBeginTime(const std::string& format = "YYYY-MM-DD HH24:MI:SS") const;

  std::string getEndTime(const std::string& format = "YYYY-MM-DD HH24:MI:SS") const;

  /**
   *  \brief Set bounding box by using epsg projection 4326 - wgs84
   *
   *  Projected Bounds: -180.0000, -90.0000, 180.0000, 90.0000 (xMin, yMin, xMax, yMax)
   *
   *  \param xMin Smallest longitude coordinate of the bounding box.
   *  \param yMin Smallest latitude coordinate of the bounding box.
   *  \param xMax Biggest longitude coordinate of the bounding box.
   *  \param yMax Biggest latitude coordinate of the bounding box.
   *  \exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *             if invalid bounding box value is found.
   */
  void setBoundingBox(const double& xMin,
                      const double& yMin,
                      const double& xMax,
                      const double& yMax);

  /**
   *  \brief Set time range.
   *  \exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *             if time range is invalid (e.g beginTime > endTime).
   */
  void setTimeRange(const pt::ptime& beginTime, const pt::ptime& endTime);

 private:
  QueryParamsBase& operator=(const QueryParamsBase& other);
  QueryParamsBase(const QueryParamsBase& other);
  std::string formattedTime(const pt::ptime& t, const std::string& format) const;

 private:
  pt::ptime m_beginTime;
  pt::ptime m_endTime;
  bool m_usingTimeRange;
  struct SmartMet::Spine::BoundingBox m_bbox;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

#endif  // QUERY_PARAMS_BASE_H
