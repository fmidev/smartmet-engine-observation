#pragma once

#include "QueryResultBase.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/lexical_cast.hpp>
#include <macgyver/StringConversion.h>
#include <spine/Exception.h>
#include <algorithm>
#include <memory>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 *  @class QueryResult
 *  @brief The class implements a storege for series of values to store.
 *
 *         A value vector will allow values that are the same type as the first
 *         one in a valueVector. Between the value vectors, value type can vary.
 */
class QueryResult : public QueryResultBase
{
 public:
  typedef QueryResultBase::ValueType ValueType;
  typedef QueryResultBase::ValueVectorType ValueVectorType;

  // The constructor follow the guidelines of the base class.
  explicit QueryResult(const size_t& numberOfValueVectors);
  explicit QueryResult(const QueryResult& other);

  ~QueryResult();

  ValueVectorType::const_iterator begin(const std::string& valueVectorName);
  ValueVectorType::const_iterator end(const std::string& valueVectorName);

  /**
   * @brief Get number of items in a value vector.
   *        If the value vector not found for the name \a valueVectorName
   *        return value is zero.
   * @return Number of items in a value vector.
   */
  size_t size(const std::string& valueVectorName);

  /**
   * @brief Convert a referenced value to string.
   * @param value Referenced value.
   * @param precision Precision of a double or float value in a result.
   * @return referenced value as a string.
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *            if referenced value type conversion is not supported.
   */
  static std::string toString(const ValueVectorType::const_iterator value,
                              const uint32_t& precision = 0);

  template <typename T>
  static T castTo(const ValueVectorType::const_iterator value, const uint32_t& precision = 0)
  {
    typedef T OutType;

    try
    {
      if ((*value).type() == typeid(int32_t))
      {
        return boost::lexical_cast<OutType>(boost::any_cast<int32_t>(*value));
      }
      else if ((*value).type() == typeid(uint32_t))
      {
        return boost::lexical_cast<OutType>(boost::any_cast<uint32_t>(*value));
      }
      else if ((*value).type() == typeid(int64_t))
      {
        return boost::lexical_cast<OutType>(boost::any_cast<int64_t>(*value));
      }
      else if ((*value).type() == typeid(uint64_t))
      {
        return boost::lexical_cast<OutType>(boost::any_cast<uint64_t>(*value));
      }
      else if ((*value).type() == typeid(int16_t))
      {
        return boost::lexical_cast<OutType>(boost::any_cast<int16_t>(*value));
      }
      else if ((*value).type() == typeid(uint16_t))
      {
        return boost::lexical_cast<OutType>(boost::any_cast<uint16_t>(*value));
      }
      else if ((*value).type() == typeid(float))
      {
        std::ostringstream out;
        out << std::setprecision(precision) << std::fixed << boost::any_cast<float>(*value);
        return boost::lexical_cast<OutType>(out.str());
      }
      else if ((*value).type() == typeid(double))
      {
        std::ostringstream out;
        out << std::setprecision(precision) << std::fixed << boost::any_cast<double>(*value);
        return boost::lexical_cast<OutType>(out.str());
      }
      else if ((*value).type() == typeid(std::string))
      {
        return boost::lexical_cast<OutType>(boost::any_cast<std::string>(*value));
      }
      else if ((*value).type() == typeid(boost::posix_time::ptime))
      {
        return boost::lexical_cast<OutType>(
            Fmi::to_iso_extended_string(boost::any_cast<boost::posix_time::ptime>(*value)) + "Z");
      }
      else
      {
        std::ostringstream msg;
        msg << "QueryResult::toString : Unsupported data type '" << (*value).type().name() << "'.";

        Spine::Exception exception(BCP, "Operation processing failed!", nullptr);
        // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
        exception.addDetail(msg.str());
        throw exception;
      }
    }
    catch (const boost::bad_any_cast& e)
    {
      std::ostringstream msg;
      msg << "QueryResult::castTo : Bad any cast from '" << (*value).type().name() << "' type. "
          << e.what();

      Spine::Exception exception(BCP, "Operation processing failed!", nullptr);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }
    catch (const boost::bad_lexical_cast& e)
    {
      std::ostringstream msg;
      msg << "QueryResult::castTo : Bad cast from '" << (*value).type().name() << "' to '"
          << typeid(OutType).name() << "'. " << e.what();

      Spine::Exception exception(BCP, "Operation processing failed!", nullptr);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }
  }

  static std::pair<double, double> minMax(const ValueVectorType::const_iterator begin,
                                          const ValueVectorType::const_iterator end);

  // The method follow the guidelines of the base class.
  void getValueVectorData(const size_t& valueVectorId, ValueVectorType& outValueVector);

  void getValueVectorData(const std::string& valueVectorName, ValueVectorType& outValueVector);

  /**
   *  @brief Get a copy of the data of value vector as strings.
   *  @param[in] valueVectorId Identity of the value vector (Id range is [0,size()-1]).
   *  @param[out] outValueVector The method will delete the old data and then stores
   *              values from container to \c outValueVector.
   *  @exception Obs_EngineException::INVALID_PARAMETER_VALUE
   *             If the value of \c valueVectorId input variable is out of allowed range.
   *  @exception Obs_EngineException::OPERATION_PROSESSING_FAILED
   *             Lexical cast to std::string of a stored value failed.
   */
  void getValueVectorData(const size_t& valueVectorId, std::vector<std::string>& outValueVector);

  void getValueVectorData(const std::string& valueVectorName,
                          std::vector<std::string>& outValueVector);

  size_t getValueVectorId(const std::string& valueVectorName);

  std::string getValueVectorName(const size_t& valueVectorId);

  bool set(const std::shared_ptr<QueryResultBase> other);

  // The method follow the guidelines of the base class.
  void set(const size_t& valueVectorId, const ValueType& value);

  void setValueVectorName(const size_t& valueVectorId, const std::string& valueVectorName);

  /**
   *  @brief Get the number of value vectors in the container.
   *  @return The number of value vectors in the container.
   */
  size_t size() const;

 private:
  QueryResult();
  QueryResult& operator=(const QueryResult& other);

 private:
  // One value vector is a column.
  size_t m_numberOfValueVectors;

  std::vector<ValueVectorType> m_valueContainer;

  std::vector<std::string> m_valueVectorName;
  // Value vector data type tracking.
  std::vector<ValueType> m_valueTypeOfVector;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
