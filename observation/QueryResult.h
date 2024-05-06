#pragma once

#include "QueryResultBase.h"
#include <macgyver/DateTime.h>
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <algorithm>
#include <memory>
#include <vector>
#include <boost/lexical_cast.hpp>

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
  using ValueType = QueryResultBase::ValueType;
  using ValueVectorType = QueryResultBase::ValueVectorType;

  // The constructor follow the guidelines of the base class.
  explicit QueryResult(const size_t& numberOfValueVectors);
  explicit QueryResult(const QueryResult& other);

  ~QueryResult() override;

  QueryResult() = delete;
  QueryResult(QueryResult&& other) = delete;
  QueryResult& operator=(const QueryResult& other) = delete;
  QueryResult& operator=(QueryResult& other) = delete;

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
                              std::uint32_t precision = 0);

  template <typename T>
  static T castTo(const ValueVectorType::const_iterator& value)
  {
    try
    {
      if (value->type() == typeid(int32_t))
        return static_cast<T>(boost::any_cast<int32_t>(*value));
      if (value->type() == typeid(uint32_t))
        return static_cast<T>(boost::any_cast<uint32_t>(*value));
      if (value->type() == typeid(int64_t))
        return static_cast<T>(boost::any_cast<int64_t>(*value));
      if (value->type() == typeid(uint64_t))
        return static_cast<T>(boost::any_cast<uint64_t>(*value));
      if (value->type() == typeid(int16_t))
        return static_cast<T>(boost::any_cast<int16_t>(*value));
      if (value->type() == typeid(uint16_t))
        return static_cast<T>(boost::any_cast<uint16_t>(*value));
      if (value->type() == typeid(float))
        return static_cast<T>(boost::any_cast<float>(*value));
      if (value->type() == typeid(double))
        return static_cast<T>(boost::any_cast<double>(*value));

      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("QueryResult::toString : Unsupported data type '{}'.",
                                 value->type().name()));
    }
    catch (const boost::bad_any_cast& e)
    {
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("QueryResult::castTo : Bad any cast from '{}' type. {}",
                                 value->type().name(),
                                 e.what()));
    }
    catch (const boost::bad_lexical_cast& e)
    {
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("QueryResult::castTo : Bad cast from '{}' to '{}'. {}",
                                 value->type().name(),
                                 typeid(T).name(),
                                 e.what()));
    }
  }

  static std::pair<double, double> minMax(const ValueVectorType::const_iterator& begin,
                                          const ValueVectorType::const_iterator& end);

  // The method follow the guidelines of the base class.
  void getValueVectorData(const size_t& valueVectorId, ValueVectorType& outValueVector) override;

  void getValueVectorData(const std::string& valueVectorName,
                          ValueVectorType& outValueVector) override;

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

  std::string getValueVectorName(const size_t& valueVectorId) override;

  bool set(const std::shared_ptr<QueryResultBase>& other) override;

  // The method follow the guidelines of the base class.
  void set(const size_t& valueVectorId, const ValueType& value) override;

  void setValueVectorName(const size_t& valueVectorId, const std::string& valueVectorName) override;

  /**
   *  @brief Get the number of value vectors in the container.
   *  @return The number of value vectors in the container.
   */
  size_t size() const override;

 private:
  // One value vector is a column.
  size_t m_numberOfValueVectors;

  std::vector<ValueVectorType> m_valueContainer;

  std::vector<std::string> m_valueVectorName;
  // Value vector data type tracking.
  std::vector<ValueType> m_valueTypeOfVector;
};

template <>
Fmi::DateTime QueryResult::castTo(
    const QueryResult::ValueVectorType::const_iterator& value);

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
