#pragma once

#include <boost/any.hpp>
#include <memory>
#include <stdexcept>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 *  @class QueryResultBase
 *  @brief The class defines a simple container interface to store boost::any values.
 *
 *         The class is constructed to be a base (or interface) class of an inherent class,
 *         that implements a container. Initially the class is designed to store
 *         any (two dimensional or column like) data returned from database. Returned
 *         data column is called as the \c valueVector and identified with an id number
 *         called as \c valueVectorId. By default all data values in a \c valueVector must
 *         be at same data type. If inherent class makes an exception from that rule
 *         it is good practice to enclose varying data types in an object.
 */
class QueryResultBase
{
 public:
  using ValueType = boost::any;
  using ValueVectorType = std::vector<ValueType>;

  /**
   *  @brief The class onstructor with input argument that defines the container size.
   *  @param[in] numberOfValueVectors The input parameter defines how many value vectors
   *             will be stored in the container.
   */
#ifdef __llvm__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#endif
  explicit QueryResultBase(const size_t& numberOfValueVectors) {}
  explicit QueryResultBase(const QueryResultBase& other) = default;
  virtual ~QueryResultBase();
#ifdef __llvm__
#pragma clang diagnostic pop
#endif

  QueryResultBase& operator=(const QueryResultBase& other) = delete;

  /**
   *  @brief Get a copy of the value vector data ordered by \c valueVectorId id.
   *  @param[in] valueVectorId Identity of the value vector (Id range is [0,size()-1]).
   *  @param[out] outValueVector The method will delete the old data and then stores
   *              the values from the value vector with \c valueVectorId id.
   *  @exception Obs_EngineException::INVALID_PARAMETER_VALUE
   *             If the value of \c valueVectorId input variable is out of allowed range.
   */
  virtual void getValueVectorData(const size_t& valueVectorId, ValueVectorType& outValueVector) = 0;

  virtual void getValueVectorData(const std::string& valueVectorName,
                                  ValueVectorType& outValueVector) = 0;

  /**
   *  @brief Get the name for a \c valueVectorId.
   *  @param[in] valueVectorId Identity of the value vector (Id range is [0,size()-1]).
   *  @return The variable name of a column in DB response.
   *  @exception Obs_EngineException::INVALID_PARAMETER_VALUE
   *             If the value of \c valueVectorId input variable is out of allowed range.
   */
  virtual std::string getValueVectorName(const size_t& valueVectorId) = 0;

  virtual bool set(const std::shared_ptr<QueryResultBase> result) = 0;

  /**
   *  @brief Set a value in a value vector.
   *  @param[in] valueVectorId Identity of the value vector (Id range is [0,size()-1]).
   *  @param[in] value The value to store.
   *  @exception Obs_EngineException::INVALID_PARAMETER_VALUE
   *             If the value of \c valueVectorId input variable is out of allowed range or
   *             the value of \c value has wrong data type.
   *            (In a value vector only values with same data type is allowed.)
   */
  virtual void set(const size_t& valueVectorId, const ValueType& value) = 0;

  /**
   *  @brief Set the name for a \c valueVectorId.
   *  If the name is given for an id of value vector, user can check
   *  that the value vector contain the data requested.
   *  @param[in] valueVectorId Identity of the value vector (Id range is [0,size()-1]).
   *  @param[in] valueVectorName The variable name of a column in DB response.
   *  @exception Obs_EngineException::INVALID_PARAMETER_VALUE
   *             If the value of \c valueVectorId input variable is out of allowed range.
   */
  virtual void setValueVectorName(const size_t& valueVectorId,
                                  const std::string& valueVectorName) = 0;

  /**
   *  @biref Get the number of value vectors in the container.
   *  @return Number of value vectors.
   */
  virtual size_t size() const = 0;

 protected:
  QueryResultBase() = default;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
