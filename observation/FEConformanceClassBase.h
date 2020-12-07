#pragma once

#include "Property.h"
#include <boost/any.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <spine/ConfigBase.h>
#include <memory>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 * Property::Base operation object storege.
 */
class OperationMap
{
 public:
  typedef std::string NameType;
  typedef int IdType;
  typedef boost::
      function2<boost::shared_ptr<const Property::Base>, const NameType&, const boost::any&>
          OperationMapValueType;
  typedef std::map<NameType, OperationMapValueType> OperationMapType;

  /**
   * @brief Add an operation.
   * @param name The operation (key) name.
   * @param opClass The operation class object (witch base class is Property::Base).
   * @retval true The operation added succesfully.
   * @retval false The operation already added.
   */
  template <typename T>
  bool add(const std::string& name, const T& opClass)
  {
    const NameType n = Fmi::ascii_toupper_copy(name);
    OperationMapType::const_iterator it = m_ops.find(n);
    if (it != m_ops.end())
    {
      std::cerr << fmt::format("{} : duplicate map key '{}'.\n", METHOD_NAME, name);
      return false;
    }
    OperationMapValueType value = boost::bind(&T::get, opClass, ::_1, ::_2);
    m_ops.insert(std::make_pair(n, value));
    return true;
  }

  /**
   * @brief Get operation by using an operation name.
   * @param name The name of an operation to look for.
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED If the operation for the given
   * value
   * not found.
   */
  OperationMapValueType get(const std::string& name)
  {
    const NameType n = Fmi::ascii_toupper_copy(name);
    OperationMapType::const_iterator it = m_ops.find(n);

    if (it == m_ops.end())
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("Operation '{}' not found.", name));
    else
      return it->second;
  }

 private:
  OperationMapType m_ops;
};

/**
 * Base class of the filter classes.
 */
class FEConformanceClassBase : public OperationMap
{
 public:
  typedef std::string NameType;
  typedef Property::Base PropertyIsBaseType;

  FEConformanceClassBase();
  virtual ~FEConformanceClassBase();

  /**
   * @brief Get a new operation (filter) instance.
   * @param field A column name of an table (or view) to witch the the \a toWhat param value will be
   * compared.
   * @param operationName A operation (case insensitive) name to select the operation.
   * @param toWhat The value witch database values are compared.
   * @return Operation object or empty object (if no match with the \a operationName).
   */
  virtual boost::shared_ptr<const PropertyIsBaseType> getNewOperationInstance(
      const std::string& field, const std::string& operationName, const boost::any& toWhat) = 0;

 private:
  FEConformanceClassBase(const FEConformanceClassBase& other);
  FEConformanceClassBase& operator=(const FEConformanceClassBase& other);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
