#pragma once

#include "Property.h"
#include <boost/bind/bind.hpp>
#include <boost/function.hpp>
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <spine/ConfigBase.h>
#include <any>
#include <iostream>
#include <map>
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
  using NameType = std::string;
  using IdType = int;
  using OperationMapValueType =
      boost::function2<std::shared_ptr<const Property::Base>, const NameType&, const std::any&>;
  using OperationMapType = std::map<NameType, OperationMapValueType>;

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
    const auto it = m_ops.find(n);
    if (it != m_ops.end())
    {
      std::cerr << fmt::format("{} : duplicate map key '{}'.\n", METHOD_NAME, name);
      return false;
    }
    OperationMapValueType value =
        boost::bind(&T::get, opClass, boost::placeholders::_1, boost::placeholders::_2);
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
    const auto it = m_ops.find(n);

    if (it == m_ops.end())
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("Operation '{}' not found.", name));

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
  using NameType = std::string;
  using PropertyIsBaseType = Property::Base;

  FEConformanceClassBase();
  virtual ~FEConformanceClassBase();

  FEConformanceClassBase(const FEConformanceClassBase& other) = delete;
  FEConformanceClassBase& operator=(const FEConformanceClassBase& other) = delete;

  /**
   * @brief Get a new operation (filter) instance.
   * @param field A column name of an table (or view) to witch the the \a toWhat param value will be
   * compared.
   * @param operationName A operation (case insensitive) name to select the operation.
   * @param toWhat The value witch database values are compared.
   * @return Operation object or empty object (if no match with the \a operationName).
   */
  virtual std::shared_ptr<const PropertyIsBaseType> getNewOperationInstance(
      const std::string& field, const std::string& operationName, const std::any& toWhat) = 0;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
