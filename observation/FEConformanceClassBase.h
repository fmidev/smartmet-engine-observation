#ifndef FE_CONFORMANCE_CLASS_BASE_H
#define FE_CONFORMANCE_CLASS_BASE_H

#include <boost/any.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <string>
#include <vector>

#include "Property.h"
#include <macgyver/StringConversion.h>
#include <spine/ConfigBase.h>
#include <spine/Exception.h>
#include <memory>

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
      function2<std::shared_ptr<const Property::Base>, const NameType&, const boost::any&>
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
      std::ostringstream msg;
      msg << METHOD_NAME << " : duplicate map key '" << name << "'.\n";
      std::cerr << msg.str();
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
    {
      std::ostringstream msg;
      msg << "Operation '" << name << "' not found.";
      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }
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
  ~FEConformanceClassBase();

  /**
   * @brief Get a new operation (filter) instance.
   * @param field A column name of an table (or view) to witch the the \a toWhat param value will be
   * compared.
   * @param operationName A operation (case insensitive) name to select the operation.
   * @param toWhat The value witch database values are compared.
   * @return Operation object or empty object (if no match with the \a operationName).
   */
  virtual std::shared_ptr<const PropertyIsBaseType> getNewOperationInstance(
      const std::string& field, const std::string& operationName, const boost::any& toWhat) = 0;

 private:
  FEConformanceClassBase(const FEConformanceClassBase& other);
  FEConformanceClassBase& operator=(const FEConformanceClassBase& other);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

#endif  // FE_CONFORMANCE_CLASS_BASE_H
