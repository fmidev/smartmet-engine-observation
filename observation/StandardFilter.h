#ifndef STANDARD_FILTER_H
#define STANDARD_FILTER_H

#include "MinimumStandardFilter.h"
#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 * Standard filter operations of FES.
 *
 * The class extends the minimum standard filters of FES.
 * The following filter operations are implemented:
 * - PropertyIsNull
 * - PropertyIsNotNull
 * - PropertyIsNil
 * - PropertyIsLike
 * - PropertyIsBetween
 */
class StandardFilter : public MinimumStandardFilter
{
 public:
  typedef FEConformanceClassBase::NameType NameType;
  typedef FEConformanceClassBase::PropertyIsBaseType PropertyIsBaseType;

  /**
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED StandardFilter initialization
   * failed.
   */
  explicit StandardFilter();

  virtual std::shared_ptr<const PropertyIsBaseType> getNewOperationInstance(
      const NameType& field, const NameType& operationName, const boost::any& toWhat);

 private:
  StandardFilter(const StandardFilter& other);
  StandardFilter operator=(const StandardFilter& other);
};

/**
 * Standard filter operations of FES and extra non standard operations.
 *
 * The class extends the standard filter operations of FES.
 * The followind filter operations are implemented:
 * - PropertyMinuteValueModuloIsEqualToZero (Can be used as timestep to select certain value on
 * time.)
 */

class ExtendedStandardFilter : public StandardFilter
{
 public:
  typedef FEConformanceClassBase::NameType NameType;
  typedef FEConformanceClassBase::PropertyIsBaseType PropertyIsBaseType;

  explicit ExtendedStandardFilter() : StandardFilter()
  {
    // Extension operations to StandardFilter.
    FEConformanceClassBase::add("PropertyMinuteValueModuloIsEqualToZero",
                                Property::MinuteValueModuloIsEqualToZero());
  }

  std::shared_ptr<const PropertyIsBaseType> getNewOperationInstance(const NameType& field,
                                                                    const NameType& operationName,
                                                                    const boost::any& toWhat)
  {
    try
    {
      OperationMapValueType op = FEConformanceClassBase::get(operationName);
      return std::shared_ptr<const PropertyIsBaseType>(op(field, toWhat));
    }
    catch (...)
    {
      std::ostringstream msg;
      msg << "ExtendedStandardFilter operation '" << operationName << "' initialization failed!";

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!", NULL);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }
  }

 private:
  ExtendedStandardFilter(const ExtendedStandardFilter& other);
  ExtendedStandardFilter operator=(const ExtendedStandardFilter& other);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

#endif  // STANDARD_FILTER_H
