#pragma once

#include "MinimumStandardFilter.h"
#include <macgyver/Exception.h>

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
  using NameType = FEConformanceClassBase::NameType;
  using PropertyIsBaseType = FEConformanceClassBase::PropertyIsBaseType;

  /**
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED StandardFilter initialization
   * failed.
   */
  explicit StandardFilter();
  ~StandardFilter();

  StandardFilter(const StandardFilter& other) = delete;
  StandardFilter operator=(const StandardFilter& other) = delete;

  virtual std::shared_ptr<const PropertyIsBaseType> getNewOperationInstance(
      const NameType& field, const NameType& operationName, const boost::any& toWhat);
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
  using NameType = FEConformanceClassBase::NameType;
  using PropertyIsBaseType = FEConformanceClassBase::PropertyIsBaseType;

  ~ExtendedStandardFilter();

  ExtendedStandardFilter(const ExtendedStandardFilter& other) = delete;
  ExtendedStandardFilter operator=(const ExtendedStandardFilter& other) = delete;

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
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(fmt::format("ExtendedStandardFilter operation '{}' initialization failed!",
                                 operationName));
    }
  }
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
