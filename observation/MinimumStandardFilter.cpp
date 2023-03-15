#include "MinimumStandardFilter.h"
#include <macgyver/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
MinimumStandardFilter::MinimumStandardFilter()
{
  try
  {
    try
    {
      FEConformanceClassBase::add("PropertyIsEqualTo", Property::IsEqualTo());
      FEConformanceClassBase::add("PropertyIsNotEqualTo", Property::IsNotEqualTo());
      FEConformanceClassBase::add("PropertyIsLessThan", Property::IsLessThan());
      FEConformanceClassBase::add("PropertyIsLessThanOrEqualTo", Property::IsLessThanOrEqualTo());
      FEConformanceClassBase::add("PropertyIsGreaterThan", Property::IsGreaterThan());
      FEConformanceClassBase::add("PropertyIsGreaterThanOrEqualTo",
                                  Property::IsGreaterThanOrEqualTo());
    }
    catch (...)
    {
      Fmi::Exception exception(BCP, "Operation processing failed!", nullptr);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "MinimumStandardFilter initialization failed!");
  }
}

std::shared_ptr<const MinimumStandardFilter::PropertyIsBaseType>
MinimumStandardFilter::getNewOperationInstance(const NameType& field,
                                               const NameType& operationName,
                                               const boost::any& toWhat)
{
  try
  {
    try
    {
      OperationMapValueType op = FEConformanceClassBase::get(operationName);
      return std::shared_ptr<const PropertyIsBaseType>(op(field, toWhat));
    }
    catch (...)
    {
      Fmi::Exception exception(BCP, "Operation processing failed!", nullptr);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      throw exception;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(
        BCP, "MinimumStandardFilter operation '" + operationName + "' initialization failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
