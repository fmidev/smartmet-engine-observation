#include "MinimumStandardFilter.h"
#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
MinimumStandardFilter::MinimumStandardFilter() : FEConformanceClassBase()
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
      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!", NULL);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "MinimumStandardFilter initialization failed!", NULL);
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
      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!", NULL);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      throw exception;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(
        BCP,
        "MinimumStandardFilter operation '" + operationName + "' initialization failed!",
        NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
