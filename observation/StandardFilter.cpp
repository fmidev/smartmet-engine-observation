#include "StandardFilter.h"
#include <macgyver/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
StandardFilter::StandardFilter() : MinimumStandardFilter()
{
  try
  {
    try
    {
      FEConformanceClassBase::add("PropertyIsNull", Property::IsNull());
      FEConformanceClassBase::add("PropertyIsNotNull", Property::IsNotNull());
      FEConformanceClassBase::add("PropertyIsNil", Property::IsNil());
      FEConformanceClassBase::add("PropertyIsLike", Property::IsLike());
      FEConformanceClassBase::add("PropertyIsBetween", Property::IsBetween());
    }
    catch (...)
    {
      Fmi::Exception exception(BCP, "Operation processing failed!", nullptr);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail("StandardFilter initialization failed.");
      throw exception;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<const StandardFilter::PropertyIsBaseType> StandardFilter::getNewOperationInstance(
    const NameType& field, const NameType& operationName, const boost::any& toWhat)
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
      throw Fmi::Exception(BCP, "Operation processing failed!")
          .addDetail(
              fmt::format("StandardFilter operation '{}' initialization failed!", operationName));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
