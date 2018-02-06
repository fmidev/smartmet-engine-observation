#include "StandardFilter.h"
#include <spine/Exception.h>

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
      Spine::Exception exception(BCP, "Operation processing failed!", nullptr);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail("StandardFilter initialization failed.");
      throw exception;
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
      std::ostringstream msg;
      msg << "StandardFilter operation '" << operationName << "' initialization failed!";

      Spine::Exception exception(BCP, "Operation processing failed!", nullptr);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
