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
      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!", NULL);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail("StandardFilter initialization failed.");
      throw exception;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!", NULL);
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg.str());
      throw exception;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
