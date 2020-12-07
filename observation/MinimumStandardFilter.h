#pragma once

#include "FEConformanceClassBase.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 * Minimum standard filter operations of FES.
 *
 * The class allow access to the following filter operations:
 * - PropertyIsEqualTo
 * - PropertyIsNotEqualTo
 * - PropertyIsLessThan
 * - PropertyIsLessThanOrEqualTo
 * - PropertyIsGreaterThan
 * - PropertyIsGreaterThanOrEqualTo
 */
class MinimumStandardFilter : public FEConformanceClassBase
{
 public:
  typedef FEConformanceClassBase::NameType NameType;
  typedef FEConformanceClassBase::PropertyIsBaseType PropertyIsBaseType;

  /**
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *            The class initialization failed.
   */
  explicit MinimumStandardFilter();

  virtual boost::shared_ptr<const PropertyIsBaseType> getNewOperationInstance(
      const NameType& field, const NameType& operationName, const boost::any& toWhat);

 private:
  MinimumStandardFilter(const MinimumStandardFilter& other);
  MinimumStandardFilter operator=(const MinimumStandardFilter& other);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
