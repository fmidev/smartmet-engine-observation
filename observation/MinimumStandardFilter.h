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
  using NameType = FEConformanceClassBase::NameType;
  using PropertyIsBaseType = FEConformanceClassBase::PropertyIsBaseType;

  /**
   * @exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *            The class initialization failed.
   */
  explicit MinimumStandardFilter();

  MinimumStandardFilter(const MinimumStandardFilter& other) = delete;
  MinimumStandardFilter operator=(const MinimumStandardFilter& other) = delete;

  std::shared_ptr<const PropertyIsBaseType> getNewOperationInstance(
      const NameType& field, const NameType& operationName, const std::any& toWhat) override;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
