#pragma once

#include <string>

namespace SmartMet
{
/*
 * \brief This class contains data of one ObservableProperty instance.
 * \author Santeri Oksman
 *
 * Example content in xml:
 * <component>
 * <ObservableProperty gml:id="air-temp-10min-max">
 *   <label>10 minutes maximum air temperature</label>
 *   <basePhenomenon>airTemperature</basePhenomenon>
 *   <uom uom="Cel"/>
 *   <statisticalMeasure>
 *     <StatisticalMeasure gml:id="max-10min">
 *       <statisticalFunction>max</statisticalFunction>
 *       <aggregationTimePeriod>PT10M</aggregationTimePeriod>
 *     </StatisticalMeasure>
 *   </statisticalMeasure>
 * </ObservableProperty>
 * </component>
 */

namespace Engine
{
namespace Observation
{
struct ObservableProperty
{
  std::string measurandId;
  std::string measurandCode;
  std::string observablePropertyId;
  std::string observablePropertyLabel;
  std::string basePhenomenon;
  std::string uom;
  std::string statisticalMeasureId;
  std::string statisticalFunction;
  std::string aggregationTimePeriod;
  std::string gmlId;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
