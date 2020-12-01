#pragma once

#include <map>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class RoadAndForeignIds
{
 public:
  RoadAndForeignIds();

  const std::string& integerToString(int value) const;
  int stringToInteger(const std::string& string_value) const;

 private:
  std::map<std::string, int> itsStringToInteger;
  std::map<int, std::string> itsIntegerToString;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
