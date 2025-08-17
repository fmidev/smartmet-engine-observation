#pragma once

#include <map>
#include <string>

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

  const std::string& integerToString(int value, const std::string& producer) const;
  int stringToInteger(const std::string& string_value) const;
  int stringToInteger(const std::string& string_value, const std::string& producer) const;

 private:
  std::map<std::string, int> itsForeignNames;
  std::map<std::string, int> itsRoadNames;

  std::map<int, std::string> itsForeignNumbers;
  std::map<int, std::string> itsRoadNumbers;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
