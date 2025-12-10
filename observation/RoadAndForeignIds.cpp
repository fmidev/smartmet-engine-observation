#include "RoadAndForeignIds.h"
#include <macgyver/Exception.h>
#include <iostream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
const std::string missing_string = "MISSING";
const int missing_integer = 9999;

// NOTE: There are no duplicate names!

RoadAndForeignIds::RoadAndForeignIds()
    : itsForeignNames{{"CH", 120},
                      {"CHL1", 132},
                      {"CHL2", 133},
                      {"CHL3", 134},
                      {"CHL4", 135},
                      {"CN", 119},
                      {"CNH", 175},
                      {"CNL1", 121},
                      {"CNL2", 122},
                      {"CNL3", 123},
                      {"CNL4", 124},
                      {"CTCH", 125},
                      {"CTCL", 126},
                      {"CTCM", 127},
                      {"E", 148},
                      {"MISSING", missing_integer},  // Legacy kludge
                      {"NET", 613},
                      {"P0", 38},
                      {"Pa", 39},
                      {"PR_12H", 63},
                      {"PR_1H", 61},
                      {"PR_24H", 64},
                      {"PR_6H", 62},
                      {"PSEA", 37},
                      {"RH", 29},
                      {"SD", 84},
                      {"SUNDUR", 100},
                      {"TA", 1},
                      {"TAMAX12H", 21},
                      {"TAMAX24H", 25},
                      {"TAMIN12H", 22},
                      {"TAMIN24H", 26},
                      {"TD", 32},
                      {"VV", 54},
                      {"WD", 44},
                      {"WG", 47},
                      {"WG1H", 47},
                      {"WS", 41},
                      {"WW", 56},
                      // These four are for some reason defined in parameters.conf as "MISSING" to
                      // make them valid for the producer but any search would return nothing. Must
                      // be some legacy kludge.
                      {"PoP", missing_integer},
                      {"WeatherSymbol3", missing_integer},
                      {"ri_10min", missing_integer},
                      {"ww_aws", missing_integer}},
      itsRoadNames{{"AKKUJ", 186},
                   {"AVIKA", 191},
                   {"DILMA", 9},
                   {"DIPAINE", 40},
                   {"DTIEL", 10},
                   {"ILMA", 1},
                   {"IPAINE", 38},
                   {"JAATJ", 90},
                   {"JAATP", 8},
                   {"KASTEP", 32},
                   {"KELI", 86},
                   {"KELI2", 193},
                   {"KITKA", 195},
                   {"KOSM", 202},
                   {"KOSTE", 29},
                   {"KPERO", 201},
                   {"KTUULI", 41},
                   {"LI", 198},
                   {"LS", 197},
                   {"LUNTA", 84},
                   {"LW", 196},
                   {"MAAL", 6},
                   {"MISSING", missing_integer},  // Legacy kludge
                   {"MTUULI", 47},
                   {"PSING", 89},
                   {"RINT", 67},
                   {"RST", 81},
                   {"RSUM", 203},
                   {"RSUM1H", 61},
                   {"SADE", 150},
                   {"SADEON", 69},
                   {"SJOHT", 88},
                   {"STILA", 80},
                   {"STST", 138},
                   {"SUOM", 204},
                   {"SUOV", 205},
                   {"TIE", 5},
                   {"TSUUNT", 44},
                   {"TURL", 206},
                   {"VARO", 87},
                   {"VARO3", 194},
                   {"VIRTA", 192},
                   {"VIS", 54},
                   {"VSAA", 199}}
{
  // Actual ids for parametrs can be found on wiki pages
  // https://wiki.fmi.fi/pages/viewpage.action?pageId=37040091
  // https://wiki.fmi.fi/pages/viewpage.action?spaceKey=Manuals&title=Ulkomaiden+SYNOP-havainnot+havaintotietokannassa
  // Ari Aaltonen: Seuraavia tiesääsuureita ei löydy kannasta: LUMIS, VALO, VIRTA, DIPAINE, PRT, 12,
  // VARO2, AKKUJ
  //
  // Here we dont use measurand id defined in abowe mentioned wiki pages, but we have assigned a
  // unique running number for each parameters since road and foreign producers have (in some cases)
  // same measurand id for different parameters (e.g. 'ILMA'/'TA','MTUULI'/'WG') and we dont want to
  // add producer column in cache table

  for (const auto& item : itsForeignNames)
    itsForeignNumbers.insert(std::make_pair(item.second, item.first));

  for (const auto& item : itsRoadNames)
    itsRoadNumbers.insert(std::make_pair(item.second, item.first));
}

int RoadAndForeignIds::stringToInteger(const std::string& string_value,
                                       const std::string& producer) const
{
  if (producer == "foreign")
  {
    const auto pos = itsForeignNames.find(string_value);
    if (pos != itsForeignNames.end())
      return pos->second;
  }
  else if (producer == "road")
  {
    const auto pos = itsRoadNames.find(string_value);
    if (pos != itsRoadNames.end())
      return pos->second;
  }
  else
    throw Fmi::Exception(BCP, "Unknown EXT producer name '" + producer + "'");

  return missing_integer;
}

int RoadAndForeignIds::stringToInteger(const std::string& string_value) const
{
  {
    const auto pos = itsForeignNames.find(string_value);
    if (pos != itsForeignNames.end())
      return pos->second;
  }

  {
    const auto pos = itsRoadNames.find(string_value);
    if (pos != itsRoadNames.end())
      return pos->second;
  }

  return missing_integer;
}

const std::string& RoadAndForeignIds::integerToString(int int_value,
                                                      const std::string& producer) const
{
  if (producer == "foreign")
  {
    const auto pos = itsForeignNumbers.find(int_value);
    if (pos != itsForeignNumbers.end())
      return pos->second;
  }
  else if (producer == "road")
  {
    const auto pos = itsRoadNumbers.find(int_value);
    if (pos != itsRoadNumbers.end())
      return pos->second;
  }
  else
    throw Fmi::Exception(BCP, "Unknown EXT producer name '" + producer + "'");

  return missing_string;
}

}  // namespace Observation

}  // namespace Engine
}  // namespace SmartMet
