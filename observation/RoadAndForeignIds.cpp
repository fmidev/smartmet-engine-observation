#include "RoadAndForeignIds.h"
#include <iostream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

const std::string missing_string = "MISSING";
const int missing_integer = 9999;

RoadAndForeignIds::RoadAndForeignIds()
    : itsStringToInteger{
  {"CH", 1},
  {"CHL1", 2},
  {"CHL2", 3},
  {"CHL3", 4},
  {"CHL4", 5},
  {"CN", 6},
  {"CNH", 7},
  {"CNL1", 8},
  {"CNL2", 9},
  {"CNL3", 10},
  {"CNL4", 11},
  {"CTCH", 12},
  {"CTCL", 13},
  {"CTCM", 14},
  {"CTL1", 15},
  {"CTL2", 16},
  {"CTL3", 17},
  {"CTL4", 18},
  {"E", 19},
  {"NET", 20},
  {"P0", 21},
  {"Pa", 22},
  {"PPP", 23},
  {"GPM", 24},
  {"PR_12H", 25},
  {"PR_1H", 26},
  {"PR_24H", 27},
  {"PR_6H", 28},
  {"PSEA", 29},
  {"RH", 30},
  {"SD", 31},
  {"SUNDUR", 32},
  {"TA", 33},
  {"TAMAX12H", 34},
  {"TAMAX24H", 35},
  {"TAMIN12H", 36},
  {"TAMIN24H", 37},
  {"TG", 38},
  {"TD", 39},
  {"VV", 40},
  {"WD", 41},
  {"WG", 42},
  {"WG1H", 43},
  {"WS", 44},
  {"WW", 45},
  {"W1", 46},
  {"W2", 47},
  {"AKKUJ", 48},
  {"AVIKA", 49},
  {"DILMA", 50},
  {"DIPAINE", 51},
  {"DTIEL", 52},
  {"ILMA", 53},
  {"IPAINE", 54},
  {"JAATJ", 55},
  {"JAATP", 56},
  {"KASTEP", 57},
  {"KELI", 58},
  {"KELI2", 59},
  {"KITKA", 60},
  {"KOSM", 61},
  {"KOSTE", 62},
  {"KPERO", 63},
  {"KTUULI", 64},
  {"LI", 65},
  {"LS", 66},
  {"LUNTA", 67},
  {"LW", 68},
  {"MAAL", 69},
  {"MTUULI", 70},
  {"PSING", 71},
  {"RINT", 72},
  {"RST", 73},
  {"RSUM", 74},
  {"RSUM1H",75},
  {"SADE", 76},
  {"SADEON", 77},
  {"SJOHT", 78},
  {"STILA", 79},
  {"STST", 80},
  {"SUOM", 81},
  {"SUOV", 82},
  {"TIE", 83},
  {"TSUUNT", 84},
  {"TURL", 85},
  {"VALO", 86},
  {"VARO", 87},
  {"VARO3", 88},
  {"VIRTA", 89},
  {"VIS", 90},
  {"VSAA", 91}}
{
  // Actual ids for parametrs can be found on wiki pages
  // https://wiki.fmi.fi/pages/viewpage.action?pageId=37040091
  // https://wiki.fmi.fi/pages/viewpage.action?spaceKey=Manuals&title=Ulkomaiden+SYNOP-havainnot+havaintotietokannassa
  // Ari Aaltonen: Seuraavia tiesääsuureita ei löydy kannasta: LUMIS, VALO, VIRTA, DIPAINE, PRT, 12, VARO2, AKKUJ
  //
  // Here we dont use measurand id defined in abowe mentioned wiki pages, but we have assigned a unique running number 
  // for each parameters since road and foreign producers have (in some cases) same measurand id for different 
  // parameters (e.g. 'ILMA'/'TA','MTUULI'/'WG') and we dont want to add producer column in cache table
 
  for(const auto & item : itsStringToInteger)
	itsIntegerToString.insert(std::make_pair(item.second, item.first));
}

const std::string& RoadAndForeignIds::integerToString(int int_value) const
{
  const auto pos = itsIntegerToString.find(int_value);
  if(pos == itsIntegerToString.end())
    return missing_string;

  return pos->second;
}

int RoadAndForeignIds::stringToInteger(const std::string& string_value) const
{
  const auto pos = itsStringToInteger.find(string_value);
  if(pos == itsStringToInteger.end())
    return missing_integer;

  return pos->second;
}

}  // namespace Observation

}  // namespace Engine
}  // namespace SmartMet

