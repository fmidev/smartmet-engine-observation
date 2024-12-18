#pragma once

#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
const std::string ROADCLOUD_PRODUCER = "roadcloud";
const std::string TECONER_PRODUCER = "teconer";
const std::string NETATMO_PRODUCER = "netatmo";
const std::string FMI_IOT_PRODUCER = "fmi_iot";
const std::string TAPSI_QC_PRODUCER = "tapsi_qc";

class ProducerId
{
 public:
  explicit ProducerId(int id) { init(id); }
  explicit ProducerId(const std::string& id)
  {
    if (std::string::npos != id.find_first_not_of("0123456789"))
      throw Fmi::Exception(BCP, "Unsupported producer id: " + id);
    init(Fmi::stoi(id));
  }

  int asInt() const { return itsIdInt; }
  const std::string& asString() const { return itsIdString; }
  const std::string& name() const { return itsName; }

 private:
  void init(int id)
  {
    if (id == 1 || id == 2 || id == 3 || id == 4 || id == 15)
    {
      itsIdString = Fmi::to_string(id);
      if (id == 1)
        itsName = ROADCLOUD_PRODUCER;
      else if (id == 2)
        itsName = TECONER_PRODUCER;
      else if (id == 3)
        itsName = NETATMO_PRODUCER;
      else if (id == 4)
        itsName = FMI_IOT_PRODUCER;
      else if (id == 15)
        itsName = TAPSI_QC_PRODUCER;
    }
    else
    {
      throw Fmi::Exception(BCP, "Unsupported producer id: " + Fmi::to_string(id));
    }
    itsIdInt = id;
  }

  int itsIdInt = 0;
  std::string itsIdString;
  std::string itsName;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
