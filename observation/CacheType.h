#pragma once

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
enum class CacheType
{
  Default,  // observation_data and weather_data_qc
  Flash,    // flash_data
  Mobile    // mobile observations
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
