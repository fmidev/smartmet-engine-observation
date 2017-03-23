#pragma once

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class Result : private boost::noncopyable
{
 public:
  Result();
  vector<std::string> headers;
  SmartMet::SparseTable data;

 private:
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
