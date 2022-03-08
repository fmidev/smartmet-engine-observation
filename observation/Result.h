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
  SparseTable data;

 private:
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
