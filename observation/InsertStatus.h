#pragma once

/*
 * \brief Keep track of used hash_values in a LRU cache
 *
 * Note: This is intentionally not thread safe. The idea
 * is to track unique elements in single writer mode to
 * avoid writing duplicates. This is hopefully faster
 * than letting sqlite resolve the duplicates, and hence
 * reduces the time needed for writer locks.
 *
 * Based on: https://stackoverflow.com/a/14503492/8896005
 */

#include <list>
#include <unordered_map>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class InsertStatus
{
 public:
  InsertStatus() = default;
  InsertStatus(std::size_t size) : mCacheSize(size) {}

  void add(std::size_t key)
  {
    mItems.push_front(key);
    mIterators.insert(std::make_pair(key, mItems.begin()));
    clean();
  }

  bool exists(std::size_t key) { return mIterators.count(key) > 0; }

  void resize(std::size_t size)
  {
    mCacheSize = size;
    clean();
  }

  const std::list<std::size_t>& items() const { return mItems; }

 private:
  std::list<std::size_t> mItems;
  std::unordered_map<std::size_t, decltype(mItems.begin())> mIterators;
  std::size_t mCacheSize = 0;

  void clean()
  {
    while (mIterators.size() > mCacheSize)
    {
      auto last = mItems.end();
      --last;
      mIterators.erase(*last);
      mItems.pop_back();
    }
  }
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
