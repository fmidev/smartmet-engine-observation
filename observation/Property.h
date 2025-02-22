#pragma once

#include <macgyver/DateTime.h>
#include <any>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 * @brief The class implements the basic parts of a single database expression.
 *
 * If the basic parts are "STATION_ID" (field name), "=" (operation) and 101000 (value)
 * returned expression is "STATION_ID = 101000"
 * A single operation is supposed to implement into an inherit class of the class.
 */
namespace Property
{
class Base
{
 public:
  using NameType = std::string;
  using OperatorType = std::string;
  explicit Base();
  virtual ~Base();

  Base& operator=(const Base& other) = delete;

  /**
   * @brief Get expression string constructed from the member parameter values.
   * @param viewName Name of a view that has the fied name (property) set into the class object.
   * @return Expression string
   */
  virtual NameType getExpression(const NameType& viewName,
                                 const std::string& database = "oracle") const;

  /**
   * @brief Get value type of the \a toWhat input parameter as a string.
   *
   * Returned value can be compared with the configured value type in
   * DBRegistryConfig configuration. Purpose for this method is that
   * a user can "ensure" datatype consistency between database datatype and
   * input value datatype in an expression.
   *
   * Following type conversions are supported:
   *   "int" <-- int16_t, int32_t, int64_t, uint16_t, uint32_t, uint64_t
   *   "float" <-- float
   *   "double" <-- double
   *   "string" <-- std::string
   *   "Fmi::DateTime" <-- Fmi::DateTime
   *   "unknown" <-- all the other types
   *
   * NOTE! If some inherit class require an other value type than
   *       the configured data type in database registry
   *       configuration it should have an own implementation.
   */
  virtual NameType getValueTypeString() const;

 protected:
  Base(const Base& other);
  inline NameType getProperty() const { return m_property; }
  inline NameType getOperator() const { return m_operator; }
  inline std::any getToWhat() const { return m_toWhat; }
  NameType toWhatString(const std::any& value, const std::string& database = "oracle") const;
  virtual bool has_vector_argument() const { return false; }

  /**
   * @brief Set basic parts of an operation.
   * @param property Field name.
   * @param toWhat A value to compared with the values behind \a property name.
   * @param op A comparison operator.
   */
  void set(const NameType& property, const std::any& toWhat, const NameType& op);

  /**
   * @brief Get an inherit operation.
   * @param property Field name.
   * @toWhat A value to compared with the values behind \a property name.
   * @return Operation object
   */
  virtual std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) = 0;

 private:
  NameType m_property;
  std::any m_toWhat;
  NameType m_operator;
};

class IsEqualTo : public Base
{
 public:
  ~IsEqualTo() override;
  IsEqualTo() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsEqualTo> obj(new IsEqualTo);
    obj->set(property, toWhat, "=");
    return obj;
  }
};

class IsNotEqualTo : public Base
{
 public:
  ~IsNotEqualTo() override;
  IsNotEqualTo() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsNotEqualTo> obj(new IsNotEqualTo);
    obj->set(property, toWhat, "!=");
    return obj;
  }
};

class IsLessThan : public Base
{
 public:
  ~IsLessThan() override;
  IsLessThan() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsLessThan> obj(new IsLessThan);
    obj->set(property, toWhat, "<");
    return obj;
  }
};

class IsLessThanOrEqualTo : public Base
{
 public:
  ~IsLessThanOrEqualTo() override;
  IsLessThanOrEqualTo() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsLessThanOrEqualTo> obj(new IsLessThanOrEqualTo);
    obj->set(property, toWhat, "<=");
    return obj;
  }
};

class IsGreaterThan : public Base
{
 public:
  ~IsGreaterThan() override;
  IsGreaterThan() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsGreaterThan> obj(new IsGreaterThan);
    obj->set(property, toWhat, ">");
    return obj;
  }
};

class IsGreaterThanOrEqualTo : public Base
{
 public:
  ~IsGreaterThanOrEqualTo() override;
  IsGreaterThanOrEqualTo() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsGreaterThanOrEqualTo> obj(new IsGreaterThanOrEqualTo);
    obj->set(property, toWhat, ">=");
    return obj;
  }
};

class IsNull : public Base
{
 public:
  ~IsNull() override;
  IsNull() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& /* toWhat */) override
  {
    std::shared_ptr<IsNull> obj(new IsNull);
    obj->set(property, std::any(std::string("NULL")), "");
    return obj;
  }
};

class IsNotNull : public Base
{
 public:
  ~IsNotNull() override;
  IsNotNull() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& /* toWhat */) override
  {
    std::shared_ptr<IsNotNull> obj(new IsNotNull);
    obj->set(property, std::any(std::string("NULL")), "NOT");
    return obj;
  }
};

class IsNil : public Base
{
 public:
  ~IsNil() override;
  IsNil() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& /* toWhat */) override
  {
    std::shared_ptr<IsNil> obj(new IsNil);
    obj->set(property, std::any(std::string("EMPTY")), "is");
    return obj;
  }
};

class IsLike : public Base
{
 public:
  ~IsLike() override;
  IsLike() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsLike> obj(new IsLike);
    std::string val;
    // Special case when the type is string. (Fmi::DateTime is also a problem).
    // toWhatString method is catenating apostrophes (') around a string.
    if ((toWhat).type() == typeid(std::string))
      val = "%" + std::any_cast<std::string>(toWhat) + "%";
    else
      val = "%" + toWhatString(toWhat) + "%";
    obj->set(property, std::any(val), "LIKE");
    return obj;
  }
};

/* This class is not fully implemented. User must give the value as a string e.g. "2 AND 3". */
class IsBetween : public Base
{
 public:
  ~IsBetween() override;
  IsBetween() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsBetween> obj(new IsBetween);
    obj->set(property, toWhat, "BETWEEN");
    return obj;
  }
};

// Extended filters
class MinuteValueModuloIsEqualToZero : public Base
{
 public:
  ~MinuteValueModuloIsEqualToZero() override;
  MinuteValueModuloIsEqualToZero() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<MinuteValueModuloIsEqualToZero> obj(new MinuteValueModuloIsEqualToZero);
    obj->set(property, toWhat, "=");
    return obj;
  }
  Base::NameType getExpression(const Base::NameType& viewName,
                               const std::string& database = "oracle") const override;

  // Owerrides the base class implementation!
  // return "Fmi::DateTime" for the following types: int16_t, int32_t, int64_t, int64_t, uint16_t,
  // uint32_t, uint64_t otherwise "unknown"
  NameType getValueTypeString() const override;
};

class IsOneOf : public Base
{
 public:
  ~IsOneOf() override;
  IsOneOf() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsOneOf> obj(new IsOneOf);
    obj->set(property, toWhat, " IN ");
    return obj;
  }

 protected:
  bool has_vector_argument() const override { return true; }
};

class IsNotOf : public Base
{
 public:
  ~IsNotOf() override;
  IsNotOf() = default;
  std::shared_ptr<Base> get(const NameType& property, const std::any& toWhat) override
  {
    std::shared_ptr<IsNotOf> obj(new IsNotOf);
    obj->set(property, toWhat, " NOT IN ");
    return obj;
  }

 protected:
  bool has_vector_argument() const override { return true; }
};

}  // namespace Property
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
