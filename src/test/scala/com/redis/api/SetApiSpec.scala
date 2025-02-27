package com.redis.api

import com.redis.common.IntSpec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

trait SetApiSpec extends AnyFunSpec
                        with Matchers
                        with IntSpec {

  // todo: remove HashApi, ListApi
  override protected def r: BaseApi with StringApi with SetApi with AutoCloseable with HashApi with ListApi

  sadd()
  saddWithVariadicArguments()
  scard()
  sdiff()
  sinter()
  sinterEmpty()
  sinterstore()
  sismember()
  smembers()
  smove()
  smoveError()
  spop()
  spopWithCount()
  srandmember()
  srandmemberWithCount()
  srem()
  sremWithVariadicArguments()
  sunion()
  sunionstore()

  protected def sadd(): Unit = {
  describe("sadd") {
    it("should add a non-existent value to the set") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
    }
    it("should not add an existing value to the set") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "foo").get should equal(0)
    }
    it("should fail if the key points to a non-set") {
      r.lpush("list-1", "foo") should equal(Some(1))
      val thrown = the [Exception] thrownBy { r.sadd("list-1", "foo") }
      thrown.getMessage should include ("Operation against a key holding the wrong kind of value")
    }
  }
  }

  protected def saddWithVariadicArguments(): Unit = {
  describe("sadd with variadic arguments") {
    it("should add a non-existent value to the set") {
      r.sadd("set-1", "foo", "bar", "baz").get should equal(3)
      r.sadd("set-1", "foo", "bar", "faz").get should equal(1)
      r.sadd("set-1", "bar").get should equal(0)
    }
  }
  }

  protected def srem(): Unit = {
  describe("srem") {
    it("should remove a value from the set") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.srem("set-1", "bar").get should equal(1)
      r.srem("set-1", "foo").get should equal(1)
    }
    it("should not do anything if the value does not exist") {
      r.sadd("set-1", "foo").get should equal(1)
      r.srem("set-1", "bar").get should equal(0)
    }
    it("should fail if the key points to a non-set") {
      r.lpush("list-1", "foo") should equal(Some(1))
      val thrown = the [Exception] thrownBy { r.srem("list-1", "foo") }
      thrown.getMessage should include ("Operation against a key holding the wrong kind of value")
    }
  }
  }

  protected def sremWithVariadicArguments(): Unit = {
  describe("srem with variadic arguments") {
    it("should remove a value from the set") {
      r.sadd("set-1", "foo", "bar", "baz", "faz").get should equal(4)
      r.srem("set-1", "foo", "bar").get should equal(2)
      r.srem("set-1", "foo").get should equal(0)
      r.srem("set-1", "baz", "bar").get should equal(1)
    }
  }
  }

  protected def spop(): Unit = {
  describe("spop") {
    it("should pop a random element") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.spop("set-1").get should (equal("foo") or equal("bar") or equal("baz"))
    }
    it("should return nil if the key does not exist") {
      r.spop("set-1") should equal(None)
    }
  }
  }

  protected def spopWithCount(): Unit = {
  describe("spop with count") {
    it("should pop a list of random members") {
      r.sadd("set-1", "one").get should equal(1)
      r.sadd("set-1", "two").get should equal(1)
      r.sadd("set-1", "three").get should equal(1)
      r.sadd("set-1", "four").get should equal(1)
      r.sadd("set-1", "five").get should equal(1)
      r.sadd("set-1", "six").get should equal(1)
      r.sadd("set-1", "seven").get should equal(1)
      r.sadd("set-1", "eight").get should equal(1)

      r.spop("set-1", 2).get.size should equal(2)

      // if supplied count > size, then whole set is returned
      r.spop("set-1", 24).get.size should equal(6)

      // if empty, returned set is empty
      r.spop("set-1", 5).get shouldBe empty
    }
  }
  }

  protected def smove(): Unit = {
  describe("smove") {
    it("should move from one set to another") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)

      r.sadd("set-2", "1").get should equal(1)
      r.sadd("set-2", "2").get should equal(1)

      r.smove("set-1", "set-2", "baz").get should equal(1)
      r.sadd("set-2", "baz").get should equal(0)
      r.sadd("set-1", "baz").get should equal(1)
    }
    it("should return 0 if the element does not exist in source set") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.smove("set-1", "set-2", "bat").get should equal(0)
      r.smove("set-3", "set-2", "bat").get should equal(0)
    }
  }
  }

  protected def smoveError(): Unit = {
  describe("smove") {
    it("should give error if the source or destination key is not a set") {
      r.lpush("list-1", "foo") should equal(Some(1))
      r.lpush("list-1", "bar") should equal(Some(2))
      r.lpush("list-1", "baz") should equal(Some(3))
      r.sadd("set-1", "foo").get should equal(1)
      val thrown = the [Exception] thrownBy { r.smove("list-1", "set-1", "bat") }
      thrown.getMessage should include ("Operation against a key holding the wrong kind of value")
    }
  }
  }

  protected def scard(): Unit = {
  describe("scard") {
    it("should return cardinality") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.scard("set-1").get should equal(3)
    }
    it("should return 0 if key does not exist") {
      r.scard("set-1").get should equal(0)
    }
  }
  }

  protected def sismember(): Unit = {
  describe("sismember") {
    it("should return true for membership") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.sismember("set-1", "foo") should equal(true)
    }
    it("should return false for no membership") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.sismember("set-1", "fo") should equal(false)
    }
    it("should return false if key does not exist") {
      r.sismember("set-1", "fo") should equal(false)
    }
  }
  }

  protected def sinter(): Unit = {
  describe("sinter") {
    it("should return intersection") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)

      r.sadd("set-2", "foo").get should equal(1)
      r.sadd("set-2", "bat").get should equal(1)
      r.sadd("set-2", "baz").get should equal(1)

      r.sadd("set-3", "for").get should equal(1)
      r.sadd("set-3", "bat").get should equal(1)
      r.sadd("set-3", "bay").get should equal(1)

      r.sinter("set-1", "set-2").get should equal(Set(Some("foo"), Some("baz")))
      r.sinter("set-1", "set-3").get should equal(Set.empty)
    }
  }
  }

  protected def sinterEmpty(): Unit = {
  describe("sinter") {
    it("should return empty set for non-existing key") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.sinter("set-1", "set-4") should equal(Some(Set()))
    }
  }
  }

  protected def sinterstore(): Unit = {
  describe("sinterstore") {
    it("should store intersection") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)

      r.sadd("set-2", "foo").get should equal(1)
      r.sadd("set-2", "bat").get should equal(1)
      r.sadd("set-2", "baz").get should equal(1)

      r.sadd("set-3", "for").get should equal(1)
      r.sadd("set-3", "bat").get should equal(1)
      r.sadd("set-3", "bay").get should equal(1)

      r.sinterstore("set-r", "set-1", "set-2").get should equal(2)
      r.scard("set-r").get should equal(2)
      r.sinterstore("set-s", "set-1", "set-3").get should equal(0)
      r.scard("set-s").get should equal(0)
    }
    it("should return empty set for non-existing key") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.sinterstore("set-r", "set-1", "set-4").get should equal(0)
      r.scard("set-r").get should equal(0)
    }
  }
  }

  protected def sunion(): Unit = {
  describe("sunion") {
    it("should return union") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)

      r.sadd("set-2", "foo").get should equal(1)
      r.sadd("set-2", "bat").get should equal(1)
      r.sadd("set-2", "baz").get should equal(1)

      r.sadd("set-3", "for").get should equal(1)
      r.sadd("set-3", "bat").get should equal(1)
      r.sadd("set-3", "bay").get should equal(1)

      r.sunion("set-1", "set-2").get should equal(Set(Some("foo"), Some("bar"), Some("baz"), Some("bat")))
      r.sunion("set-1", "set-3").get should equal(Set(Some("foo"), Some("bar"), Some("baz"), Some("for"), Some("bat"), Some("bay")))
    }
    it("should return empty set for non-existing key") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.sunion("set-1", "set-2").get should equal(Set(Some("foo"), Some("bar"), Some("baz")))
    }
  }
  }

  protected def sunionstore(): Unit = {
  describe("sunionstore") {
    it("should store union") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)

      r.sadd("set-2", "foo").get should equal(1)
      r.sadd("set-2", "bat").get should equal(1)
      r.sadd("set-2", "baz").get should equal(1)

      r.sadd("set-3", "for").get should equal(1)
      r.sadd("set-3", "bat").get should equal(1)
      r.sadd("set-3", "bay").get should equal(1)

      r.sunionstore("set-r", "set-1", "set-2").get should equal(4)
      r.scard("set-r").get should equal(4)
      r.sunionstore("set-s", "set-1", "set-3").get should equal(6)
      r.scard("set-s").get should equal(6)
    }
    it("should treat non-existing keys as empty sets") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.sunionstore("set-r", "set-1", "set-4").get should equal(3)
      r.scard("set-r").get should equal(3)
    }
  }
  }

  protected def sdiff(): Unit = {
  describe("sdiff") {
    it("should return diff") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)

      r.sadd("set-2", "foo").get should equal(1)
      r.sadd("set-2", "bat").get should equal(1)
      r.sadd("set-2", "baz").get should equal(1)

      r.sadd("set-3", "for").get should equal(1)
      r.sadd("set-3", "bat").get should equal(1)
      r.sadd("set-3", "bay").get should equal(1)

      r.sdiff("set-1", "set-2", "set-3").get should equal(Set(Some("bar")))
    }
    it("should treat non-existing keys as empty sets") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.sdiff("set-1", "set-2").get should equal(Set(Some("foo"), Some("bar"), Some("baz")))
    }
  }
  }

  protected def smembers(): Unit = {
  describe("smembers") {
    it("should return members of a set") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.smembers("set-1").get should equal(Set(Some("foo"), Some("bar"), Some("baz")))
    }
    it("should return None for an empty set") {
      r.smembers("set-1") should equal(Some(Set()))
    }
  }
  }

  protected def srandmember(): Unit = {
  describe("srandmember") {
    it("should return a random member") {
      r.sadd("set-1", "foo").get should equal(1)
      r.sadd("set-1", "bar").get should equal(1)
      r.sadd("set-1", "baz").get should equal(1)
      r.srandmember("set-1").get should (equal("foo") or equal("bar") or equal("baz"))
    }
    it("should return None for a non-existing key") {
      r.srandmember("set-1") should equal(None)
    }
  }
  }

  protected def srandmemberWithCount(): Unit = {
  describe("srandmember with count") {
    it("should return a list of random members") {
      r.sadd("set-1", "one").get should equal(1)
      r.sadd("set-1", "two").get should equal(1)
      r.sadd("set-1", "three").get should equal(1)
      r.sadd("set-1", "four").get should equal(1)
      r.sadd("set-1", "five").get should equal(1)
      r.sadd("set-1", "six").get should equal(1)
      r.sadd("set-1", "seven").get should equal(1)
      r.sadd("set-1", "eight").get should equal(1)

      r.srandmember("set-1", 2).get.size should equal(2)

      // returned elements should be unique
      val l = r.srandmember("set-1", 4).get
      l.size should equal(l.toSet.size)

      // returned elements may have duplicates
      r.srandmember("set-1", -4).get.toSet.size should (be <= (4))

      // if supplied count > size, then whole set is returned
      r.srandmember("set-1", 24).get.toSet.size should equal(8)
    }
  }
  }
}
