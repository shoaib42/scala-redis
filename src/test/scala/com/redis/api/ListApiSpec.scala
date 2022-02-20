package com.redis.api

import com.redis.RedisClient
import com.redis.common.IntSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterEach}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future


trait ListApiSpec extends AnyFunSpec with ScalaFutures
  with Matchers
  with BeforeAndAfterEach
  with IntSpec {

  override protected def r: BaseApi with StringApi with ListApi with AutoCloseable

  blpop()
  blmovell()
  blmovelr()
  blmoverl()
  blmoverr()
  brpoplpush()
  lindex()
  llen()
  lmovell()
  lmovelr()
  lmoverl()
  lmoverr()
  lpop()
  lpush()
  lpushWithArrayBytes()
  lpushWithNewlines()
  lpushWithVariadicArguments()
  lpushx()
  lrange()
  lrem()
  lset()
  ltrim()
  rpop()
  rpoplpush()
  rpush()
  rpushWithVariadicArguments()
  rpushx()

  protected def lpush(): Unit = {
  describe("lpush") {
    it("should add to the head of the list") {
      r.lpush("list-1", "foo") should equal(Some(1))
      r.lpush("list-1", "bar") should equal(Some(2))
    }
    it("should throw if the key has a non-list value") {
      r.set("anshin-1", "debasish") should equal(true)
      val thrown = the [Exception] thrownBy { r.lpush("anshin-1", "bar") }
      thrown.getMessage should include("Operation against a key holding the wrong kind of value")
    }
  }
  }

  protected def lpushWithVariadicArguments(): Unit = {
  describe("lpush with variadic arguments") {
    it("should add to the head of the list") {
      r.lpush("list-1", "foo", "bar", "baz") should equal(Some(3))
      r.lpush("list-1", "bag", "fog") should equal(Some(5))
      r.lpush("list-1", "bag", "fog") should equal(Some(7))
    }
  }
  }

  protected def lpushx(): Unit = {
  describe("lpushx") {
    it("should add to the tail of the list") {
      r.lpush("list-1", "foo") should equal(Some(1))
      r.lpushx("list-1", "bar") should equal(Some(2))
    }
    it("should throw if the key has a non-list value") {
      r.set("anshin-1", "debasish") should equal(true)
      val thrown = the [Exception] thrownBy { r.lpushx("anshin-1", "bar") }
      thrown.getMessage should include("Operation against a key holding the wrong kind of value")
    }
  }
  }

  protected def rpush(): Unit = {
  describe("rpush") {
    it("should add to the tail of the list") {
      r.rpush("list-1", "foo") should equal(Some(1))
      r.rpush("list-1", "bar") should equal(Some(2))
    }
    it("should throw if the key has a non-list value") {
      r.set("anshin-1", "debasish") should equal(true)
      val thrown = the [Exception] thrownBy { r.rpush("anshin-1", "bar") }
      thrown.getMessage should include("Operation against a key holding the wrong kind of value")
    }
  }
  }

  protected def rpushWithVariadicArguments(): Unit = {
  describe("rpush with variadic arguments") {
    it("should add to the head of the list") {
      r.rpush("list-1", "foo", "bar", "baz") should equal(Some(3))
      r.rpush("list-1", "bag", "fog") should equal(Some(5))
      r.rpush("list-1", "bag", "fog") should equal(Some(7))
    }
  }
  }

  protected def rpushx(): Unit = {
  describe("rpushx") {
    it("should add to the tail of the list") {
      r.rpush("list-1", "foo") should equal(Some(1))
      r.rpushx("list-1", "bar") should equal(Some(2))
    }
    it("should throw if the key has a non-list value") {
      r.set("anshin-1", "debasish") should equal(true)
      val thrown = the [Exception] thrownBy { r.rpushx("anshin-1", "bar") }
      thrown.getMessage should include("Operation against a key holding the wrong kind of value")
    }
  }
  }

  protected def llen(): Unit = {
  describe("llen") {
    it("should return the length of the list") {
      r.lpush("list-1", "foo") should equal(Some(1))
      r.lpush("list-1", "bar") should equal(Some(2))
      r.llen("list-1").get should equal(2)
    }
    it("should return 0 for a non-existent key") {
      r.llen("list-2").get should equal(0)
    }
    it("should throw for a non-list key") {
      r.set("anshin-1", "debasish") should equal(true)
      val thrown = the [Exception] thrownBy { r.llen("anshin-1") }
      thrown.getMessage should include("Operation against a key holding the wrong kind of value")
    }
  }
  }

  protected def lrange(): Unit = {
  describe("lrange") {
    it("should return the range") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lpush("list-1", "3") should equal(Some(4))
      r.lpush("list-1", "2") should equal(Some(5))
      r.lpush("list-1", "1") should equal(Some(6))
      r.llen("list-1").get should equal(6)
      r.lrange("list-1", 0, 4).get should equal(List(Some("1"), Some("2"), Some("3"), Some("4"), Some("5")))
    }
    it("should return empty list if start > end") {
      r.lpush("list-1", "3") should equal(Some(1))
      r.lpush("list-1", "2") should equal(Some(2))
      r.lpush("list-1", "1") should equal(Some(3))
      r.lrange("list-1", 2, 0).get should equal(List())
    }
    it("should treat as end of list if end is over the actual end of list") {
      r.lpush("list-1", "3") should equal(Some(1))
      r.lpush("list-1", "2") should equal(Some(2))
      r.lpush("list-1", "1") should equal(Some(3))
      r.lrange("list-1", 0, 7).get should equal(List(Some("1"), Some("2"), Some("3")))
    }
  }
  }

  protected def ltrim(): Unit = {
  describe("ltrim") {
    it("should trim to the input size") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lpush("list-1", "3") should equal(Some(4))
      r.lpush("list-1", "2") should equal(Some(5))
      r.lpush("list-1", "1") should equal(Some(6))
      r.ltrim("list-1", 0, 3) should equal(true)
      r.llen("list-1") should equal(Some(4))
    }
    it("should should return empty list for start > end") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.ltrim("list-1", 6, 3) should equal(true)
      r.llen("list-1") should equal(Some(0))
    }
    it("should treat as end of list if end is over the actual end of list") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.ltrim("list-1", 0, 12) should equal(true)
      r.llen("list-1") should equal(Some(3))
    }
  }
  }

  protected def lindex(): Unit = {
  describe("lindex") {
    it("should return the value at index") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lpush("list-1", "3") should equal(Some(4))
      r.lpush("list-1", "2") should equal(Some(5))
      r.lpush("list-1", "1") should equal(Some(6))
      r.lindex("list-1", 2) should equal(Some("3"))
      r.lindex("list-1", 3) should equal(Some("4"))
      r.lindex("list-1", -1) should equal(Some("6"))
    }
    it("should return None if the key does not point to a list") {
      r.set("anshin-1", "debasish") should equal(true)
      r.lindex("list-1", 0) should equal(None)
    }
    it("should return empty string for an index out of range") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lindex("list-1", 8) should equal(None) // the protocol says it will return empty string
    }
  }
  }

  protected def lset(): Unit = {
  describe("lset") {
    it("should set value for key at index") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lpush("list-1", "3") should equal(Some(4))
      r.lpush("list-1", "2") should equal(Some(5))
      r.lpush("list-1", "1") should equal(Some(6))
      r.lset("list-1", 2, "30") should equal(true)
      r.lindex("list-1", 2) should equal(Some("30"))
    }
    it("should generate error for out of range index") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      val thrown = the [Exception] thrownBy { r.lset("list-1", 12, "30") }
      thrown.getMessage should include("index out of range")
    }
  }
  }

  protected def lrem(): Unit = {
  describe("lrem") {
    it("should remove count elements matching value from beginning") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "hello") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lpush("list-1", "hello") should equal(Some(4))
      r.lpush("list-1", "hello") should equal(Some(5))
      r.lpush("list-1", "hello") should equal(Some(6))
      r.lrem("list-1", 2, "hello") should equal(Some(2))
      r.llen("list-1") should equal(Some(4))
    }
    it("should remove all elements matching value from beginning") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "hello") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lpush("list-1", "hello") should equal(Some(4))
      r.lpush("list-1", "hello") should equal(Some(5))
      r.lpush("list-1", "hello") should equal(Some(6))
      r.lrem("list-1", 0, "hello") should equal(Some(4))
      r.llen("list-1") should equal(Some(2))
    }
    it("should remove count elements matching value from end") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "hello") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lpush("list-1", "hello") should equal(Some(4))
      r.lpush("list-1", "hello") should equal(Some(5))
      r.lpush("list-1", "hello") should equal(Some(6))
      r.lrem("list-1", -2, "hello") should equal(Some(2))
      r.llen("list-1") should equal(Some(4))
      r.lindex("list-1", -2) should equal(Some("4"))
    }
  }
  }

  protected def lpop(): Unit = {
  describe("lpop") {
    it("should pop the first one from head") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lpush("list-1", "3") should equal(Some(4))
      r.lpush("list-1", "2") should equal(Some(5))
      r.lpush("list-1", "1") should equal(Some(6))
      r.lpop("list-1") should equal(Some("1"))
      r.lpop("list-1") should equal(Some("2"))
      r.lpop("list-1") should equal(Some("3"))
      r.llen("list-1") should equal(Some(3))
    }
    it("should give nil for non-existent key") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpop("list-2") should equal(None)
      r.llen("list-1") should equal(Some(2))
    }
  }
  }

  protected def rpop(): Unit = {
  describe("rpop") {
    it("should pop the first one from tail") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.lpush("list-1", "4") should equal(Some(3))
      r.lpush("list-1", "3") should equal(Some(4))
      r.lpush("list-1", "2") should equal(Some(5))
      r.lpush("list-1", "1") should equal(Some(6))
      r.rpop("list-1") should equal(Some("6"))
      r.rpop("list-1") should equal(Some("5"))
      r.rpop("list-1") should equal(Some("4"))
      r.llen("list-1") should equal(Some(3))
    }
    it("should give nil for non-existent key") {
      r.lpush("list-1", "6") should equal(Some(1))
      r.lpush("list-1", "5") should equal(Some(2))
      r.rpop("list-2") should equal(None)
      r.llen("list-1") should equal(Some(2))
    }
  }
  }

  protected def lmovell(): Unit = {
    describe("lmovell") {
      it("should do") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))

        r.rpush("list-2", "foo") should equal(Some(1))
        r.rpush("list-2", "bar") should equal(Some(2))
        r.lmovell("list-1", "list-2") should equal(Some("a"))
        r.lindex("list-2", 0) should equal(Some("a"))
        r.llen("list-1") should equal(Some(2))
        r.llen("list-2") should equal(Some(3))
      }

      it("should keep the list same when src and dest are the same") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))
        r.lmovell("list-1", "list-1") should equal(Some("a"))
        r.lindex("list-1", 0) should equal(Some("a"))
        r.lindex("list-1", 2) should equal(Some("c"))
        r.llen("list-1") should equal(Some(3))
      }

      it("should time out and give None for non-existent key") {
        r.lmovell("test-1", "test-2") should equal(None)
        r.rpush("test-1", "a") should equal(Some(1))
        r.rpush("test-1", "b") should equal(Some(2))
        r.lmovell("test-1", "test-2") should equal(Some("a"))
      }
    }
  }

  protected def lmovelr(): Unit = {
    describe("blmovelr") {
      it("should do") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))

        r.rpush("list-2", "foo") should equal(Some(1))
        r.rpush("list-2", "bar") should equal(Some(2))
        r.lmovelr("list-1", "list-2") should equal(Some("a"))
        r.lindex("list-2", 2) should equal(Some("a"))
        r.llen("list-1") should equal(Some(2))
        r.llen("list-2") should equal(Some(3))
      }

      it("should rotate the list when src and dest are the same") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))
        r.lmovelr("list-1", "list-1") should equal(Some("a"))
        r.lindex("list-1", 0) should equal(Some("b"))
        r.lindex("list-1", 2) should equal(Some("a"))
        r.llen("list-1") should equal(Some(3))
      }

      it("should time out and give None for non-existent key") {
        r.lmovelr("test-1", "test-2") should equal(None)
        r.rpush("test-1", "a") should equal(Some(1))
        r.rpush("test-1", "b") should equal(Some(2))
        r.lmovelr("test-1", "test-2") should equal(Some("a"))
      }
    }
  }


  protected def lmoverl(): Unit = {
    describe("lmoverl") {
      it("should do") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))

        r.rpush("list-2", "foo") should equal(Some(1))
        r.rpush("list-2", "bar") should equal(Some(2))
        r.lmoverl("list-1", "list-2") should equal(Some("c"))
        r.lindex("list-2", 0) should equal(Some("c"))
        r.llen("list-1") should equal(Some(2))
        r.llen("list-2") should equal(Some(3))
      }

      it("should rotate the list when src and dest are the same") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))
        r.lmoverl("list-1", "list-1") should equal(Some("c"))
        r.lindex("list-1", 0) should equal(Some("c"))
        r.lindex("list-1", 2) should equal(Some("b"))
        r.llen("list-1") should equal(Some(3))
      }

      it("should give None for non-existent key") {
        r.lmoverl("list-1", "list-2") should equal(None)
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.lmoverl("list-1", "list-2") should equal(Some("b"))
      }
    }
  }

  protected def lmoverr(): Unit = {
    describe("lmoverr") {
      it("should do") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))

        r.rpush("list-2", "foo") should equal(Some(1))
        r.rpush("list-2", "bar") should equal(Some(2))
        r.lmoverr("list-1", "list-2") should equal(Some("c"))
        r.lindex("list-2", 2) should equal(Some("c"))
        r.llen("list-1") should equal(Some(2))
        r.llen("list-2") should equal(Some(3))
      }

      it("should keep the list same when src and dest are the same") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))
        r.lmoverr("list-1", "list-1") should equal(Some("c"))
        r.lindex("list-1", 0) should equal(Some("a"))
        r.lindex("list-1", 2) should equal(Some("c"))
        r.llen("list-1") should equal(Some(3))
      }

      it("should give None for non-existent key") {
        r.lmoverr("list-1", "list-2") should equal(None)
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.lmoverr("list-1", "list-2") should equal(Some("b"))
      }
    }
  }

  protected def rpoplpush(): Unit = {
  describe("rpoplpush") {
    it("should do") {
      r.rpush("list-1", "a") should equal(Some(1))
      r.rpush("list-1", "b") should equal(Some(2))
      r.rpush("list-1", "c") should equal(Some(3))

      r.rpush("list-2", "foo") should equal(Some(1))
      r.rpush("list-2", "bar") should equal(Some(2))
      r.rpoplpush("list-1", "list-2") should equal(Some("c"))
      r.lindex("list-2", 0) should equal(Some("c"))
      r.llen("list-1") should equal(Some(2))
      r.llen("list-2") should equal(Some(3))
    }

    it("should rotate the list when src and dest are the same") {
      r.rpush("list-1", "a") should equal(Some(1))
      r.rpush("list-1", "b") should equal(Some(2))
      r.rpush("list-1", "c") should equal(Some(3))
      r.rpoplpush("list-1", "list-1") should equal(Some("c"))
      r.lindex("list-1", 0) should equal(Some("c"))
      r.lindex("list-1", 2) should equal(Some("b"))
      r.llen("list-1") should equal(Some(3))
    }

    it("should give None for non-existent key") {
      r.rpoplpush("list-1", "list-2") should equal(None)
      r.rpush("list-1", "a") should equal(Some(1))
      r.rpush("list-1", "b") should equal(Some(2))
      r.rpoplpush("list-1", "list-2") should equal(Some("b"))
    }
  }
  }

  protected def lpushWithNewlines(): Unit = {
  describe("lpush with newlines in strings") {
    it("should add to the head of the list") {
      r.lpush("list-1", "foo\nbar\nbaz") should equal(Some(1))
      r.lpush("list-1", "bar\nfoo\nbaz") should equal(Some(2))
      r.lpop("list-1") should equal(Some("bar\nfoo\nbaz"))
      r.lpop("list-1") should equal(Some("foo\nbar\nbaz"))
    }
  }
  }

  protected def brpoplpush(): Unit = {
  describe("brpoplpush") {
    it("should do") {
      r.rpush("list-1", "a") should equal(Some(1))
      r.rpush("list-1", "b") should equal(Some(2))
      r.rpush("list-1", "c") should equal(Some(3))

      r.rpush("list-2", "foo") should equal(Some(1))
      r.rpush("list-2", "bar") should equal(Some(2))
      r.brpoplpush("list-1", "list-2", 2) should equal(Some("c"))
      r.lindex("list-2", 0) should equal(Some("c"))
      r.llen("list-1") should equal(Some(2))
      r.llen("list-2") should equal(Some(3))
    }

    it("should rotate the list when src and dest are the same") {
      r.rpush("list-1", "a") should equal(Some(1))
      r.rpush("list-1", "b") should equal(Some(2))
      r.rpush("list-1", "c") should equal(Some(3))
      r.brpoplpush("list-1", "list-1", 2) should equal(Some("c"))
      r.lindex("list-1", 0) should equal(Some("c"))
      r.lindex("list-1", 2) should equal(Some("b"))
      r.llen("list-1") should equal(Some(3))
    }

    it("should time out and give None for non-existent key") {
      r.brpoplpush("test-1", "test-2", 2) should equal(None)
      r.rpush("test-1", "a") should equal(Some(1))
      r.rpush("test-1", "b") should equal(Some(2))
      r.brpoplpush("test-1", "test-2", 2) should equal(Some("b"))
    }

    it("should pop blockingly") {
      val r1 = new RedisClient(redisContainerHost, redisContainerPort)

      val testVal: Future[Option[String]] = Future {
        r1.brpoplpush("l1", "l2", 3) should equal(Some("a"))
        r1.lpop("l2")
      }

      r.llen("l1").get should equal(0)
      r.lpush("l1", "a")

      testVal.futureValue should equal(Some("a"))

      r1.close()
    }
  }
  }

  protected def blmovell(): Unit = {
    describe("blmovell") {
      it("should do") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))

        r.rpush("list-2", "foo") should equal(Some(1))
        r.rpush("list-2", "bar") should equal(Some(2))
        r.blmovell("list-1", "list-2", 2) should equal(Some("a"))
        r.lindex("list-2", 0) should equal(Some("a"))
        r.llen("list-1") should equal(Some(2))
        r.llen("list-2") should equal(Some(3))
      }

      it("should keep the list same when src and dest are the same") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))
        r.blmovell("list-1", "list-1", 2) should equal(Some("a"))
        r.lindex("list-1", 0) should equal(Some("a"))
        r.lindex("list-1", 2) should equal(Some("c"))
        r.llen("list-1") should equal(Some(3))
      }

      it("should time out and give None for non-existent key") {
        r.blmovell("test-1", "test-2", 2) should equal(None)
        r.rpush("test-1", "a") should equal(Some(1))
        r.rpush("test-1", "b") should equal(Some(2))
        r.blmovell("test-1", "test-2", 2) should equal(Some("a"))
      }

      it("should pop blockingly") {
        val r1 = new RedisClient(redisContainerHost, redisContainerPort)

        val testVal: Future[Option[String]] = Future {
          r1.blmovell("l1", "l2", 3) should equal(Some("a"))
          r1.lpop("l2")
        }

        r.llen("l1").get should equal(0)
        r.lpush("l1", "a")

        testVal.futureValue should equal(Some("a"))

        r1.close()
      }
    }
  }

  protected def blmovelr(): Unit = {
    describe("blmovelr") {
      it("should do") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))

        r.rpush("list-2", "foo") should equal(Some(1))
        r.rpush("list-2", "bar") should equal(Some(2))
        r.blmovelr("list-1", "list-2", 2) should equal(Some("a"))
        r.lindex("list-2", 2) should equal(Some("a"))
        r.llen("list-1") should equal(Some(2))
        r.llen("list-2") should equal(Some(3))
      }

      it("should rotate the list when src and dest are the same") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))
        r.blmovelr("list-1", "list-1", 2) should equal(Some("a"))
        r.lindex("list-1", 0) should equal(Some("b"))
        r.lindex("list-1", 2) should equal(Some("a"))
        r.llen("list-1") should equal(Some(3))
      }

      it("should time out and give None for non-existent key") {
        r.blmovelr("test-1", "test-2", 2) should equal(None)
        r.rpush("test-1", "a") should equal(Some(1))
        r.rpush("test-1", "b") should equal(Some(2))
        r.blmovelr("test-1", "test-2", 2) should equal(Some("a"))
      }

      it("should pop blockingly") {
        val r1 = new RedisClient(redisContainerHost, redisContainerPort)

        val testVal: Future[Option[String]] = Future {
          r1.blmovelr("l1", "l2", 3) should equal(Some("a"))
          r1.lpop("l2")
        }

        r.llen("l1").get should equal(0)
        r.lpush("l1", "a")

        testVal.futureValue should equal(Some("a"))

        r1.close()
      }
    }
  }

  protected def blmoverl(): Unit = {
    describe("blmoverl") {
      it("should do") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))

        r.rpush("list-2", "foo") should equal(Some(1))
        r.rpush("list-2", "bar") should equal(Some(2))
        r.blmoverl("list-1", "list-2", 2) should equal(Some("c"))
        r.lindex("list-2", 0) should equal(Some("c"))
        r.llen("list-1") should equal(Some(2))
        r.llen("list-2") should equal(Some(3))
      }

      it("should rotate the list when src and dest are the same") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))
        r.blmoverl("list-1", "list-1", 2) should equal(Some("c"))
        r.lindex("list-1", 0) should equal(Some("c"))
        r.lindex("list-1", 2) should equal(Some("b"))
        r.llen("list-1") should equal(Some(3))
      }

      it("should time out and give None for non-existent key") {
        r.blmoverl("test-1", "test-2", 2) should equal(None)
        r.rpush("test-1", "a") should equal(Some(1))
        r.rpush("test-1", "b") should equal(Some(2))
        r.blmoverl("test-1", "test-2", 2) should equal(Some("b"))
      }

      it("should pop blockingly") {
        val r1 = new RedisClient(redisContainerHost, redisContainerPort)

        val testVal: Future[Option[String]] = Future {
          r1.blmoverl("l1", "l2", 3) should equal(Some("a"))
          r1.lpop("l2")
        }

        r.llen("l1").get should equal(0)
        r.lpush("l1", "a")

        testVal.futureValue should equal(Some("a"))

        r1.close()
      }
    }
  }

  protected def blmoverr(): Unit = {
    describe("blmoverr") {
      it("should do") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))

        r.rpush("list-2", "foo") should equal(Some(1))
        r.rpush("list-2", "bar") should equal(Some(2))
        r.blmoverr("list-1", "list-2", 2) should equal(Some("c"))
        r.lindex("list-2", 2) should equal(Some("c"))
        r.llen("list-1") should equal(Some(2))
        r.llen("list-2") should equal(Some(3))
      }

      it("should keep the list same when src and dest are the same") {
        r.rpush("list-1", "a") should equal(Some(1))
        r.rpush("list-1", "b") should equal(Some(2))
        r.rpush("list-1", "c") should equal(Some(3))
        r.blmoverr("list-1", "list-1", 2) should equal(Some("c"))
        r.lindex("list-1", 0) should equal(Some("a"))
        r.lindex("list-1", 2) should equal(Some("c"))
        r.llen("list-1") should equal(Some(3))
      }

      it("should time out and give None for non-existent key") {
        r.blmoverr("test-1", "test-2", 2) should equal(None)
        r.rpush("test-1", "a") should equal(Some(1))
        r.rpush("test-1", "b") should equal(Some(2))
        r.blmoverr("test-1", "test-2", 2) should equal(Some("b"))
      }

      it("should pop blockingly") {
        val r1 = new RedisClient(redisContainerHost, redisContainerPort)

        val testVal: Future[Option[String]] = Future {
          r1.blmoverr("l1", "l2", 3) should equal(Some("a"))
          r1.lpop("l2")
        }

        r.llen("l1").get should equal(0)
        r.lpush("l1", "a")

        testVal.futureValue should equal(Some("a"))

        r1.close()
      }
    }
  }

  protected def lpushWithArrayBytes(): Unit = {
  describe("lpush with array bytes") {
    it("should add to the head of the list") {
      r.lpush("list-1", "foo\nbar\nbaz".getBytes("UTF-8")) should equal(Some(1))
      r.lpop("list-1") should equal(Some("foo\nbar\nbaz"))
    }
  }
  }

  protected def blpop(): Unit = {
  describe("blpop") {
    it("should pop in a blocking mode") {
      val r1 = new RedisClient(redisContainerHost, redisContainerPort)

      val blpopV: Future[Option[(String, String)]] = Future {
        r1.blpop(3, "l1", "l2")
      }
      r.llen("l1").get should equal(0)
      r.lpush("l1", "a")
      blpopV.futureValue should equal(Some("l1", "a"))

      r1.close()
    }
  }
  }

}
