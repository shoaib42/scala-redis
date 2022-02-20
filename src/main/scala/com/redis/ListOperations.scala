package com.redis

import com.redis.api.ListApi
import com.redis.serialization._

trait ListOperations extends ListApi {
  self: Redis =>

  override def lpush(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] =
    send("LPUSH", List(key, value) ::: values.toList)(asLong)

  override def lpushx(key: Any, value: Any)(implicit format: Format): Option[Long] =
    send("LPUSHX", List(key, value))(asLong)

  override def rpush(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Long] =
    send("RPUSH", List(key, value) ::: values.toList)(asLong)

  override def rpushx(key: Any, value: Any)(implicit format: Format): Option[Long] =
    send("RPUSHX", List(key, value))(asLong)

  override def llen(key: Any)(implicit format: Format): Option[Long] =
    send("LLEN", List(key))(asLong)

  override def lrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    send("LRANGE", List(key, start, end))(asList)

  override def ltrim(key: Any, start: Int, end: Int)(implicit format: Format): Boolean =
    send("LTRIM", List(key, start, end))(asBoolean)

  override def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("LINDEX", List(key, index))(asBulk)

  override def lset(key: Any, index: Int, value: Any)(implicit format: Format): Boolean =
    send("LSET", List(key, index, value))(asBoolean)

  override def lrem(key: Any, count: Int, value: Any)(implicit format: Format): Option[Long] =
    send("LREM", List(key, count, value))(asLong)

  override def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("LPOP", List(key))(asBulk)

  override def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("RPOP", List(key))(asBulk)

  override def rpoplpush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("RPOPLPUSH", List(srcKey, dstKey))(asBulk)

  override def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("BRPOPLPUSH", List(srcKey, dstKey, timeoutInSeconds))(asBulkWithTime)

  override def lmovell[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("LMOVE", List(srcKey, dstKey, "LEFT", "LEFT"))(asBulk)

  override def blmovell[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("BLMOVE", List(srcKey, dstKey, "LEFT", "LEFT", timeoutInSeconds))(asBulkWithTime)

  override def lmovelr[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("LMOVE", List(srcKey, dstKey, "LEFT", "RIGHT"))(asBulk)

  override def blmovelr[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("BLMOVE", List(srcKey, dstKey, "LEFT", "RIGHT", timeoutInSeconds))(asBulkWithTime)

  override def lmoverl[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("LMOVE", List(srcKey, dstKey, "RIGHT", "LEFT"))(asBulk)

  override def blmoverl[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("BLMOVE", List(srcKey, dstKey, "RIGHT", "LEFT", timeoutInSeconds))(asBulkWithTime)

  override def lmoverr[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("LMOVE", List(srcKey, dstKey, "RIGHT", "RIGHT"))(asBulk)

  override def blmoverr[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("BLMOVE", List(srcKey, dstKey, "RIGHT", "RIGHT", timeoutInSeconds))(asBulkWithTime)

  override def blpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V)] =
    send("BLPOP", key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _))(asListPairs[K, V].flatMap(_.flatten.headOption))

  override def brpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): Option[(K, V)] =
    send("BRPOP", key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _))(asListPairs[K, V].flatMap(_.flatten.headOption))
}
