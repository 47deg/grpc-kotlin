package io.grpc.kotlin

import arrow.core.Either
import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ForkConnected
import arrow.fx.coroutines.milliseconds
import arrow.fx.coroutines.sleep
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.append
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.terminateOnNone
import arrow.fx.coroutines.stream.toList
import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import org.junit.Assert.fail
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class HelpersTest : AbstractCallsTest() {

  @Test
  fun `Empty stream results in none internal exception`() = runBlocking {
    try {
      Stream.empty<Int>()
        .singleOrStatusStream("request", "")
        .toList()
    } catch (e: StatusException) {
      assertThat(e.status.code).isEqualTo(Status.Code.INTERNAL)
      assertThat(e.status.description).contains("but received none")
    } catch (e: Throwable) {
      fail("Expected StatusException but found $e")
    }
  }

  @Test
  fun `Single element stream passes correctly`() = runBlocking {
    val res = Stream(1)
      .singleOrStatusStream("request", "")
      .toList()

    assertThat(res).isEqualTo(listOf(1))
  }

  @Test
  fun `flatMapped Single element stream passes correctly`() = runBlocking {
    val res = Stream(1)
      .singleOrStatusStream("request", "")
      .flatMap { Stream(it, it) }
      .toList()

    assertThat(res).isEqualTo(listOf(1, 1))
  }

  @Test
  fun `Single flatmapped stream passes correctly`() = runBlocking {
    val res = Stream(1)
      .flatMap { Stream.effect { it } }
      .singleOrStatusStream("request", "")
      .effectMap { it + 1 }
      .toList()

    assertThat(res).isEqualTo(listOf(2))
  }

  @Test
  fun `Single effectMap stream passes correctly`() = runBlocking {
    val res = Stream(1)
      .effectMap { it + 1 }
      .singleOrStatusStream("request", "")
      .toList()

    assertThat(res).isEqualTo(listOf(2))
  }

  @Test
  fun `effectMap Single element stream passes correctly`() = runBlocking {
    val res = Stream(1)
      .singleOrStatusStream("request", "")
      .effectMap { it + 1 }
      .toList()

    assertThat(res).isEqualTo(listOf(2))
  }

  @Test
  fun `Multi element single chunk stream results in two found internal exception`() = runBlocking {
    val res = Either.catch {
      Stream(1, 2, 3)
        .singleOrStatusStream("request", "")
        .toList()
    }

    res.fold({ e ->
      if (e is StatusException) {
        assertThat(e.status.code).isEqualTo(Status.Code.INTERNAL)
        assertThat(e.status.description).contains("but received two")
      } else fail("Expected StatusException but found $e")
    }, { fail("Expected StatusException with Status.Code.INTERNAL about two emissions") })
  }

  @Test
  fun `Multi chunk stream results in two found internal exception`() = runBlocking {
    val res = Either.catch {
      val list = Stream(1).append { Stream(2) }
        .singleOrStatusStream("request", "")
        .toList()

      println("Here: $list")


      assertThat(list).isEqualTo(list)
    }

    res.fold({ e ->
      if (e is StatusException) {
        assertThat(e.status.code).isEqualTo(Status.Code.INTERNAL)
        assertThat(e.status.description).contains("but received two")
      } else fail("Expected StatusException but found $e")
    }, { fail("Expected StatusException with Status.Code.INTERNAL about two emissions") })
  }


  @Test
  fun `test scenario`() = runBlocking {
    val q = Queue.synchronous<Option<Int>>()

    ForkConnected {
      sleep(100.milliseconds)
      q.enqueue1(Some(1))
      q.enqueue1(None)
    }

    Stream.effect { Unit }
      .flatMap {
        q.dequeue()
          .terminateOnNone()
          .effectTap { println(it) }
      }.onFinalizeCase { case ->
        assertThat(case).isEqualTo(ExitCase.Completed)
      }.singleOrStatusStream("request", "")
      .effectMap { it + 1 }
      .toList()
  }

}