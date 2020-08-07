package io.grpc.kotlin

import arrow.core.Either
import arrow.fx.coroutines.milliseconds
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.append
import arrow.fx.coroutines.stream.compile
import com.google.common.truth.Truth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import org.junit.Assert.fail
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class HelpersTest {

  @Test
  fun `Empty stream results in none internal exception`() = runBlocking {
    try {
      Stream.empty<Int>()
        .singleOrStatusStream("request", "")
        .compile()
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
      .compile()
      .toList()

    assertThat(res).isEqualTo(listOf(1))
  }

  @Test
  fun `Multi item stream results in two found internal exception`() = runBlocking {
    val res = Either.catch {
      Stream(1, 2, 3)
        .singleOrStatusStream("request", "")
        .compile()
        .toList()
    }

    res.fold({ e ->
      if (e is StatusException) {
        assertThat(e.status.code).isEqualTo(Status.Code.INTERNAL)
        assertThat(e.status.description).contains("but received two")
      } else fail("Expected StatusException but found $e")
    }, { fail("Expected StatusException with Status.Code.INTERNAL about two emissions") })
  }

}