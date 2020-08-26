/*
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.kotlin

import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import arrow.core.getOrElse
import arrow.fx.coroutines.ForkAndForget
import arrow.fx.coroutines.ForkConnected
import arrow.fx.coroutines.milliseconds
import arrow.fx.coroutines.sleep
import arrow.fx.coroutines.stream.Chunk
import arrow.fx.coroutines.stream.Pull
import arrow.fx.coroutines.stream.PullUncons
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.drain
import arrow.fx.coroutines.stream.filterOption
import arrow.fx.coroutines.stream.flatMap
import arrow.fx.coroutines.stream.stream
import arrow.fx.coroutines.stream.terminateOnNone
import arrow.fx.coroutines.stream.toList
import arrow.fx.coroutines.stream.unconsN
import com.google.common.truth.Truth.assertThat
import io.grpc.examples.helloworld.HelloReply
import io.grpc.examples.helloworld.HelloRequest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/** Tests for the flow control of the Kotlin gRPC APIs. */
@RunWith(JUnit4::class)
class FlowControlTest : AbstractCallsTest() {
  val context = CoroutineName("server context")

  private suspend fun <T> Stream<T>.produceUnbuffered(): Queue<Option<T>> =
    produce {
      effectMap { enqueue1(Some(it)) }.drain()
    }

  @Test
  fun bidiPingPongFlowControl() = runBlocking {
    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context = context,
        descriptor = bidiStreamingSayHelloMethod,
        implementation = { requests -> requests.map { helloReply("Hello, ${it.name}") } }
      )
    )
    val latch = UnsafePromise<Unit>()
    val requests = Queue.synchronous<Option<HelloRequest>>()
    val responses: Queue<Option<HelloReply>> =
      ClientCalls.bidiStreamingRpc(
        channel = channel,
        requests = requests.dequeue().terminateOnNone(),
        method = bidiStreamingSayHelloMethod
      ).produceUnbuffered()
    requests.enqueue1(Some(helloRequest("Garnet")))
    requests.enqueue1(Some(helloRequest("Amethyst")))
    val third = ForkConnected {
      requests.enqueue1(Some(helloRequest("Steven")))
      latch.complete(Result.success(Unit))
    }
    sleep(200.milliseconds)  // wait for everything to work its way through the system
    assertThat(latch.tryGet()).isNull()
    assertThat(responses.dequeue1().getOrElse { null }).isEqualTo(helloReply("Hello, Garnet"))
    third.join() // pulling one element allows the cycle to advance
    requests.enqueue1(None)
    //responses.cancel()
  }

  @Test
  fun bidiPingPongFlowControlExpandedServerBuffer() = runBlocking {
    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context = context,
        descriptor = bidiStreamingSayHelloMethod,
        implementation = { requests: Stream<HelloRequest> ->
          // TODO: Channel.RENDEZVOUS ==? 0
          requests.buffer(1).map { helloReply("Hello, ${it.name}") }
        }
      )
    )
    val latch = UnsafePromise<Unit>()
    val requests = Queue.synchronous<Option<HelloRequest>>()
    val responses = ClientCalls.bidiStreamingRpc(
      channel = channel,
      requests = requests.dequeue().terminateOnNone(),
      method = bidiStreamingSayHelloMethod
    ).produceUnbuffered()
    requests.enqueue1(Some(helloRequest("Garnet")))
    requests.enqueue1(Some(helloRequest("Amethyst")))
    requests.enqueue1(Some(helloRequest("Pearl")))
    val fourth = ForkAndForget {
      requests.enqueue1(Some(helloRequest("Pearl")))
      latch.complete(Result.success(Unit))
    }
    sleep(200.milliseconds)
    // assertThat(latch.tryGet()).isNull()
    assertThat(responses.dequeue1().getOrElse { null }).isEqualTo(helloReply("Hello, Garnet"))
    fourth.join() // pulling one element allows the cycle to advance
    // responses.cancel()
  }

  private fun <O> Stream<O>.zipInPairs(): Stream<Pair<O, O>> {
    fun go(last: Chunk<O>, s: Pull<O, Unit>): Pull<Pair<O, O>, Unit> =
      s.unconsN(2).flatMap { uncons2: PullUncons<O>? ->
        when (uncons2) {
          null -> Pull.output1(Pair(last[0], last[1]))
          else -> Pull.output(Chunk(Pair(last[0], last[1]))).flatMap {
            go(uncons2.head, uncons2.tail)
          }
        }
      }

    return asPull().unconsN(2).flatMap { uncons2: PullUncons<O>? ->
      when (uncons2) {
        null -> Pull.done
        else -> go(uncons2.head, uncons2.tail)
      }
    }.stream()
  }

  @Test
  fun testZipInPairs() = runBlocking {
    Stream(1, 2, 3, 4, 5, 6)
      .zipInPairs()
      .toList()
      .let(::println)
  }

  @Test
  fun testZipInPairs2() = runBlocking {
    Stream(1, 2, 3, 4, 5, 6, 7)
      .zipInPairs()
      .toList()
      .let(::println)
  }

  @Test
  fun bidiPingPongFlowControlServerDrawsMultipleRequests() = runBlocking {
    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context = context,
        descriptor = bidiStreamingSayHelloMethod,
        implementation = { requests ->
          requests.zipInPairs().map { (a, b) -> helloReply("Hello, ${a.name} and ${b.name}") }
        }
      )
    )
    val latch = UnsafePromise<Unit>()
    val requests = Queue.synchronous<Option<HelloRequest>>()
    val responses = ClientCalls.bidiStreamingRpc(
      channel = channel,
      requests = requests.dequeue().terminateOnNone(),
      method = bidiStreamingSayHelloMethod
    ).produceUnbuffered()
    requests.enqueue1(Some(helloRequest("Garnet")))
    requests.enqueue1(Some(helloRequest("Amethyst")))
    requests.enqueue1(Some(helloRequest("Pearl")))
    requests.enqueue1(Some(helloRequest("Steven")))
    val fourth = ForkAndForget {
      requests.enqueue1(Some(helloRequest("Onion")))
      latch.complete(Result.success(Unit))
    }
    sleep(300.milliseconds) // wait for everything to work its way through the system
    // assertThat(latch.tryGet()).isNull()
    assertThat(responses.dequeue1().getOrElse { null }).isEqualTo(helloReply("Hello, Garnet and Amethyst"))
    fourth.join() // pulling one element allows the cycle to advance
    requests.enqueue1(Some(helloRequest("Rainbow 2.0")))
    requests.enqueue1(None)
    val helloReplyList = responses.dequeue().filterOption().toList()
    assertThat(
      helloReplyList
    ).containsExactly(
      helloReply("Hello, Pearl and Steven"), helloReply("Hello, Onion and Rainbow 2.0")
    )
  }

  @Test
  fun bidiPingPongFlowControlServerSendsMultipleResponses() = runBlocking {
    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context = context,
        descriptor = bidiStreamingSayHelloMethod,
        implementation = { requests: Stream<HelloRequest> ->
          requests.flatMap {
            Stream(
              helloReply("Hello, ${it.name}"),
              helloReply("Goodbye, ${it.name}")
            )
          }
        }
      )
    )
    val latch = UnsafePromise<Unit>()
    val requests = Queue.synchronous<Option<HelloRequest>>()
    val responses = ClientCalls.bidiStreamingRpc(
      channel = channel,
      requests = requests.dequeue().terminateOnNone(),
      method = bidiStreamingSayHelloMethod
    ).produceUnbuffered()
    requests.enqueue1(Some(helloRequest("Garnet")))
    val second = ForkConnected {
      requests.enqueue1(Some(helloRequest("Pearl")))
      latch.complete(Result.success(Unit))
    }
    sleep(200.milliseconds) // wait for everything to work its way through the system
    // assertThat(latch.tryGet()).isNull()
    assertThat(responses.dequeue1().getOrElse { null }).isEqualTo(helloReply("Hello, Garnet"))
    second.join()
    assertThat(responses.dequeue1().getOrElse { null }).isEqualTo(helloReply("Goodbye, Garnet"))
    requests.enqueue1(None)
    //responses.cancel()
  }
}
