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
import arrow.fx.coroutines.ForkConnected
import arrow.fx.coroutines.milliseconds
import arrow.fx.coroutines.sleep
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.terminateOnNone
import com.google.common.truth.Truth.assertThat
import io.grpc.examples.helloworld.HelloReply
import io.grpc.examples.helloworld.HelloRequest
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.concurrent.CancellationException

/** Tests for the flow control of the Kotlin gRPC APIs. */
@Ignore
@RunWith(JUnit4::class)
class FlowControlTest : AbstractCallsTest() {
  val context = CoroutineName("server context")

  private suspend fun <T> Stream<T>.produceUnbuffered(): Queue<T> =
    produceIn()

  @Test
  fun bidiPingPongFlowControl() = runBlocking {
    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context = context,
        descriptor = bidiStreamingSayHelloMethod,
        implementation = { requests ->
          requests.map { helloReply("Hello, ${it.name}") }
        }
      )
    )
    val latch = UnsafePromise<Unit>()
    val requests = Queue.unsafeUnbounded<Option<HelloRequest>>()
    val responses: Queue<HelloReply> =
      ClientCalls.bidiStreamingRpc(
        channel = channel,
        requests = requests.dequeue().terminateOnNone()
          .close { latch.tryGet() },
        method = bidiStreamingSayHelloMethod
      ).produceUnbuffered()
    requests.enqueue1(Some(helloRequest("Garnet")))
    requests.enqueue1(Some(helloRequest("Amethyst")))
    val third = ForkConnected { requests.enqueue1(Some(helloRequest("Steven"))) }
    sleep(200.milliseconds)
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet"))
    third.join() // pulling one element allows the cycle to advance
    latch.complete(Result.failure(CancellationException()))
    //responses.cancel()
  }

  @Test
  fun bidiPingPongFlowControlExpandedServerBuffer() = runBlocking {
    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context = context,
        descriptor = bidiStreamingSayHelloMethod,
        implementation = { requests ->
          // TODO: Channel.RENDEZVOUS ==? 0
          requests.buffer(0).map { helloReply("Hello, ${it.name}") }
        }
      )
    )
    val requests = Queue.unsafeUnbounded<HelloRequest>()
    val responses = ClientCalls.bidiStreamingRpc(
      channel = channel,
      requests = requests.dequeue(),
      method = bidiStreamingSayHelloMethod
    ).produceUnbuffered()
    requests.enqueue1(helloRequest("Garnet"))
    requests.enqueue1(helloRequest("Amethyst"))
    requests.enqueue1(helloRequest("Pearl"))
    val fourth = ForkConnected { requests.enqueue1(helloRequest("Pearl")) }
    sleep(200.milliseconds)
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet"))
    fourth.join() // pulling one element allows the cycle to advance
  }

  @Test
  fun bidiPingPongFlowControlServerDrawsMultipleRequests() = runBlocking {

    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context = context,
        descriptor = bidiStreamingSayHelloMethod,
        implementation = { requests: Stream<HelloRequest> ->
          // how to pair streams in Pairs of 2, .chunkN(2)?
          requests.zipWithNext().map { (a, b) -> helloReply("Hello, ${a.name} and ${b?.name}") }
        }
      )
    )
    val requests = Queue.unsafeUnbounded<Option<HelloRequest>>()
    val responses = ClientCalls.bidiStreamingRpc(
      channel = channel,
      requests = requests.dequeue().terminateOnNone(),
      method = bidiStreamingSayHelloMethod
    ).produceUnbuffered()
    requests.enqueue1(Some(helloRequest("Garnet")))
    requests.enqueue1(Some(helloRequest("Amethyst")))
    requests.enqueue1(Some(helloRequest("Pearl")))
    requests.enqueue1(Some(helloRequest("Steven")))
    val fourth = ForkConnected { requests.enqueue1(Some(helloRequest("Onion"))) }
    sleep(300.milliseconds)
    //assertThat(fourth.isCompleted).isFalse()
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet and Amethyst"))
    fourth.join() // pulling one element allows the cycle to advance
    requests.enqueue1(Some(helloRequest("Rainbow 2.0")))
    requests.tryOffer1(None)
    val helloReplyList = responses.dequeue().compile().toList()
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
    val requests = Queue.unsafeUnbounded<Option<HelloRequest>>()

    val latch = UnsafePromise<Unit>()

    val responses = ClientCalls.bidiStreamingRpc(
      channel = channel,
      requests = requests
        .dequeue()
        .terminateOnNone()
        .close { latch.tryGet() },
      method = bidiStreamingSayHelloMethod
    ).produceUnbuffered()
    requests.enqueue1(Some(helloRequest("Garnet")))
    val second = ForkConnected { requests.enqueue1(Some(helloRequest("Pearl"))) }
    sleep(200.milliseconds)
    //assertThat(second.isCompleted).isFalse()
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet"))
    second.join()
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Goodbye, Garnet"))
    latch.complete(Result.failure(CancellationException()))
    //responses.cancel()
  }
}
