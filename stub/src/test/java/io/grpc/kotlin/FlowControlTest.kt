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

import arrow.fx.coroutines.Fiber
import arrow.fx.coroutines.ForkConnected
import arrow.fx.coroutines.milliseconds
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.stream.concurrent.Queue
import com.google.common.truth.Truth.assertThat
import io.grpc.examples.helloworld.HelloReply
import io.grpc.examples.helloworld.HelloRequest
import kotlinx.coroutines.CoroutineName
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

/** Tests for the flow control of the Kotlin gRPC APIs. */
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
        implementation = { requests -> requests.map { helloReply("Hello, ${it.name}") } }
      )
    )
    val requests = Queue.unsafeUnbounded<HelloRequest>()
    val responses: Queue<HelloReply> =
      ClientCalls.bidiStreamingRpc(
        channel = channel,
        requests = requests.dequeue(),
        method = bidiStreamingSayHelloMethod
      ).produceUnbuffered()
    requests.tryOffer1(helloRequest("Garnet"))
    requests.tryOffer1(helloRequest("Amethyst"))
    val third: Fiber<Boolean> = ForkConnected { requests.tryOffer1(helloRequest("Steven")) }
    Stream.unit.delayBy(200.milliseconds) // delay(200)? // wait for everything to work its way through the system
    // assertThat(third.isCompleted).isFalse() how to check state
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet"))
    third.join() // pulling one element allows the cycle to advance
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
    requests.tryOffer1(helloRequest("Garnet"))
    requests.tryOffer1(helloRequest("Amethyst"))
    requests.tryOffer1(helloRequest("Pearl"))
    val fourth = ForkConnected { requests.tryOffer1(helloRequest("Pearl")) }
    Stream.unit.delayBy(200.milliseconds) // delay(200)? // wait for everything to work its way through the system
    // assertThat(fourth.isCompleted).isFalse()
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet"))
    fourth.join() // pulling one element allows the cycle to advance
    // responses.cancel()
  }

  @Test
  fun bidiPingPongFlowControlServerDrawsMultipleRequests() = runBlocking {
//    suspend fun <T : Any> Stream<T>.zipOdds(): Stream<Pair<T, T>> {
//      fun go(last: T, s: Pull<T, Unit>): Pull<Pair<T, T>, Unit> =
//        s.unconsOrNull().flatMap { uncons ->
//          when (uncons) {
//            null -> Pull.done
//            else -> {
//              val (newLast: T, out: Chunk<Pair<T, T>>) = uncons.head.mapAccumulate(last) { prev, next ->
//                Pair(next, Pair(prev, next))
//              }
//              Pull.output(out).flatMap { go(newLast, uncons.tail) }
//            }
//          }
//        }
//
//      return asPull().uncons1OrNull().flatMap { uncons1: PullUncons1<T>? ->
//        when (uncons1) {
//          null -> Pull.done
//          else -> go(uncons1.head, uncons1.tail)
//        }
//      }.stream()
//    }

    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context = context,
        descriptor = bidiStreamingSayHelloMethod,
        implementation = { requests: Stream<HelloRequest> ->
          requests.zipWithNext().map { (a, b) -> helloReply("Hello, ${a.name} and ${b?.name}") }
        }
      )
    )
    val requests = Queue.unsafeUnbounded<HelloRequest>()
    val responses = ClientCalls.bidiStreamingRpc(
      channel = channel,
      requests = requests.dequeue(),
      method = bidiStreamingSayHelloMethod
    ).produceUnbuffered()
    requests.tryOffer1(helloRequest("Garnet"))
    requests.tryOffer1(helloRequest("Amethyst"))
    requests.tryOffer1(helloRequest("Pearl"))
    requests.tryOffer1(helloRequest("Steven"))
    val fourth = ForkConnected { requests.tryOffer1(helloRequest("Onion")) }
    Stream.unit.delayBy(300.milliseconds) // delay(300)? // wait for everything to work its way through the system
    //assertThat(fourth.isCompleted).isFalse()
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet and Amethyst"))
    fourth.join() // pulling one element allows the cycle to advance
    requests.tryOffer1(helloRequest("Rainbow 2.0"))
    //requests.close()
    assertThat(
      responses.dequeue().compile().toList()
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
    val requests = Queue.unsafeUnbounded<HelloRequest>()
    val responses = ClientCalls.bidiStreamingRpc(
      channel = channel,
      requests = requests.dequeue(),
      method = bidiStreamingSayHelloMethod
    ).produceUnbuffered()
    requests.tryOffer1(helloRequest("Garnet"))
    val second = ForkConnected { requests.tryOffer1(helloRequest("Pearl")) }
    Stream.unit.delayBy(200.milliseconds) // delay(200)? // wait for everything to work its way through the system
    //assertThat(second.isCompleted).isFalse()
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet"))
    second.join()
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Goodbye, Garnet"))
    //responses.cancel()
  }
}
