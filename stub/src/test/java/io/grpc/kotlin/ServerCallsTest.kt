/*
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ForkConnected
import arrow.fx.coroutines.IOPool
import arrow.fx.coroutines.Promise
import arrow.fx.coroutines.evalOn
import arrow.fx.coroutines.guaranteeCase
import arrow.fx.coroutines.milliseconds
import arrow.fx.coroutines.never
import arrow.fx.coroutines.sleep
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.terminateOnNone
import com.google.common.truth.Truth.assertThat
import io.grpc.CallOptions
import io.grpc.ClientCall
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.grpc.examples.helloworld.GreeterGrpc
import io.grpc.examples.helloworld.HelloReply
import io.grpc.examples.helloworld.HelloRequest
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext


data class CoroutineName(val name: String) : AbstractCoroutineContextElement(CoroutineName) {
  companion object : CoroutineContext.Key<CoroutineName>
}

@RunWith(JUnit4::class)
class ServerCallsTest : AbstractCallsTest() {

  val context = CoroutineName("server context")

  @Test // works
  fun simpleUnaryMethod() = runBlocking {
    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(context, sayHelloMethod) { request: HelloRequest ->
        helloReply("Hello, ${request.name}")
      }
    )

    val stub = GreeterGrpc.newBlockingStub(channel)

    assertThat(stub.sayHello(helloRequest("Steven"))).isEqualTo(helloReply("Hello, Steven"))
    assertThat(stub.sayHello(helloRequest("Pearl"))).isEqualTo(helloReply("Hello, Pearl"))
  }

  @Test // works
  fun unaryMethodCancellationPropagatedToServer() = runBlocking {
    val request = Promise<HelloRequest>()
    val cancelled = Promise<ExitCase>()

    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(context, sayHelloMethod) { r: HelloRequest ->
        request.complete(r)
        guaranteeCase({ never<HelloReply>() }) { case ->
          cancelled.complete(case)
        }
      }
    )

    val expected = helloRequest("Garnet")

    val stub = GreeterGrpc.newFutureStub(channel)

    val future = stub.sayHello(expected)

    assertThat(request.get()).isEqualTo(expected)

    future.cancel(true)

    assertThat(cancelled.get()).isEqualTo(ExitCase.Cancelled)
  }

  // Failing due to it was closed before calling: clientCall.halfClose()
  @Test
  fun unaryRequestHandledWithoutWaitingForHalfClose() = runBlocking {
    val processingStarted = Promise<Unit>()
    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(context, sayHelloMethod) {
        processingStarted.complete(Unit)
        helloReply("Hello!")
      }
    )

    val clientCall = channel.newCall(sayHelloMethod, CallOptions.DEFAULT)
    val response = UnsafePromise<HelloReply>()
    val closeStatus = UnsafePromise<Status>()

    clientCall.start(object : ClientCall.Listener<HelloReply>() {
      override fun onMessage(message: HelloReply) {
        println("onMessage: $message")
        response.complete(Result.success(message))
      }

      override fun onClose(status: Status, trailers: Metadata?) {
        println("onClose: $status, $trailers")
        closeStatus.complete(Result.success(status))
      }
    }, Metadata())

    clientCall.sendMessage(helloRequest(""))
    clientCall.request(1)

    processingStarted.get()

    val responseResult = response.join()

    assertThat(responseResult).isEqualTo(helloReply("Hello!"))

    sleep(200.milliseconds)

    assertThat(closeStatus.isEmpty()).isTrue()

    clientCall.halfClose()

    val closeStatusResult = closeStatus.join()

    assertThat(closeStatusResult.code).isEqualTo(Status.Code.OK)
  }

  // Failing due to it was closed before calling: clientCall.halfClose()
  @Test
  fun unaryMethodReceivedTooManyRequests() = runBlocking {
    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(context, sayHelloMethod) {
        helloReply("Hello, ${it.name}")
      }
    )

    val call = channel.newCall(sayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()

    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )

    call.request(1)

    call.sendMessage(helloRequest("Amethyst"))
    call.sendMessage(helloRequest("Pearl"))

    call.halfClose()

    val status = closeStatus.join()

    assertThat(status.code).isEqualTo(Status.Code.INTERNAL)
    assertThat(status.description).contains("received two")
  }

  @Test
  fun unaryMethodFailedWithStatusWithTrailers() = runBlocking {
    val key: Metadata.Key<String> = Metadata.Key.of("key", Metadata.ASCII_STRING_MARSHALLER)

    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(context, sayHelloMethod) {
        val trailers = Metadata()
        trailers.put(key, "value")
        throw StatusException(Status.DATA_LOSS, trailers)
      }
    )

    val call = channel.newCall(sayHelloMethod, CallOptions.DEFAULT)
    val closeTrailers = UnsafePromise<Metadata?>()

    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeTrailers.complete(Result.success(trailers))
        }
      },
      Metadata()
    )

    call.request(1)
    call.sendMessage(helloRequest("Garnet"))
    call.halfClose()

    val metadata = closeTrailers.join()
    assertThat(metadata?.get(key)).isEqualTo("value")
  }

  @Test
  fun unaryMethodReceivedNoRequests() = runBlocking {
    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(context, sayHelloMethod) {
        helloReply("Hello, ${it.name}")
      }
    )

    val call = channel.newCall(sayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()

    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )

    call.request(1)
    call.halfClose()

    val status = closeStatus.join()

    assertThat(status.code).isEqualTo(Status.Code.INTERNAL)
    assertThat(status.description).contains("received none")
  }

  @Test
  fun unaryMethodThrowsStatusException() = runBlocking {
    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(context, sayHelloMethod) {
        throw StatusException(Status.OUT_OF_RANGE)
      }
    )

    val stub = GreeterGrpc.newBlockingStub(channel)
    val ex = assertThrows<StatusRuntimeException> {
      stub.sayHello(helloRequest("Peridot"))
    }

    assertThat(ex.status.code).isEqualTo(Status.Code.OUT_OF_RANGE)
  }

  class MyException : Exception()

  @Test // debugging works, race condition?
  fun unaryMethodThrowsException() = runBlocking {
    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(context, sayHelloMethod) {
        throw MyException()
      }
    )

    val stub = GreeterGrpc.newBlockingStub(channel)
    val ex = assertThrows<StatusRuntimeException> {
      stub.sayHello(helloRequest("Lapis Lazuli"))
    }
    println("ex.status.code: ${ex.status.code}")
    assertThat(ex.status.code).isEqualTo(Status.Code.UNKNOWN)
  }

  @Test // debugging works, race condition?
  fun simpleServerStreaming() = runBlocking {
    val channel = makeChannel(
      ServerCalls.serverStreamingServerMethodDefinition(context, serverStreamingSayHelloMethod) {
        Stream.iterable(it.nameList).map { helloReply("Hello, $it") }
      }
    )

    val responses: Stream<HelloReply> = ClientCalls.serverStreamingRpc(
      channel,
      serverStreamingSayHelloMethod,
      multiHelloRequest("Garnet", "Amethyst", "Pearl")
    )
    val result = responses.compile().toList()
    assertThat(result)
      .containsExactly(
        helloReply("Hello, Garnet"),
        helloReply("Hello, Amethyst"),
        helloReply("Hello, Pearl")
      ).inOrder()
  }

  @Test // ExitCase is correct (ExitCase.Cancelled) but never ends
  fun serverStreamingCancellationPropagatedToServer() = runBlocking {
    val requestReceived = Promise<Unit>()
    val cancelled = Promise<ExitCase>()
    val channel = makeChannel(
      ServerCalls.serverStreamingServerMethodDefinition(
        context,
        serverStreamingSayHelloMethod
      ) {
        Stream.effect {
          requestReceived.complete(Unit)
          guaranteeCase({ never<HelloReply>() }) { case ->
            cancelled.complete(case)
          }
        }
      }
    )

    val call = channel.newCall(serverStreamingSayHelloMethod, CallOptions.DEFAULT)

    val closeStatus = UnsafePromise<Status>()

    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )

    call.sendMessage(multiHelloRequest("Steven"))
    call.halfClose()

    requestReceived.get()
    call.cancel("Test cancellation", null)

    val cancelledResult = cancelled.get()
    assertThat(cancelledResult).isEqualTo(ExitCase.Cancelled)

    val status = closeStatus.join()
    assertThat(status.code).isEqualTo(Status.Code.CANCELLED)
  }

  @Test
  fun serverStreamingThrowsStatusException() = runBlocking {
    val channel = makeChannel(
      ServerCalls.serverStreamingServerMethodDefinition(
        context,
        serverStreamingSayHelloMethod
      ) { Stream.raiseError(StatusException(Status.OUT_OF_RANGE)) }
    )

    val call = channel.newCall(serverStreamingSayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()
    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )

    // serverStreamingMethodDefinition waits until the client has definitely sent exactly one
    // message before executing the implementation, so we have to halfClose
    call.sendMessage(multiHelloRequest("Steven"))
    call.halfClose()

    val status = closeStatus.join()
    assertThat(status.code).isEqualTo(Status.Code.OUT_OF_RANGE)
  }

  // TODO FIX
//  @Test
//  fun serverStreamingHandledWithoutWaitingForHalfClose() = runBlocking {
//    val processingStarted = UnsafePromise<Unit>()
//
//    val channel: ManagedChannel = makeChannel(
//      ServerCalls.serverStreamingServerMethodDefinition(context, serverStreamingSayHelloMethod) {
//        processingStarted.complete(Result.success(Unit))
//        Stream.iterable(it.nameList).map { helloReply("Hello, $it") }
//      }
//    )
//
//    val clientCall = channel.newCall(serverStreamingSayHelloMethod, CallOptions.DEFAULT)
//    val responseChannel = Queue.unbounded<HelloReply>()
//
//    clientCall.start(object : ClientCall.Listener<HelloReply>() {
//      override fun onMessage(message: HelloReply) {
//        responseChannel.tryOffer1(message)
//      }
//
//      override fun onClose(status: Status, trailers: Metadata?) {
//        // no need to close it: responseChannel.close()
//      }
//    }, Metadata())
//
//    clientCall.sendMessage(multiHelloRequest("Ruby", "Sapphire"))
//    clientCall.request(2)
//
//    processingStarted.join()
//
//    assertThat(responseChannel.dequeue1()).isEqualTo(helloReply("Hello, Ruby"))
//    assertThat(responseChannel.dequeue1()).isEqualTo(helloReply("Hello, Sapphire"))
//
//    sleep(200.milliseconds)
//
//    // assertThat(responseChannel.isClosedForReceive).isFalse()
//
//    clientCall.halfClose()
//    assertThat(responseChannel.tryDequeue1()).isEqualTo(None) // closed with no further responses
//  }

  @Test
  fun serverStreamingThrowsException() = runBlocking {
    val channel = makeChannel(
      ServerCalls.serverStreamingServerMethodDefinition(
        context,
        serverStreamingSayHelloMethod
      ) { throw MyException() }
    )

    val call = channel.newCall(serverStreamingSayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()

    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )

    // serverStreamingMethodDefinition waits until the client has definitely sent exactly one
    // message before executing the implementation, so we have to halfClose
    call.sendMessage(multiHelloRequest("Steven"))
    call.halfClose()

    val status = closeStatus.join()
    assertThat(status.code).isEqualTo(Status.Code.UNKNOWN)
  }

  @Test
  fun simpleClientStreaming() = runBlocking {
    val channel = makeChannel(
      ServerCalls.clientStreamingServerMethodDefinition(
        context,
        clientStreamingSayHelloMethod
      ) { requests: Stream<HelloRequest> ->
        helloReply(
          requests
            .compile()
            .toList().joinToString(separator = ", ", prefix = "Hello, ") { it.name }
        )
      }
    )

    val requestChannel = Stream(
      helloRequest("Ruby"),
      helloRequest("Sapphire")
    )

    assertThat(
      ClientCalls.clientStreamingRpc(
        channel,
        clientStreamingSayHelloMethod,
        requestChannel
      )
    ).isEqualTo(helloReply("Hello, Ruby, Sapphire"))
  }

  @Test
  fun clientStreamingDoesntWaitForAllRequests() = runBlocking {
    val channel = makeChannel(
      ServerCalls.clientStreamingServerMethodDefinition(
        context,
        clientStreamingSayHelloMethod
      ) { requests ->
        val (req1, req2) = requests.take(2).compile().toList()
        helloReply("Hello, ${req1.name} and ${req2.name}")
      }
    )

    val requests = Stream(
      helloRequest("Peridot"),
      helloRequest("Lapis"),
      helloRequest("Jasper"),
      helloRequest("Aquamarine")
    )
    assertThat(
      ClientCalls.clientStreamingRpc(
        channel,
        clientStreamingSayHelloMethod,
        requests
      )
    ).isEqualTo(helloReply("Hello, Peridot and Lapis"))
  }

  @Test
  fun clientStreamingWhenRequestsCancelledNoBackpressure() = runBlocking {
    val latch = Promise<Unit>()

    val channel = makeChannel(
      ServerCalls.clientStreamingServerMethodDefinition(
        context,
        clientStreamingSayHelloMethod
      ) { requests ->
        val (req1, req2) = requests.take(2).compile().toList()
        latch.get()
        helloReply("Hello, ${req1.name} and ${req2.name}")
      }
    )

    val requestChannel = Queue.bounded<HelloRequest>(1)
    val response = ForkConnected {
      ClientCalls.clientStreamingRpc(
        channel,
        clientStreamingSayHelloMethod,
        requestChannel.dequeue()
      )
    }
    requestChannel.enqueue1(helloRequest("Lapis"))
    requestChannel.enqueue1(helloRequest("Peridot"))

    for (i in 1..1000) {
      requestChannel.enqueue1(helloRequest("Ruby"))
    }

    latch.complete(Unit)
    assertThat(response.join()).isEqualTo(helloReply("Hello, Lapis and Peridot"))
  }

  @Test // executed individually it works :S
  fun clientStreamingCancellationPropagatedToServer() = runBlocking {
    val requestReceived = Promise<Unit>()
    val cancelled = Promise<ExitCase>()

    val channel = makeChannel(
      ServerCalls.clientStreamingServerMethodDefinition(
        context,
        clientStreamingSayHelloMethod
      ) { requests: Stream<HelloRequest> ->
        requests.effectMap {
          requestReceived.complete(Unit)
          guaranteeCase({ never<Unit>() }) { case ->
            cancelled.complete(case)
          }
        }.compile().drain()

        helloReply("Impossible?")
      }
    )

    val call = channel.newCall(clientStreamingSayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()
    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )
    call.sendMessage(helloRequest("Steven"))

    requestReceived.get()
    call.cancel("Test cancellation", null)
    assertThat(cancelled.get()).isEqualTo(ExitCase.Cancelled)

    val result = closeStatus.join()
    assertThat(result.code).isEqualTo(Status.Code.CANCELLED)
  }

  @Test
  fun clientStreamingThrowsStatusException() = runBlocking {
    val channel = makeChannel(
      ServerCalls.clientStreamingServerMethodDefinition(
        context,
        clientStreamingSayHelloMethod
      ) { throw StatusException(Status.INVALID_ARGUMENT) }
    )

    val call = channel.newCall(clientStreamingSayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()

    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )

    call.sendMessage(helloRequest("Steven"))

    val result = closeStatus.join()
    assertThat(result.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun clientStreamingThrowsException() = runBlocking {
    val channel = makeChannel(
      ServerCalls.clientStreamingServerMethodDefinition(
        context,
        clientStreamingSayHelloMethod
      ) {
        throw MyException()
      }
    )

    val call = channel.newCall(clientStreamingSayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()

    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )

    call.sendMessage(helloRequest("Steven"))

    val result = closeStatus.join()
    assertThat(result.code).isEqualTo(Status.Code.UNKNOWN)
  }

  @Test
  fun simpleBidiStreamingPingPong() = runBlocking {
    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(context, bidiStreamingSayHelloMethod) { requests ->
        requests.map { helloReply("Hello, ${it.name}") }.onFinalize { Stream.just(helloReply("Goodbye")) }
      }
    )

    val requests = Queue.unbounded<Option<HelloRequest>>()
    val responses: Queue<HelloReply> =
      ClientCalls.bidiStreamingRpc(channel, bidiStreamingSayHelloMethod, requests.dequeue().terminateOnNone())
        .produceIn()

    requests.enqueue1(Some(helloRequest("Garnet")))
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet"))
    requests.enqueue1(Some(helloRequest("Steven")))
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Steven"))
    requests.enqueue1(None)

    assertThat(responses.dequeue1()).isEqualTo(helloReply("Goodbye"))
    assertThat(responses.tryDequeue1()).isEqualTo(None)
  }

  @Test
  fun bidiStreamingCancellationPropagatedToServer() = runBlocking {
    val requestReceived = Promise<Unit>()
    val cancelled = Promise<ExitCase>()

    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context,
        bidiStreamingSayHelloMethod
      ) { requests: Stream<HelloRequest> ->
        requests.effectMap {
          requestReceived.complete(Unit)

          guaranteeCase({ never<HelloReply>() }) { case ->
            cancelled.complete(case)
          }
        }
      }
    )

    val call = channel.newCall(bidiStreamingSayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()
    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )
    call.sendMessage(helloRequest("Steven"))
    requestReceived.get()
    call.cancel("Test cancellation", null)
    assertThat(cancelled.get()).isEqualTo(ExitCase.Cancelled)

    val result = closeStatus.join()
    assertThat(result.code).isEqualTo(Status.Code.CANCELLED)
  }

  @Test
  fun bidiStreamingThrowsStatusException() = runBlocking {
    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context,
        bidiStreamingSayHelloMethod
      ) { Stream.raiseError(StatusException(Status.INVALID_ARGUMENT)) }
    )

    val call = channel.newCall(bidiStreamingSayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()

    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )
    call.sendMessage(helloRequest("Steven"))
    val result = closeStatus.join()

    assertThat(result.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun bidiStreamingThrowsException() = runBlocking {
    val channel = makeChannel(
      ServerCalls.bidiStreamingServerMethodDefinition(
        context,
        bidiStreamingSayHelloMethod
      ) { throw MyException() }
    )

    val call = channel.newCall(bidiStreamingSayHelloMethod, CallOptions.DEFAULT)
    val closeStatus = UnsafePromise<Status>()
    call.start(
      object : ClientCall.Listener<HelloReply>() {
        override fun onClose(status: Status, trailers: Metadata?) {
          closeStatus.complete(Result.success(status))
        }
      },
      Metadata()
    )

    call.sendMessage(helloRequest("Steven"))

    val result = closeStatus.join()
    assertThat(result.code).isEqualTo(Status.Code.UNKNOWN)
  }

  @Test
  fun rejectNonUnaryMethod() = runBlocking {
    assertThrows<IllegalArgumentException> {
      ServerCalls.unaryServerMethodDefinition(context, bidiStreamingSayHelloMethod) { TODO() }
    }
  }

  @Test
  fun rejectNonClientStreamingMethod() = runBlocking {
    assertThrows<IllegalArgumentException> {
      ServerCalls
        .clientStreamingServerMethodDefinition(context, sayHelloMethod) { TODO() }
    }
  }

  @Test
  fun rejectNonServerStreamingMethod() = runBlocking {
    assertThrows<IllegalArgumentException> {
      ServerCalls
        .serverStreamingServerMethodDefinition(context, sayHelloMethod) { TODO() }
    }
  }

  @Test
  fun rejectNonBidiStreamingMethod() = runBlocking {
    assertThrows<IllegalArgumentException> {
      ServerCalls
        .bidiStreamingServerMethodDefinition(context, sayHelloMethod) { TODO() }
    }
  }

  @Test
  fun unaryContextPropagated() = runBlocking {
    val differentThreadContext: CoroutineContext = IOPool
    val contextKey = Context.key<String>("testKey")
    val contextToInject = Context.ROOT.withValue(contextKey, "testValue")

    val interceptor = object : ServerInterceptor {
      override fun <RequestT, ResponseT> interceptCall(
        call: ServerCall<RequestT, ResponseT>,
        headers: Metadata,
        next: ServerCallHandler<RequestT, ResponseT>
      ): ServerCall.Listener<RequestT> {
        return Contexts.interceptCall(
          contextToInject,
          call,
          headers,
          next
        )
      }
    }

    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(context, sayHelloMethod) {
        evalOn(differentThreadContext) {
          // Run this in a definitely different thread, just to verify context propagation
          // is WAI.
          assertThat(contextKey.get(Context.current())).isEqualTo("testValue")
          helloReply("Hello, ${it.name}")
        }
      },
      interceptor
    )

    val stub = GreeterGrpc.newBlockingStub(channel)
    assertThat(stub.sayHello(helloRequest("Peridot"))).isEqualTo(helloReply("Hello, Peridot"))
  }

//  @Test
//  fun serverStreamingFlowControl() = runBlocking {
//    val receiveFirstMessage = ForkConnected { }
//    val receivedFirstMessage = ForkConnected { }
//    val channel: ManagedChannel = makeChannel(
//      ServerCalls.serverStreamingServerMethodDefinition(
//        EmptyCoroutineContext,
//        serverStreamingSayHelloMethod
//      ) {
//        Stream.cancellable<HelloReply> {
//          val queue = Queue.unbounded<HelloReply>()
//          queue.enqueue1(helloReply("1st"))
//          queue.enqueue1(helloReply("2nd"))
//          val thirdSend: Fiber<Unit> = ForkConnected {
//            queue.enqueue1(helloReply("3rd"))
//          }
//          sleep(200.milliseconds)
//          // assertThat(thirdSend.isCompleted).isFalse() // Fiber doesn't have isCompleted flag
//          receiveFirstMessage.cancel()
//          receivedFirstMessage.join()
//          thirdSend.join()
//          queue.dequeue()
//
//          CancelToken { }
//
//        }.buffer(0) // Channel.RENDEZVOUS, see [kotlinx.coroutines.channels.RendezvousChannel]
//      }
//    )
//
//    val responses = ClientCalls.serverStreamingRpc(
//      channel,
//      serverStreamingSayHelloMethod,
//      multiHelloRequest()
//    )
//    receiveFirstMessage.join()
//    val helloReply1st = responses.take(1).compile().lastOrError()
//    assertThat(helloReply1st).isEqualTo(helloReply("1st"))
//    receivedFirstMessage.cancel()
//    assertThat(
//      responses.compile().toList()
//    ).containsExactly(helloReply("2nd"), helloReply("3rd"))
//  }

  @Test
  fun contextPreservation() = runBlocking {
    val contextKey = Context.key<String>("foo")
    val channel = makeChannel(
      ServerCalls.unaryServerMethodDefinition(
        context,
        sayHelloMethod
      ) {
        assertThat(contextKey.get()).isEqualTo("bar")
        helloReply("Hello!")
      },
      object : ServerInterceptor {
        override fun <ReqT, RespT> interceptCall(
          call: ServerCall<ReqT, RespT>,
          headers: Metadata,
          next: ServerCallHandler<ReqT, RespT>
        ): ServerCall.Listener<ReqT> =
          Contexts.interceptCall(
            Context.current().withValue(contextKey, "bar"),
            call,
            headers,
            next
          )
      }
    )

    val response = ClientCalls.unaryRpc(channel, sayHelloMethod, helloRequest(""))
    assertThat(response).isEqualTo(helloReply("Hello!"))
  }
}
