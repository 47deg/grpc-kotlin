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
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.Stream.Companion.effect
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.stream.concurrent.Queue
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.CallOptions
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.examples.helloworld.GreeterGrpc
import io.grpc.examples.helloworld.HelloReply
import io.grpc.examples.helloworld.HelloRequest
import io.grpc.examples.helloworld.MultiHelloRequest
import io.grpc.stub.StreamObserver
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** Tests for [ClientCalls]. */
@RunWith(JUnit4::class)
class ClientCallsTest : AbstractCallsTest() {

  /**
   * Verifies that a simple unary RPC successfully returns results to a suspend function.
   */
  @Test
  fun simpleUnary(): Unit = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun sayHello(request: HelloRequest, responseObserver: StreamObserver<HelloReply>) {
        responseObserver.onNext(helloReply("Hello, ${request.name}"))
        responseObserver.onCompleted()
      }
    }

    channel = makeChannel(serverImpl)

    assertThat(
      ClientCalls.unaryRpc(
        channel = channel,
        callOptions = CallOptions.DEFAULT,
        method = sayHelloMethod,
        request = helloRequest("Cindy")
      )
    ).isEqualTo(helloReply("Hello, Cindy"))

    assertThat(
      ClientCalls.unaryRpc(
        channel = channel,
        callOptions = CallOptions.DEFAULT,
        method = sayHelloMethod,
        request = helloRequest("Jeff")
      )
    ).isEqualTo(helloReply("Hello, Jeff"))
  }

  /**
   * Verify that a unary RPC that does not respond within a timeout specified by [CallOptions]
   * fails on the client with a DEADLINE_EXCEEDED and is cancelled on the server.
   */
  @Test
  fun unaryServerDoesNotRespondGrpcTimeout(): Unit = runBlocking {
    val serverCancelled = UnsafePromise<Unit>()

    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun sayHello(request: HelloRequest, responseObserver: StreamObserver<HelloReply>) {
        whenContextIsCancelled { serverCancelled.complete(Result.success(Unit)) }
      }
    }

    channel = makeChannel(serverImpl)

    val ex = assertThrows<StatusException> {
      ClientCalls.unaryRpc(
        channel = channel,
        callOptions = CallOptions.DEFAULT.withDeadlineAfter(200, TimeUnit.MILLISECONDS),
        method = sayHelloMethod,
        request = helloRequest("Jeff")
      )
    }
    assertThat(ex.status.code).isEqualTo(Status.Code.DEADLINE_EXCEEDED)
    serverCancelled.join()
  }

  /** Verify that a server that sends two responses to a unary RPC causes an exception. */
  @Test
  fun unaryTooManyResponses() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun sayHello(request: HelloRequest, responseObserver: StreamObserver<HelloReply>) {
        responseObserver.onNext(helloReply("Hello, ${request.name}"))
        responseObserver.onNext(helloReply("It's nice to meet you, ${request.name}"))
        responseObserver.onCompleted()
      }
    }

    channel = makeChannel(serverImpl)

    // Apparently this fails with a server cancellation.
    assertThrows<Exception> {
      ClientCalls.unaryRpc(
        channel = channel,
        callOptions = CallOptions.DEFAULT,
        method = sayHelloMethod,
        request = helloRequest("Cindy")
      )
    }
    Unit
  }

  /** Verify that a server that sends zero responses to a unary RPC causes an exception. */
  @Test
  fun unaryNoResponses() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun sayHello(request: HelloRequest, responseObserver: StreamObserver<HelloReply>) {
        responseObserver.onCompleted()
      }
    }

    channel = makeChannel(serverImpl)

    // Apparently this fails with a server cancellation.
    assertThrows<Exception> {
      ClientCalls.unaryRpc(
        channel = channel,
        callOptions = CallOptions.DEFAULT,
        method = sayHelloMethod,
        request = helloRequest("Cindy")
      )
    }
    Unit
  }

  /**
   * Verify that cancelling a coroutine job that includes the RPC as a subtask propagates the
   * cancellation to the server.
   */
  @Test
  fun unaryCancelCoroutinePropagatesToServer() = runBlocking {
    // Completes if and only if the server processes cancellation.
    val serverReceived = UnsafePromise<Unit>()
    val serverCancelled = UnsafePromise<Unit>()

    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun sayHello(request: HelloRequest, responseObserver: StreamObserver<HelloReply>) {
        serverReceived.complete(Result.success(Unit))
        whenContextIsCancelled { serverCancelled.complete(Result.success(Unit)) }
      }
    }

    channel = makeChannel(serverImpl)

    val job = ForkConnected {
      ClientCalls.unaryRpc(
        channel = channel,
        callOptions = CallOptions.DEFAULT,
        method = sayHelloMethod,
        request = helloRequest("Jeff")
      )
    }
    serverReceived.join()
    job.cancel()
    serverCancelled.join()
  }

  @Test
  fun unaryServerExceptionPropagated() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun sayHello(request: HelloRequest, responseObserver: StreamObserver<HelloReply>) {
        throw IllegalArgumentException("No hello for you!")
      }
    }

    channel = makeChannel(serverImpl)

    val ex = assertThrows<StatusException> {
      ClientCalls.unaryRpc(
        channel = channel,
        callOptions = CallOptions.DEFAULT,
        method = sayHelloMethod,
        request = helloRequest("Cindy")
      )
    }
    assertThat(ex.status.code).isEqualTo(Status.Code.UNKNOWN)
  }

  @Test
  fun unaryRejectsNonUnaryMethod() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {}

    channel = makeChannel(serverImpl)

    assertThrows<IllegalArgumentException> {
      ClientCalls.unaryRpc(
        channel = channel,
        callOptions = CallOptions.DEFAULT,
        method = clientStreamingSayHelloMethod,
        request = helloRequest("Cindy")
      )
    }
    Unit
  }

  @Test
  fun serverStreamingRejectsNonServerStreamingMethod() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {}

    channel = makeChannel(serverImpl)

    assertThrows<IllegalArgumentException> {
      ClientCalls.serverStreamingRpc(
        channel = channel,
        method = sayHelloMethod,
        request = helloRequest("Cindy"),
        callOptions = CallOptions.DEFAULT
      )
    }
    Unit
  }

  @Test
  fun simpleServerStreamingRpc() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun serverStreamSayHello(
        request: MultiHelloRequest,
        responseObserver: StreamObserver<HelloReply>
      ) {
        for (name in request.nameList) {
          responseObserver.onNext(helloReply("Hello, $name"))
        }
        responseObserver.onCompleted()
      }
    }

    channel = makeChannel(serverImpl)

    val rpc: Stream<HelloReply> = ClientCalls.serverStreamingRpc(
      channel = channel,
      method = serverStreamingSayHelloMethod,
      request = multiHelloRequest("Cindy", "Jeff", "Aki")
    )

    val helloReplies = rpc.compile().toList()
    println("helloReplies: $helloReplies")
    assertThat(helloReplies).containsExactly(
      helloReply("Hello, Cindy"), helloReply("Hello, Jeff"), helloReply("Hello, Aki")
    ).inOrder()
  }

  @Test
  fun serverStreamingRpcCancellation() = runBlocking {
    val serverCancelled = ForkConnected { }
    val serverReceived = ForkConnected { }
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun serverStreamSayHello(
        request: MultiHelloRequest,
        responseObserver: StreamObserver<HelloReply>
      ) {
        effect {
          whenContextIsCancelled {
            effect {
              serverCancelled.cancel()
            }
          }
          serverReceived.cancel()
        }
        for (name in request.nameList) {
          responseObserver.onNext(helloReply("Hello, $name"))
        }
        responseObserver.onCompleted()
      }
    }

    channel = makeChannel(serverImpl)

    val rpc = ClientCalls.serverStreamingRpc(
      channel = channel,
      method = serverStreamingSayHelloMethod,
      request = multiHelloRequest("Tim", "Jim", "Pym")
    )
    assertThrows<CancellationException> {
      rpc.compile().lastOrError().let {
        serverReceived.join()
        throw CancellationException("no longer needed")
      }
    }
    serverCancelled.join()
  }

  @Test
  fun simpleClientStreamingRpc() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun clientStreamSayHello(
        responseObserver: StreamObserver<HelloReply>
      ): StreamObserver<HelloRequest> {
        return object : StreamObserver<HelloRequest> {
          private val names = mutableListOf<String>()

          override fun onNext(value: HelloRequest) {
            names += value.name
          }

          override fun onError(t: Throwable) = throw t

          override fun onCompleted() {
            responseObserver.onNext(
              helloReply(names.joinToString(prefix = "Hello, ", separator = ", "))
            )
            responseObserver.onCompleted()
          }
        }
      }
    }

    channel = makeChannel(serverImpl)

    val requests = Stream.emits(
      helloRequest("Tim"),
      helloRequest("Jim")
    )
    assertThat(
      ClientCalls.clientStreamingRpc(
        channel = channel,
        method = clientStreamingSayHelloMethod,
        requests = requests
      )
    ).isEqualTo(helloReply("Hello, Tim, Jim"))
  }

  @Test
  fun clientStreamingRpcReturnsEarly() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun clientStreamSayHello(
        responseObserver: StreamObserver<HelloReply>
      ): StreamObserver<HelloRequest> {
        return object : StreamObserver<HelloRequest> {
          private val names = mutableListOf<String>()
          private var isComplete = false

          override fun onNext(value: HelloRequest) {
            names += value.name
            if (names.size >= 2 && !isComplete) {
              onCompleted()
            }
          }

          override fun onError(t: Throwable) = throw t

          override fun onCompleted() {
            if (!isComplete) {
              responseObserver.onNext(
                helloReply(names.joinToString(prefix = "Hello, ", separator = ", "))
              )
              responseObserver.onCompleted()
              isComplete = true
            }
          }
        }
      }
    }

    channel = makeChannel(serverImpl)

    val requests = Queue.bounded<HelloRequest>(1)
    Stream.bracket({
      ForkConnected {
        ClientCalls.clientStreamingRpc(
          channel = channel,
          method = clientStreamingSayHelloMethod,
          requests = requests.dequeue()
        )
      }
    }, { response: Fiber<HelloReply> ->
      requests.tryOffer1(helloRequest("Tim"))
      requests.tryOffer1(helloRequest("Jim"))
      val helloReply = response.join()
      assertThat(helloReply).isEqualTo(helloReply("Hello, Tim, Jim"))
      try {
        requests.tryOffer1(helloRequest("John"))
      } catch (allowed: CancellationException) {
        // Either this should successfully send, or the channel should be cancelled; either is
        // acceptable.  The one unacceptable outcome would be for these operations to suspend
        // indefinitely, waiting for them to be sent.
      }
    }).compile().drain()
  }

  @Test
  fun clientStreamingRpcCancelled() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun clientStreamSayHello(
        responseObserver: StreamObserver<HelloReply>
      ): StreamObserver<HelloRequest> {
        return object : StreamObserver<HelloRequest> {
          private val names = mutableListOf<String>()

          override fun onNext(value: HelloRequest) {
            names += value.name
          }

          override fun onError(t: Throwable) = throw t

          override fun onCompleted() {
            responseObserver.onNext(
              helloReply(names.joinToString(prefix = "Hello, ", separator = ", "))
            )
            responseObserver.onCompleted()
          }
        }
      }
    }

    channel = makeChannel(serverImpl)

    val requests = Queue.bounded<HelloRequest>(1)
    Stream.bracket({
      ForkConnected {
        ClientCalls.clientStreamingRpc(
          channel = channel,
          method = clientStreamingSayHelloMethod,
          requests = requests.dequeue()
        )
      }
    }, { response: Fiber<HelloReply> ->
      requests.tryOffer1(helloRequest("Tim"))
      response.cancel()
      response.join()
      assertThrows<CancellationException> {
        requests.tryOffer1(helloRequest("John"))
      }
    }).compile().drain()
  }

  @Test
  fun simpleBidiStreamingRpc() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun bidiStreamSayHello(
        responseObserver: StreamObserver<HelloReply>
      ): StreamObserver<HelloRequest> {
        return object : StreamObserver<HelloRequest> {
          override fun onNext(value: HelloRequest) {
            responseObserver.onNext(helloReply("Hello, ${value.name}"))
          }

          override fun onError(t: Throwable) = throw t

          override fun onCompleted() {
            responseObserver.onCompleted()
          }
        }
      }
    }

    channel = makeChannel(serverImpl)

    val requests = Queue.bounded<HelloRequest>(1)
    val rpc: Queue<HelloReply> = ClientCalls.bidiStreamingRpc(
      channel = channel,
      method = bidiStreamingSayHelloMethod,
      requests = requests.dequeue()
    ).compile().toList().let { items ->
      val rpcQueue = Queue.bounded<HelloReply>(items.size)
      items.forEach { reply -> rpcQueue.enqueue1(reply) }
      rpcQueue
    }
    requests.enqueue1(helloRequest("Tim"))
    assertThat(rpc.dequeue1()).isEqualTo(helloReply("Hello, Tim"))
    requests.enqueue1(helloRequest("Jim"))
    assertThat(rpc.dequeue1()).isEqualTo(helloReply("Hello, Jim"))
    // requests.close() not needed
    assertThat(rpc.tryDequeue1().isEmpty()).isTrue()
  }

  @Test
  fun bidiStreamingRpcReturnsEarly() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun bidiStreamSayHello(
        responseObserver: StreamObserver<HelloReply>
      ): StreamObserver<HelloRequest> {
        return object : StreamObserver<HelloRequest> {
          private var responseCount = 0

          override fun onNext(value: HelloRequest) {
            responseCount++
            responseObserver.onNext(helloReply("Hello, ${value.name}"))
            if (responseCount >= 2) {
              onCompleted()
            }
          }

          override fun onError(t: Throwable) = throw t

          override fun onCompleted() {
            responseObserver.onCompleted()
          }
        }
      }
    }

    channel = makeChannel(serverImpl)

    val requests = Queue.bounded<HelloRequest>(1)
    val rpc: Queue<HelloReply> = ClientCalls.bidiStreamingRpc(
      channel = channel,
      method = bidiStreamingSayHelloMethod,
      requests = requests.dequeue()
    ).produceIn()

    requests.enqueue1(helloRequest("Tim"))
    assertThat(rpc.dequeue1()).isEqualTo(helloReply("Hello, Tim"))
    requests.enqueue1(helloRequest("Jim"))
    assertThat(rpc.dequeue1()).isEqualTo(helloReply("Hello, Jim"))
    assertThat(rpc.tryDequeue1().isEmpty()).isTrue() // rpc closes responses
    // TODO: this doesn't make sense for Queue since cant be closed or cancelled, does it?
//    try {
//      requests.send(helloRequest("John"))
//    } catch (allowed: CancellationException) {
//      // Either this should successfully send, or the channel should be cancelled; either is
//      // acceptable.  The one unacceptable outcome would be for these operations to suspend
//      // indefinitely, waiting for them to be sent.
//    }
  }

  @Test
  fun bidiStreamingRpcRequestsFail() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun bidiStreamSayHello(
        responseObserver: StreamObserver<HelloReply>
      ): StreamObserver<HelloRequest> {
        return object : StreamObserver<HelloRequest> {
          override fun onNext(value: HelloRequest) {
            responseObserver.onNext(helloReply("Hello, ${value.name}"))
          }

          override fun onError(t: Throwable) = throw t

          override fun onCompleted() {
            responseObserver.onCompleted()
          }
        }
      }
    }
    channel = makeChannel(serverImpl)

    val responses: Stream<HelloReply> = ClientCalls.bidiStreamingRpc(
      channel = channel,
      method = bidiStreamingSayHelloMethod,
      requests = Stream.raiseError(MyException())
    )

    assertThrows<MyException> {
      responses.compile().drain()
    }
  }

  private class MyException : Exception()

  @Test
  fun bidiStreamingRpcCollectsRequestsEachTime() = runBlocking {
    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
      override fun bidiStreamSayHello(
        responseObserver: StreamObserver<HelloReply>
      ): StreamObserver<HelloRequest> {
        return object : StreamObserver<HelloRequest> {
          override fun onNext(value: HelloRequest) {
            responseObserver.onNext(helloReply("Hello, ${value.name}"))
          }

          override fun onError(t: Throwable) = throw t

          override fun onCompleted() {
            responseObserver.onCompleted()
          }
        }
      }
    }
    channel = makeChannel(serverImpl)

    val requestsEvaluations = AtomicInteger()
    val requests = effect<HelloRequest> {
      requestsEvaluations.incrementAndGet()
      helloRequest("Sunstone")
    }

    val responses: Stream<HelloReply> = ClientCalls.bidiStreamingRpc(
      channel = channel,
      method = bidiStreamingSayHelloMethod,
      requests = requests
    )

    assertThat(responses.take(1).compile().lastOrError()).isEqualTo(helloReply("Hello, Sunstone"))
    assertThat(responses.take(1).compile().lastOrError()).isEqualTo(helloReply("Hello, Sunstone"))
    assertThat(requestsEvaluations.get()).isEqualTo(2)
  }
}
