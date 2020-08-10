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

import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.ForkConnected
import arrow.fx.coroutines.Promise
import arrow.fx.coroutines.guaranteeCase
import arrow.fx.coroutines.never
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.stream.concurrent.Queue
import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.examples.helloworld.GreeterGrpc
import io.grpc.examples.helloworld.GreeterGrpcKt
import io.grpc.examples.helloworld.GreeterGrpcKt.GreeterArrowCoroutineStub
import io.grpc.examples.helloworld.GreeterGrpcKt.GreeterCoroutineImplBase
import io.grpc.examples.helloworld.HelloReply
import io.grpc.examples.helloworld.HelloRequest
import io.grpc.examples.helloworld.MultiHelloRequest
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.concurrent.CancellationException
import java.util.concurrent.TimeUnit

@RunWith(JUnit4::class)
class GeneratedCodeTest : AbstractCallsTest() {

  @Test
  fun simpleUnary() {
    val server = object : GreeterCoroutineImplBase() {
      override suspend fun sayHello(request: HelloRequest): HelloReply {
        return HelloReply.newBuilder()
          .setMessage("Hello, ${request.name}!")
          .build()
      }
    }
    val channel = makeChannel(server)
    val stub = GreeterArrowCoroutineStub(channel)

    runBlocking {
      assertThat(stub.sayHello(helloRequest("Steven")))
        .isEqualTo(helloReply("Hello, Steven!"))
    }
  }

  @Test
  fun unaryServerDoesNotRespondGrpcTimeout() = runBlocking {
    val serverCancelled = Promise<ExitCase>()

    val channel = makeChannel(object : GreeterCoroutineImplBase() {
      override suspend fun sayHello(request: HelloRequest): HelloReply {
        return guaranteeCase({ never<HelloReply>() }) { case ->
          serverCancelled.complete(case)
        }
      }
    })

    val stub = GreeterArrowCoroutineStub(channel).withDeadlineAfter(100, TimeUnit.MILLISECONDS)

    val ex = assertThrows<StatusException> {
      stub.sayHello(helloRequest("Topaz"))
    }
    assertThat(ex.status.code).isEqualTo(Status.Code.DEADLINE_EXCEEDED)
    serverCancelled.get()
  }

  @Test
  fun unaryClientCancellation() = runBlocking {
    val helloReceived = Promise<HelloRequest>()
    val helloCancelled = Promise<ExitCase>()
    val helloChannel = makeChannel(object : GreeterCoroutineImplBase() {
      override suspend fun sayHello(request: HelloRequest): HelloReply {
        helloReceived.complete(request)
        return guaranteeCase({ never<HelloReply>() }) { case ->
          helloCancelled.complete(case)
        }
      }
    })
    val helloStub = GreeterArrowCoroutineStub(helloChannel)

    val result = ForkConnected {
      val request = helloRequest("Steven")
      helloStub.sayHello(request)
    }
    helloReceived.get()
    result.cancel()
    helloCancelled.get()
  }


  @Test
  fun unaryMethodThrowsStatusException() = runBlocking {
    val channel = makeChannel(
      object : GreeterCoroutineImplBase() {
        override suspend fun sayHello(request: HelloRequest): HelloReply {
          throw StatusException(Status.PERMISSION_DENIED)
        }
      }
    )

    val stub = GreeterArrowCoroutineStub(channel)
    val ex = assertThrows<StatusException> {
      stub.sayHello(helloRequest("Peridot"))
    }
    assertThat(ex.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun unaryMethodThrowsException() = runBlocking {
    val channel = makeChannel(
      object : GreeterCoroutineImplBase() {
        override suspend fun sayHello(request: HelloRequest): HelloReply {
          throw IllegalArgumentException()
        }
      }
    )

    val stub = GreeterArrowCoroutineStub(channel)
    val ex = assertThrows<StatusException> {
      stub.sayHello(helloRequest("Peridot"))
    }
    assertThat(ex.status.code).isEqualTo(Status.Code.UNKNOWN)
  }

  @Test
  fun simpleClientStreamingRpc() = runBlocking {
    val channel = makeChannel(object : GreeterCoroutineImplBase() {
      override suspend fun clientStreamSayHello(requests: Stream<HelloRequest>): HelloReply {
        return HelloReply.newBuilder()
          .setMessage(
            requests.compile().toList()
              .joinToString(prefix = "Hello, ", separator = ", ") { it.name }
          ).build()
      }
    })

    val stub = GreeterArrowCoroutineStub(channel)
    val requests = Stream(
      helloRequest("Peridot"),
      helloRequest("Lapis")
    )
    val response = ForkConnected { stub.clientStreamSayHello(requests) }
    assertThat(response.join()).isEqualTo(helloReply("Hello, Peridot, Lapis"))
  }

  @Test
  fun clientStreamingRpcCancellation() = runBlocking {
    val serverReceived = Promise<HelloRequest>()
    val serverCancelled = Promise<ExitCase>()
    val channel = makeChannel(object : GreeterCoroutineImplBase() {
      override suspend fun clientStreamSayHello(requests: Stream<HelloRequest>): HelloReply {
        requests.compile().lastOrNull()?.let {
          serverReceived.complete(it)
          return guaranteeCase({ never<HelloReply>() }) { case ->
            serverCancelled.complete(case)
          }
        } ?: throw AssertionError("unreachable")
      }
    })

    val stub = GreeterArrowCoroutineStub(channel)
    val requests = Queue.unsafeUnbounded<HelloRequest>()
    val response = ForkConnected {
      stub.clientStreamSayHello(requests.dequeue())
    }
    requests.tryOffer1(helloRequest("Aquamarine"))
    serverReceived.get()
    response.cancel()
    serverCancelled.get()
    assertThrows<CancellationException> {
      requests.tryOffer1(helloRequest("John"))
    }
  }

  @Test
  fun clientStreamingRpcThrowsStatusException() = runBlocking {
    val channel = makeChannel(object : GreeterCoroutineImplBase() {
      override suspend fun clientStreamSayHello(requests: Stream<HelloRequest>): HelloReply {
        throw StatusException(Status.PERMISSION_DENIED)
      }
    })
    val stub = GreeterArrowCoroutineStub(channel)

    val ex = assertThrows<StatusException> {
      stub.clientStreamSayHello(Stream.empty<HelloRequest>())
    }
    assertThat(ex.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun simpleServerStreamingRpc() = runBlocking {
    val channel = makeChannel(object : GreeterCoroutineImplBase() {
      override fun serverStreamSayHello(request: MultiHelloRequest): Stream<HelloReply> {
        return Stream.iterable(request.nameList).map { helloReply("Hello, $it") }
      }
    })

    val responses = GreeterArrowCoroutineStub(channel).serverStreamSayHello(
      multiHelloRequest("Garnet", "Amethyst", "Pearl")
    )

    assertThat(responses.compile().toList())
      .containsExactly(
        helloReply("Hello, Garnet"),
        helloReply("Hello, Amethyst"),
        helloReply("Hello, Pearl")
      )
      .inOrder()
  }


  @Test
  fun serverStreamingRpcCancellation() = runBlocking {
    val serverCancelled = Promise<ExitCase>()
    val serverReceived = Promise<Unit>()

    val channel = makeChannel(object : GreeterCoroutineImplBase() {
      override fun serverStreamSayHello(request: MultiHelloRequest): Stream<HelloReply> {
        return Stream.effect {
          serverReceived.complete(Unit)
          guaranteeCase({ never<HelloReply>() }) { case ->
            serverCancelled.complete(case)
          }
        }
      }
    })

    val response = GreeterArrowCoroutineStub(channel).serverStreamSayHello(
      multiHelloRequest("Topaz", "Aquamarine")
    ).produceIn()
    serverReceived.get()
    //response.cancel()
    serverCancelled.get()
  }

  @Test
  fun bidiPingPong() = runBlocking {
    val channel = makeChannel(object : GreeterCoroutineImplBase() {
      override fun bidiStreamSayHello(requests: Stream<HelloRequest>): Stream<HelloReply> {
        return requests.map { helloReply("Hello, ${it.name}") }
      }
    })

    val requests = Queue.unsafeUnbounded<HelloRequest>()
    val responses = GreeterArrowCoroutineStub(channel).bidiStreamSayHello(requests.dequeue()).produceIn()

    requests.tryOffer1(helloRequest("Steven"))
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Steven"))
    requests.tryOffer1(helloRequest("Garnet"))
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Garnet"))
    //requests.close()
    assertThat(responses.dequeue().compile().toList()).isEmpty()
  }

  @Test
  fun bidiStreamingRpcReturnsEarly() = runBlocking {
    val channel = makeChannel(object : GreeterCoroutineImplBase() {
      override fun bidiStreamSayHello(requests: Stream<HelloRequest>): Stream<HelloReply> {
        return requests.take(2).map { helloReply("Hello, ${it.name}") }
      }
    })

    val stub = GreeterArrowCoroutineStub(channel)
    val requests = Queue.unsafeUnbounded<HelloRequest>()
    val responses = stub.bidiStreamSayHello(requests.dequeue()).produceIn()
    requests.tryOffer1(helloRequest("Peridot"))
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Peridot"))
    requests.tryOffer1(helloRequest("Lapis"))
    assertThat(responses.dequeue1()).isEqualTo(helloReply("Hello, Lapis"))
    assertThat(responses.dequeue().compile().toList()).isEmpty()
    try {
      requests.tryOffer1(helloRequest("Jasper"))
    } catch (allowed: CancellationException) {
    }
  }

  @Test
  fun serverScopeCancelledDuringRpc() = runBlocking {
    val serverJob = Promise<ExitCase>()
    val serverReceived = Promise<Unit>()
    val channel = makeChannel(
      // TODO: do we need to pass the context to GreeterCoroutineImplBase(serverJob)
      object : GreeterCoroutineImplBase() {
        override suspend fun sayHello(request: HelloRequest): HelloReply {
          serverReceived.complete(Unit)
          return guaranteeCase({ never<HelloReply>() }) { case ->
            serverJob.complete(case)
          }
        }
      }
    )

    val stub = GreeterArrowCoroutineStub(channel)
    val test = ForkConnected {
      val ex = assertThrows<StatusException> {
        stub.sayHello(helloRequest("Greg"))
      }
      assertThat(ex.status.code).isEqualTo(Status.Code.CANCELLED)
    }
    serverReceived.get()
    serverJob.get()
    test.join()
  }

  @Test
  fun serverScopeCancelledBeforeRpc() = runBlocking {
    val serverJob = Promise<ExitCase>()
    val channel = makeChannel(
      object : GreeterCoroutineImplBase() {
        override suspend fun sayHello(request: HelloRequest): HelloReply {
          return guaranteeCase({ never<HelloReply>() }) { case ->
            serverJob.complete(case)
          }
        }
      }
    )

    serverJob.get()
    val stub = GreeterArrowCoroutineStub(channel)
    val ex = assertThrows<StatusException> {
      stub.sayHello(helloRequest("Greg"))
    }
    assertThat(ex.status.code).isEqualTo(Status.Code.CANCELLED)
  }

  @Test
  fun serviceDescriptor() {
    assertThat(GreeterGrpcKt.serviceDescriptor).isEqualTo(GreeterGrpc.getServiceDescriptor())
  }

  @Test
  fun methodDescriptor() {
    assertThat(GreeterGrpcKt.sayHelloMethod).isEqualTo(GreeterGrpc.getSayHelloMethod())
  }
}
