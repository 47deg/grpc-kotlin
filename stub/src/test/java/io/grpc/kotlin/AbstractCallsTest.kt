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
import arrow.fx.coroutines.Environment
import arrow.fx.coroutines.ForkAndForget
import arrow.fx.coroutines.IOPool
import arrow.fx.coroutines.stream.concurrent.Enqueue
import arrow.fx.coroutines.stream.concurrent.Queue
import com.google.common.util.concurrent.MoreExecutors
import io.grpc.BindableService
import io.grpc.Context
import io.grpc.ManagedChannel
import io.grpc.MethodDescriptor
import io.grpc.ServerBuilder
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.ServerMethodDefinition
import io.grpc.ServerServiceDefinition
import io.grpc.ServiceDescriptor
import io.grpc.examples.helloworld.GreeterGrpc
import io.grpc.examples.helloworld.HelloReply
import io.grpc.examples.helloworld.HelloRequest
import io.grpc.examples.helloworld.MultiHelloRequest
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.rules.Timeout
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext

abstract class AbstractCallsTest {
  companion object {
    fun helloRequest(name: String): HelloRequest = HelloRequest.newBuilder().setName(name).build()
    fun helloReply(message: String): HelloReply = HelloReply.newBuilder().setMessage(message).build()
    fun multiHelloRequest(vararg name: String): MultiHelloRequest =
      MultiHelloRequest.newBuilder().addAllName(name.asList()).build()

    val sayHelloMethod: MethodDescriptor<HelloRequest, HelloReply> =
      GreeterGrpc.getSayHelloMethod()
    val clientStreamingSayHelloMethod: MethodDescriptor<HelloRequest, HelloReply> =
      GreeterGrpc.getClientStreamSayHelloMethod()
    val serverStreamingSayHelloMethod: MethodDescriptor<MultiHelloRequest, HelloReply> =
      GreeterGrpc.getServerStreamSayHelloMethod()
    val bidiStreamingSayHelloMethod: MethodDescriptor<HelloRequest, HelloReply> =
      GreeterGrpc.getBidiStreamSayHelloMethod()
    val greeterService: ServiceDescriptor = GreeterGrpc.getServiceDescriptor()

    suspend fun <E> produce(block: suspend Enqueue<Option<E>>.() -> Unit): Queue<Option<E>> {
      // RENDEZVOUS
      val queue = Queue.synchronous<Option<E>>()
      ForkAndForget {
        queue.block()
        queue.enqueue1(None)
      }
      return queue
    }

    fun whenContextIsCancelled(onCancelled: () -> Unit) {
      Context.current().withCancellation().addListener(
        Context.CancellationListener { onCancelled() },
        MoreExecutors.directExecutor()
      )
    }
  }

  @get:Rule
  var globalTimeout: Timeout = Timeout.seconds(3) // 10 seconds max per method tested

  // We want the coroutines timeout to come first, because it comes with useful debug logs.
  @get:Rule
  val grpcCleanup = GrpcCleanupRule().setTimeout(11, TimeUnit.SECONDS)

  lateinit var channel: ManagedChannel

  private lateinit var executor: ExecutorService

  private val context: CoroutineContext
    get() = IOPool

  @Before
  fun setUp() {
    executor = Executors.newFixedThreadPool(10)
  }

  @After
  fun tearDown() {
    executor.shutdown()
    if (this::channel.isInitialized) {
      channel.shutdownNow()
    }
  }

  inline fun <reified E : Exception> assertThrows(
    callback: () -> Unit
  ): E {
    var ex: Exception? = null
    try {
      callback()
    } catch (e: Exception) {
      ex = e
    }
    if (ex is E) {
      return ex
    } else {
      throw Error("Expected an ${E::class.qualifiedName}", ex)
    }
  }

  /** Generates a channel to a Greeter server with the specified implementation. */
  fun makeChannel(impl: BindableService, vararg interceptors: ServerInterceptor): ManagedChannel =
    makeChannel(ServerInterceptors.intercept(impl, *interceptors))

  fun makeChannel(serverServiceDefinition: ServerServiceDefinition): ManagedChannel {
    val serverName = InProcessServerBuilder.generateName()

    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .run { this as ServerBuilder<*> } // workaround b/123879662
        .executor(executor)
        .addService(serverServiceDefinition)
        .build()
        .start()
    )

    return grpcCleanup.register(
      InProcessChannelBuilder
        .forName(serverName)
        .run { this as io.grpc.ManagedChannelBuilder<*> } // workaround b/123879662
        .executor(executor)
        .build()
    )
  }

  fun makeChannel(
    impl: ServerMethodDefinition<*, *>,
    vararg interceptors: ServerInterceptor
  ): ManagedChannel {
    val builder = ServerServiceDefinition.builder(greeterService)
    for (method in greeterService.methods) {
      if (method == impl.methodDescriptor) {
        builder.addMethod(impl)
      } else {
        builder.addMethod(method, ServerCallHandler { _, _ -> TODO() })
      }
    }
    return makeChannel(ServerInterceptors.intercept(builder.build(), *interceptors))
  }

  fun <R> runBlocking(block: suspend () -> R): Unit = Environment(context).unsafeRunSync {
    block()
    Unit
  }
}
