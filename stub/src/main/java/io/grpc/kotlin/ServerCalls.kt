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

import arrow.fx.coroutines.Environment
import arrow.fx.coroutines.ExitCase
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.flatten
import io.grpc.MethodDescriptor
import io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING
import io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING
import io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING
import io.grpc.MethodDescriptor.MethodType.UNARY
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerMethodDefinition
import io.grpc.Status
import io.grpc.StatusException
import java.util.concurrent.CancellationException
import kotlin.coroutines.CoroutineContext
import io.grpc.Metadata as GrpcMetadata

/**
 * Helpers for implementing a gRPC server based on Arrow Fx coroutines implementation.
 */
object ServerCalls {
  /**
   * Creates a [ServerMethodDefinition] that implements the specified unary RPC method by running
   * the specified implementation and associated implementation details within a per-RPC
   * [CoroutineScope] generated with the specified [CoroutineContext].
   *
   * When the RPC is received, this method definition will pass the request from the client
   * to [implementation], and send the response back to the client when it is returned.
   *
   * If [implementation] fails with a [StatusException], the RPC will fail with the corresponding
   * [Status].  If [implementation] fails with a [CancellationException], the RPC will fail
   * with [Status.CANCELLED].  If [implementation] fails for any other reason, the RPC will
   * fail with [Status.UNKNOWN] with the exception as a cause.  If a cancellation is received
   * from the client before [implementation] is complete, the coroutine will be cancelled and the
   * RPC will fail with [Status.CANCELLED].
   *
   * @param context The context of the scopes the RPC implementation will run in
   * @param descriptor The descriptor of the method being implemented
   * @param implementation The implementation of the RPC method
   */
  fun <RequestT, ResponseT> unaryServerMethodDefinition(
    context: CoroutineContext,
    descriptor: MethodDescriptor<RequestT, ResponseT>,
    implementation: suspend (request: RequestT) -> ResponseT
  ): ServerMethodDefinition<RequestT, ResponseT> {
    require(descriptor.type == UNARY) {
      "Expected a unary method descriptor but got $descriptor"
    }
    return serverMethodDefinition(context, descriptor) { requests: Stream<RequestT> ->
      requests
        .singleOrStatusStream("request", descriptor)
        .flatMap { Stream.effect { implementation(it) } }
    }
  }

  /**
   * Creates a [ServerMethodDefinition] that implements the specified client-streaming RPC method by
   * running the specified implementation and associated implementation details within a per-RPC
   * [CoroutineScope] generated with the specified [CoroutineContext].
   *
   * When the RPC is received, this method definition will pass a [Flow] of requests from the client
   * to [implementation], and send the response back to the client when it is returned.
   * Exceptions are handled as in [unaryServerMethodDefinition].  Additionally, attempts to collect
   * the requests flow more than once will throw an [IllegalStateException], and if [implementation]
   * cancels collection of the requests flow, further requests from the client will be ignored
   * (and no backpressure will be applied).
   *
   * @param context The context of the scopes the RPC implementation will run in
   * @param descriptor The descriptor of the method being implemented
   * @param implementation The implementation of the RPC method
   */
  fun <RequestT, ResponseT> clientStreamingServerMethodDefinition(
    context: CoroutineContext,
    descriptor: MethodDescriptor<RequestT, ResponseT>,
    implementation: suspend (requests: Stream<RequestT>) -> ResponseT
  ): ServerMethodDefinition<RequestT, ResponseT> {
    require(descriptor.type == CLIENT_STREAMING) {
      "Expected a client streaming method descriptor but got $descriptor"
    }
    return serverMethodDefinition(context, descriptor) { requests: Stream<RequestT> ->
      Stream.effect { implementation(requests) }
    }
  }

  /**
   * Creates a [ServerMethodDefinition] that implements the specified server-streaming RPC method by
   * running the specified implementation and associated implementation details within a per-RPC
   * [CoroutineScope] generated with the specified [CoroutineContext].  When the RPC is received,
   * this method definition will collect the flow returned by [implementation] and send the emitted
   * values back to the client.
   *
   * When the RPC is received, this method definition will pass the request from the client
   * to [implementation], and collect the returned [Flow], sending responses to the client as they
   * are emitted.  Exceptions and cancellation are handled as in [unaryServerMethodDefinition].
   *
   * @param context The context of the scopes the RPC implementation will run in
   * @param descriptor The descriptor of the method being implemented
   * @param implementation The implementation of the RPC method
   */
  fun <RequestT, ResponseT> serverStreamingServerMethodDefinition(
    context: CoroutineContext,
    descriptor: MethodDescriptor<RequestT, ResponseT>,
    implementation: (request: RequestT) -> Stream<ResponseT>
  ): ServerMethodDefinition<RequestT, ResponseT> {
    require(descriptor.type == SERVER_STREAMING) {
      "Expected a server streaming method descriptor but got $descriptor"
    }

    return serverMethodDefinition(context, descriptor) { requests: Stream<RequestT> ->
      requests
        .singleOrStatusStream("request", descriptor)
        .flatMap(implementation)
    }
  }

  /**
   * Creates a [ServerMethodDefinition] that implements the specified bidirectional-streaming RPC
   * method by running the specified implementation and associated implementation details within a
   * per-RPC [CoroutineScope] generated with the specified [CoroutineContext].
   *
   * When the RPC is received, this method definition will pass a [Flow] of requests from the client
   * to [implementation], and collect the returned [Flow], sending responses to the client as they
   * are emitted.
   *
   * Exceptions and cancellation are handled as in [clientStreamingServerMethodDefinition] and as
   * in [serverStreamingServerMethodDefinition].
   *
   * @param context The context of the scopes the RPC implementation will run in
   * @param descriptor The descriptor of the method being implemented
   * @param implementation The implementation of the RPC method
   */
  fun <RequestT, ResponseT> bidiStreamingServerMethodDefinition(
    context: CoroutineContext,
    descriptor: MethodDescriptor<RequestT, ResponseT>,
    implementation: (requests: Stream<RequestT>) -> Stream<ResponseT>
  ): ServerMethodDefinition<RequestT, ResponseT> {
    require(descriptor.type == BIDI_STREAMING) {
      "Expected a bidi streaming method descriptor but got $descriptor"
    }
    return serverMethodDefinition(context, descriptor, implementation)
  }

  /**
   * Builds a [ServerMethodDefinition] that implements the specified RPC method by running the
   * specified channel-based implementation within the specified [CoroutineScope] (and/or a
   * subscope).
   */
  private fun <RequestT, ResponseT> serverMethodDefinition(
    context: CoroutineContext,
    descriptor: MethodDescriptor<RequestT, ResponseT>,
    implementation: (Stream<RequestT>) -> Stream<ResponseT>
  ): ServerMethodDefinition<RequestT, ResponseT> =
    ServerMethodDefinition.create(
      descriptor,
      serverCallHandler(context, implementation)
    )

  /**
   * Returns a [ServerCallHandler] that implements an RPC method by running the specified
   * channel-based implementation within the specified [CoroutineScope] (and/or a subscope).
   */
  private fun <RequestT, ResponseT> serverCallHandler(
    context: CoroutineContext,
    implementation: (Stream<RequestT>) -> Stream<ResponseT>
  ): ServerCallHandler<RequestT, ResponseT> =
    ServerCallHandler { call, _ ->
      serverCallListener(
        context
          + CoroutineContextServerInterceptor.COROUTINE_CONTEXT_KEY.get()
          + GrpcContextElement.current(),
        call,
        implementation
      )
    }

  private fun <RequestT, ResponseT> serverCallListener(
    context: CoroutineContext,
    call: ServerCall<RequestT, ResponseT>,
    implementation: (Stream<RequestT>) -> Stream<ResponseT>
  ): ServerCall.Listener<RequestT> {
    call.sendHeaders(GrpcMetadata())

    val readiness = Readiness { call.isReady }
    val requestsChannel = Queue.unsafeBounded<RequestT>(1)

    // We complete this latch when processing requests fails, or when we halfClose.
    // Check `isEmpty` to check if we're still taking requests.
    val isActive = UnsafePromise<Unit>()

    val requests = // TODO should we request messages in `Chunks` ???
      Stream.effect { call.request(1) } // Request first message
        .flatMap {
          requestsChannel
            .dequeue() // For every value we receive, we need to request the next one
            .stopWhen { !isActive.isEmpty() }
            .effectTap { call.request(1) }
        }.onFinalizeCase { ex ->
          println("ServerCall.Requests.onFinalizeCase: $ex")
          when (ex) {
            is ExitCase.Failure -> call.request(1) // make sure we don't cause backpressure
            else -> Unit
          }
        }

    // Runs async cancellable on the provided context, always returns a new cancellable scope.
    val rpcCancelToken = Environment(context).unsafeRunAsyncCancellable {
      Stream.effect { implementation(requests) }.flatten()
        .effectMap { response: ResponseT ->
          readiness.suspendUntilReady()
          call.sendMessage(response)
        }.onFinalizeCase { case ->
          println("ServerCalls.Server.onFinalizeCase: $case")
          when (case) {
            ExitCase.Completed -> {
              isActive.join() // Before closing the call we need to await the call calling onHalfClosed.
              call.close(Status.OK, GrpcMetadata())
            }
            ExitCase.Cancelled -> call.close(Status.CANCELLED, GrpcMetadata())
            is ExitCase.Failure -> call.close(Status.fromThrowable(case.failure), Status.trailersFromThrowable(case.failure))
          }
        }
        .attempt() // We have already forwarded any errors in call.close
        .compile()
        .drain()
    }

    return object : ServerCall.Listener<RequestT>() {
      override fun onCancel() {
        println("ServerCall.Listener.onCancel()")
        rpcCancelToken.invoke()
      }

      override fun onMessage(message: RequestT) {
        println("ServerCall.Listener.onMessage($message)")
        if (isActive.isEmpty() && !requestsChannel.tryOffer1(message)) {
          throw Status.INTERNAL
            .withDescription("onMessage should never be called when requestsChannel is unready")
            .asException()
        }

        if (!isActive.isEmpty()) {
          call.request(1) // do not exert backpressure
        }
      }

      override fun onHalfClose() {
        println("ServerCall.Listener.onHalfClose()")
        isActive.complete(Result.success(Unit))
      }

      override fun onReady() {
        println("ServerCall.Listener.onReady")
        readiness.onReady()
      }
    }
  }
}
