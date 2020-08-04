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

import arrow.fx.coroutines.*
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import arrow.fx.coroutines.stream.*
import arrow.fx.coroutines.stream.Stream.Companion.effect
import arrow.fx.coroutines.stream.Stream.Companion.raiseError
import arrow.fx.coroutines.stream.concurrent.Queue
import io.grpc.CallOptions
import io.grpc.ClientCall
import io.grpc.MethodDescriptor
import io.grpc.Status
import kotlin.coroutines.resumeWithException
import io.grpc.Channel as GrpcChannel
import io.grpc.Metadata as GrpcMetadata

/**
 * Helpers for gRPC clients implemented in Kotlin.  Can be used directly, but intended to be used
 * from generated Kotlin APIs.
 */
object ClientCalls {
  /**
   * Launches a unary RPC on the specified channel, suspending until the result is received.
   */
  suspend fun <RequestT, ResponseT> unaryRpc(
    channel: GrpcChannel,
    method: MethodDescriptor<RequestT, ResponseT>,
    request: RequestT,
    callOptions: CallOptions = CallOptions.DEFAULT,
    headers: GrpcMetadata = GrpcMetadata()
  ): ResponseT {
    require(method.type == MethodDescriptor.MethodType.UNARY) {
      "Expected a unary RPC method, but got $method"
    }
    return rpcImpl(
      channel = channel,
      method = method,
      callOptions = callOptions,
      headers = headers,
      request = Request.Unary(request)
    ).singleOrStatus("request", method)
  }

  /**
   * Returns a function object representing a unary RPC.
   *
   * The input headers may be asynchronously formed. [headers] will be called each time the returned
   * RPC is called - the headers are *not* cached.
   */
  fun <RequestT, ResponseT> unaryRpcFunction(
    channel: GrpcChannel,
    method: MethodDescriptor<RequestT, ResponseT>,
    callOptions: CallOptions = CallOptions.DEFAULT,
    headers: suspend () -> GrpcMetadata = { GrpcMetadata() }
  ): suspend (RequestT) -> ResponseT =
    { unaryRpc(channel, method, it, callOptions, headers()) }

  /**
   * Returns a [Stream] which launches the specified server-streaming RPC and emits the responses.
   */
  fun <RequestT, ResponseT> serverStreamingRpc(
    channel: GrpcChannel,
    method: MethodDescriptor<RequestT, ResponseT>,
    request: RequestT,
    callOptions: CallOptions = CallOptions.DEFAULT,
    headers: GrpcMetadata = GrpcMetadata()
  ): Stream<ResponseT> {
    require(method.type == MethodDescriptor.MethodType.SERVER_STREAMING) {
      "Expected a server streaming RPC method, but got $method"
    }
    return rpcImpl(
      channel = channel,
      method = method,
      callOptions = callOptions,
      headers = headers,
      request = Request.Unary(request)
    )
  }

  /**
   * Returns a function object representing a server streaming RPC.
   *
   * The input headers may be asynchronously formed. [headers] will be called each time the returned
   * RPC is called - the headers are *not* cached.
   */
  fun <RequestT, ResponseT> serverStreamingRpcFunction(
    channel: GrpcChannel,
    method: MethodDescriptor<RequestT, ResponseT>,
    callOptions: CallOptions = CallOptions.DEFAULT,
    headers: suspend () -> GrpcMetadata = { GrpcMetadata() }
  ): (RequestT) -> Stream<ResponseT> = { request ->
    effect {
      serverStreamingRpc(
        channel,
        method,
        request,
        callOptions,
        headers()
      )
    }.flatten()
  }


  /**
   * Launches a client-streaming RPC on the specified channel, suspending until the server returns
   * the result. The caller is expected to provide a [Stream] of requests.
   */
  suspend fun <RequestT, ResponseT> clientStreamingRpc(
    channel: GrpcChannel,
    method: MethodDescriptor<RequestT, ResponseT>,
    requests: Stream<RequestT>,
    callOptions: CallOptions = CallOptions.DEFAULT,
    headers: GrpcMetadata = GrpcMetadata()
  ): ResponseT {
    require(method.type == MethodDescriptor.MethodType.CLIENT_STREAMING) {
      "Expected a server streaming RPC method, but got $method"
    }
    return rpcImpl(
      channel = channel,
      method = method,
      callOptions = callOptions,
      headers = headers,
      request = Request.Flowing(requests)
    ).singleOrStatus("response", method)
  }

  /**
   * Returns a function object representing a client streaming RPC.
   *
   * The input headers may be asynchronously formed. [headers] will be called each time the returned
   * RPC is called - the headers are *not* cached.
   */
  fun <RequestT, ResponseT> clientStreamingRpcFunction(
    channel: GrpcChannel,
    method: MethodDescriptor<RequestT, ResponseT>,
    callOptions: CallOptions = CallOptions.DEFAULT,
    headers: suspend () -> GrpcMetadata = { GrpcMetadata() }
  ): suspend (Stream<RequestT>) -> ResponseT =
    {
      clientStreamingRpc(
        channel,
        method,
        it,
        callOptions,
        headers()
      )
    }

  /**
   * Returns a [Stream] which launches the specified bidirectional-streaming RPC, collecting the
   * requests flow, sending them to the server, and emitting the responses.
   *
   * Cancelling collection of the flow cancels the RPC upstream and collection of the requests.
   * For example, if `responses.take(2).toList()` is executed, the RPC will be cancelled after
   * the first two responses are returned.
   */
  fun <RequestT, ResponseT> bidiStreamingRpc(
    channel: GrpcChannel,
    method: MethodDescriptor<RequestT, ResponseT>,
    requests: Stream<RequestT>,
    callOptions: CallOptions = CallOptions.DEFAULT,
    headers: GrpcMetadata = GrpcMetadata()
  ): Stream<ResponseT> {
    check(method.type == MethodDescriptor.MethodType.BIDI_STREAMING) {
      "Expected a bidi streaming method, but got $method"
    }
    return rpcImpl(
      channel = channel,
      method = method,
      callOptions = callOptions,
      headers = headers,
      request = Request.Flowing(requests)
    )
  }

  /**
   * Returns a function object representing a bidirectional streaming RPC.
   *
   * The input headers may be asynchronously formed. [headers] will be called each time the returned
   * RPC is called - the headers are *not* cached.
   */
  fun <RequestT, ResponseT> bidiStreamingRpcFunction(
    channel: GrpcChannel,
    method: MethodDescriptor<RequestT, ResponseT>,
    callOptions: CallOptions = CallOptions.DEFAULT,
    headers: suspend () -> GrpcMetadata = { GrpcMetadata() }
  ): (Stream<RequestT>) -> Stream<ResponseT> = {
    effect {
      bidiStreamingRpc(
        channel,
        method,
        it,
        callOptions,
        headers()
      )
    }.flatten()
  }

  /** The client's request(s). */
  private sealed class Request<RequestT> {
    /**
     * Send the request(s) to the ClientCall, with `readiness` indicating calls to `onReady` from
     * the listener.  Returns when sending the requests is done, either because all the requests
     * were sent (in which case `null` is returned) or because the requests channel was closed
     * with an exception (in which case the exception is returned).
     */
    abstract suspend fun sendTo(
      clientCall: ClientCall<RequestT, *>,
      readiness: Readiness
    )

    class Unary<RequestT>(private val request: RequestT) : Request<RequestT>() {
      override suspend fun sendTo(
        clientCall: ClientCall<RequestT, *>,
        readiness: Readiness
      ) {
        clientCall.sendMessage(request)
      }
    }

    class Flowing<RequestT>(private val requestStream: Stream<RequestT>) : Request<RequestT>() {
      override suspend fun sendTo(
        clientCall: ClientCall<RequestT, *>,
        readiness: Readiness
      ) {
        readiness.suspendUntilReady()
        requestStream.effectMap { request: RequestT ->
          clientCall.sendMessage(request)
          readiness.suspendUntilReady()
        }
      }
    }
  }

  /**
   * Returns a [Stream] that, when collected, issues the specified RPC with the specified request
   * on the specified channel, and emits the responses.  This is intended to be the root
   * implementation of the client side of all Kotlin coroutine-based RPCs, with non-streaming
   * implementations simply emitting or receiving a single message in the appropriate direction.
   */
  private fun <RequestT, ResponseT> rpcImpl(
    channel: GrpcChannel,
    method: MethodDescriptor<RequestT, ResponseT>,
    callOptions: CallOptions,
    headers: GrpcMetadata,
    request: Request<RequestT>
  ): Stream<ResponseT> = effect {
    val clientCall: ClientCall<RequestT, ResponseT> =
      channel.newCall<RequestT, ResponseT>(method, callOptions)

    /*
     * We maintain a buffer of size 1 so onMessage never has to block: it only gets called after
     * we request a response from the server, which only happens when responses is empty and
     * there is room in the buffer.
     */
    val responses = Queue.unsafeBounded<ResponseT>(1)
    val readiness = Readiness { clientCall.isReady }

    clientCall.start(
      object : ClientCall.Listener<ResponseT>() {
        override fun onMessage(message: ResponseT) {
          if (!responses.tryOffer1(message)) {
            throw AssertionError("onMessage should never be called until responses is ready")
          }
        }

        override fun onClose(status: Status, trailersMetadata: GrpcMetadata) {
          // responses.close()
        }

        override fun onReady() {
          readiness.onReady()
        }
      },
      headers
    )


    val isFinished = Atomic(false)

    val sender: Fiber<Unit> = ForkConnected {
      try {
        request.sendTo(clientCall, readiness)
        clientCall.halfClose()
      } catch (ex: Exception) {
        clientCall.cancel("Collection of requests completed exceptionally", ex)
        throw ex // propagate failure upward
      }

      isFinished.set(true)
    }

    Stream.effect {
      clientCall.request(1)
    }.flatMap {
      responses
        .dequeue()
        .effectTap { clientCall.request(1) }
    }.handleErrorWith { e ->
      Stream.effect {
        uncancellable {
          sender.cancel()

//          sender.join()
//          sender.cancelAndJoin("Collection of responses completed exceptionally", e)

          // we want sender to be done cancelling before we cancel clientCall, or it might try
          // sending to a dead call, which results in ugly exception messages
          clientCall.cancel("Collection of responses completed exceptionally", e)
        }
      }.flatMap { Stream.raiseError<ResponseT>(e) }
    }.append { Stream.effect_ { if (!isFinished.get()) sender.cancel() } }
  }.flatten()
}
