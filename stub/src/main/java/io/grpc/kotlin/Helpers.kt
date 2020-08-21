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
import arrow.fx.coroutines.ForkAndForget
import arrow.fx.coroutines.ForkConnected
import arrow.fx.coroutines.stream.Pull
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.concurrent.Enqueue
import arrow.fx.coroutines.stream.concurrent.Queue
import arrow.fx.coroutines.stream.drain
import arrow.fx.coroutines.stream.firstOrNull
import arrow.fx.coroutines.stream.flatMap
import arrow.fx.coroutines.stream.lastOrNull
import arrow.fx.coroutines.stream.stream
import arrow.fx.coroutines.stream.terminateOnNone
import arrow.fx.coroutines.stream.unconsOrNull
import io.grpc.Status
import io.grpc.StatusException

/**
 * Extracts the value of a [Deferred] known to be completed, or throws its exception if it was
 * not completed successfully.  (Non-experimental variant of `getDone`.)
 */
//internal val <T> Deferred<T>.doneValue: T
//  get() {
//    check(isCompleted) { "doneValue should only be called on completed Deferred values" }
//    return runBlocking(Dispatchers.Unconfined) {
//      await()
//    }
//  }

/**
 * Cancels a [Job] with a cause and suspends until the job completes/is finished cancelling.
 */
//internal suspend fun Job.cancelAndJoin(message: String, cause: Exception? = null) {
//  cancel(message, cause)
//  join()
//}

/**
 * Returns this flow, save that if there is not exactly one element, it throws a [StatusException].
 *
 * The purpose of this function is to enable the one element to get processed before we have
 * confirmation that the input flow is done.
 */
internal fun <O> Stream<O>.singleOrStatusStream(expected: String, descriptor: Any): Stream<O> {
  fun go(prev: O?, s: Pull<O, Unit>, count: Int): Pull<Nothing, O> =
    s.unconsOrNull().flatMap { uncons ->
      when (uncons) {
        null -> when {
          (count == 1) -> Pull.just(prev) as Pull<Nothing, O>
          else -> Pull.raiseError(StatusException(
            Status.INTERNAL.withDescription("Expected one $expected for $descriptor but received none")
          ))
        }
        else -> when {
          uncons.head.size() > 1 || count > 0 -> Pull.raiseError(StatusException(
            Status.INTERNAL.withDescription("Expected one $expected for $descriptor but received two")
          ))
          else -> go(uncons.head[0], uncons.tail, 1)
        }
      }
    }

  return go(null, asPull(), 0).flatMap(Pull.Companion::output1).stream()
}

/**
 * Returns the one and only element of this flow, and throws a [StatusException] if there is not
 * exactly one element.
 */
internal suspend fun <T> Stream<T>.singleOrStatus(
  expected: String,
  descriptor: Any
): T = singleOrStatusStream(expected, descriptor).firstOrNull()!!

suspend fun <T> Stream<T>.produceIn(): Queue<T> {
  val queue = Queue.unbounded<T>()
  ForkConnected {
    fold(Unit) { _, item: T ->
      queue.tryOffer1(item)
    }.drain()
  }
  return queue
}

suspend fun <T> Stream<Option<T>>.produceIn2(): Queue<Option<T>> {
  val queue = Queue.bounded<Option<T>>(0)
  terminateOnNone().effectMap { item: T ->
    queue.enqueue1(Some(item))
  }.drain()
  return queue
}

suspend fun <E> produce(block: suspend Enqueue<Option<E>>.() -> Unit): Queue<Option<E>> {
  val queue = Queue.unsafeBounded<Option<E>>(0)
  ForkAndForget {
    queue.block()
    queue.enqueue1(None)
  }
  return queue
}
