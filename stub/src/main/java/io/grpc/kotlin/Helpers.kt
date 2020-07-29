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

import arrow.fx.coroutines.stream.Pull
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.compile
import arrow.fx.coroutines.stream.flatMap
import arrow.fx.coroutines.stream.stream
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
internal fun <T> Stream<T>.singleOrStatusStream(
  expected: String,
  descriptor: Any
): Stream<T> {
  fun <O> Pull<O, Unit>.firstOrStatus(): Pull<O, Unit> =
    unconsOrNull().flatMap { uncons ->
      when {
        uncons == null -> Pull.done()
        uncons.head.size() == 1 -> Pull.output1(uncons.head[0])
        else -> Pull.raiseError(StatusException(
          Status.INTERNAL.withDescription("Expected one $expected for $descriptor but received two")
        ))
      }
    }

  return asPull().firstOrStatus().stream()
}

/**
 * Returns the one and only element of this flow, and throws a [StatusException] if there is not
 * exactly one element.
 */
internal suspend fun <T> Stream<T>.singleOrStatus(
  expected: String,
  descriptor: Any
): T =
  singleOrStatusStream(expected, descriptor).take(1).compile().lastOrNull()!!
