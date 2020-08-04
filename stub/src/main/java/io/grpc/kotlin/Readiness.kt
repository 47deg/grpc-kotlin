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

import arrow.fx.coroutines.stream.concurrent.Queue

/**
 * A simple helper allowing a notification of "ready" to be broadcast, and waited for.
 */
internal class Readiness (
  private val isReallyReady: () -> Boolean
) {
  // A CONFLATED channel never suspends to send, and two notifications of readiness are equivalent
  // to one
  /**
   * are these equivalent? unsafeBounded(1)[Strategy.boundedFifo] and Channel<Unit>(Channel.CONFLATED)
   * Channel.CONFLATED: Channel that buffers at most one element and conflates all subsequent `send` and `offer` invocations,
   * so that the receiver always gets the most recently sent element.
   * Back-to-send sent elements are _conflated_ -- only the the most recently sent element is received,
   * while previously sent elements **are lost**.
   * Sender to this channel never suspends and [offer] always returns `true`.
   *
   * This channel is created by `Channel(Channel.CONFLATED)` factory function invocation.
   *
   * This implementation is fully lock-free.
   */
  private val channel = Queue.unsafeBounded<Unit>(1)

  fun onReady() {
    if (!channel.tryOffer1(Unit)) {
      throw AssertionError(
        "Should be impossible; a CONFLATED channel should never return false on offer"
      )
    }
  }

  suspend fun suspendUntilReady() {
    while (!isReallyReady()) {
      channel.dequeue()
    }
  }
}
