package io.grpc.kotlin

import arrow.fx.coroutines.CancelToken
import arrow.fx.coroutines.ForkAndForget
import arrow.fx.coroutines.cancellable
import arrow.fx.coroutines.stream.Pull
import arrow.fx.coroutines.stream.Stream
import arrow.fx.coroutines.stream.flatMap
import arrow.fx.coroutines.stream.map
import arrow.fx.coroutines.stream.repeat
import arrow.fx.coroutines.stream.stream
import arrow.fx.coroutines.stream.uncons1OrNull
import arrow.fx.coroutines.stream.unconsOrNull
import java.util.concurrent.atomic.AtomicReference

fun <O, B> Stream<O>.effectFold(init: B, f: suspend (B, O) -> B): Stream<B> {
  fun go(z: B, s: Pull<O, Unit>): Pull<B, Unit> =
    s.uncons1OrNull().flatMap { uncons1 ->
      when (uncons1) {
        null -> Pull.done
        else -> Pull.effect { f(z, uncons1.head) }
          .flatMap { newO2 -> go(newO2, uncons1.tail) }
      }
    }

  return asPull().uncons1OrNull().flatMap { uncons1 ->
    when (uncons1) {
      null -> Pull.output1(init)
      else -> go(init, asPull())
    }
  }.stream()
}

fun <O> Stream<O>.stopWhen(terminator: () -> Boolean): Stream<O> =
  asPull().repeat { pull ->
    pull.unconsOrNull().flatMap { uncons ->
      when (uncons) {
        null -> Pull.just(null)
        else -> {
          if(!terminator()) {
            Pull.output<O>(uncons.head).map { uncons.tail }
          } else {
            Pull.output(uncons.head).map { null }
          }
        }
      }
    }
  }.stream()

/**
 * An eager Promise implementation to bridge results across processes internally.
 * @see ForkAndForget
 */
internal class UnsafePromise<A> {

  private sealed class State<out A> {
    object Empty : State<Nothing>()
    data class Waiting<A>(val joiners: List<(Result<A>) -> Unit>) : State<A>()

    @Suppress("RESULT_CLASS_IN_RETURN_TYPE")
    data class Full<A>(val a: Result<A>) : State<A>()
  }

  private val state: AtomicReference<State<A>> = AtomicReference(State.Empty)

  fun isEmpty(): Boolean =
    when (state.get()) {
      State.Empty -> true
      else -> false
    }


  @Suppress("RESULT_CLASS_IN_RETURN_TYPE")
  fun tryGet(): Result<A>? =
    when (val curr = state.get()) {
      is State.Full -> curr.a
      else -> null
    }

  fun get(cb: (Result<A>) -> Unit) {
    tailrec fun go(): Unit = when (val oldState = state.get()) {
      State.Empty -> if (state.compareAndSet(oldState, State.Waiting(listOf(cb)))) Unit else go()
      is State.Waiting -> if (state.compareAndSet(oldState, State.Waiting(oldState.joiners + cb))) Unit else go()
      is State.Full -> cb(oldState.a)
    }

    go()
  }

  suspend fun join(): A =
    cancellable { cb ->
      get(cb)
      CancelToken { remove(cb) }
    }

  fun complete(value: Result<A>) {
    tailrec fun go(): Unit = when (val oldState = state.get()) {
      State.Empty -> if (state.compareAndSet(oldState, State.Full(value))) Unit else go()
      is State.Waiting -> {
        if (state.compareAndSet(oldState, State.Full(value))) oldState.joiners.forEach { it(value) }
        else go()
      }
      is State.Full -> throw RuntimeException()
    }

    go()
  }

  fun remove(cb: (Result<A>) -> Unit) = when (val oldState = state.get()) {
    State.Empty -> Unit
    is State.Waiting -> state.set(State.Waiting(oldState.joiners - cb))
    is State.Full -> Unit
  }
}
