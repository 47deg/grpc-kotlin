package io.grpc.kotlin

import arrow.fx.coroutines.stream.*


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