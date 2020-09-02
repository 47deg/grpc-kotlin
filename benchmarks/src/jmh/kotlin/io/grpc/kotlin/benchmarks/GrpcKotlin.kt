package io.grpc.kotlin.benchmarks

import org.openjdk.jmh.annotations.CompilerControl
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
open class GrpcKotlin {

  @Param("1000")
  var size: Int = 0

  

}