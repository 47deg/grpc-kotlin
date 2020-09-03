package io.grpc.kotlin.benchmarks

import org.openjdk.jmh.annotations.CompilerControl
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
open class GrpcKotlinUnary {

//  lateinit var channel: ManagedChannel
//
//  @Setup(Level.Trial)
//  fun setupUnaryServer(): Unit {
//    val serverImpl = object : GreeterGrpc.GreeterImplBase() {
//      override fun sayHello(request: HelloRequest, responseObserver: StreamObserver<HelloReply>) {
//        responseObserver.onNext(helloReply("Hello, ${request.name}"))
//        responseObserver.onCompleted()
//      }
//    }
//    channel = makeChannel(serverImpl)
//  }
//
//  @Benchmark
//  fun simpleUnary(): Unit = runBlocking {
//    val helloReply =
//      ClientCalls.unaryRpc(
//        channel = channel,
//        callOptions = CallOptions.DEFAULT,
//        method = sayHelloMethod,
//        request = helloRequest("Cindy")
//      )
//    println(helloReply) // blackhole
//  }

}