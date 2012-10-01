package com.zeebox.akka

import collection.immutable
import akka.actor.{Supervisor, MaximumNumberOfRestartsWithinTimeRangeReached, Actor, ActorRef}
import akka.routing.{CyclicIterator, LoadBalancer, InfiniteIterator}
import akka.actor.Actor._
import akka.config.Supervision.SupervisorConfig
import akka.config.Supervision

private[zeebox] object Routers{

  def loadBalancer(actors: immutable.Seq[ActorRef], iterator : immutable.Seq[ActorRef] => InfiniteIterator[ActorRef] ): ActorRef = {
    actorOf(new Actor with LoadBalancer {
      val seq = iterator(actors)

      override def postStop() {
        super.postStop()
        actors foreach (actor => try actor.stop() catch{ case e => System.err.println("Failed to stop actor [%s]" format actor)})
      }

    }).start()
  }

  def cyclicIteratorLoadBalancer(numWorkers: Int, actor: => Actor, maxRestartsHandler: (ActorRef, MaximumNumberOfRestartsWithinTimeRangeReached) => Unit): ActorRef = {
    val actors = supervisedPool(actor, numWorkers, maxRestartsHandler)
    loadBalancer(actors, CyclicIterator(_))
  }

  def supervisedPool(actor: => Actor, numWorkers: Int, maxRestartsHandler: (ActorRef, MaximumNumberOfRestartsWithinTimeRangeReached) => Unit): immutable.Seq[ActorRef] = {
    val supervisor = Supervisor(SupervisorConfig(Supervision.OneForOneStrategy(List(classOf[Throwable])), Nil, maxRestartsHandler))
    val actors = Vector.fill(numWorkers) {Actor.actorOf(actor).start()}
    actors.foreach(supervisor.link)
    actors
  }

}
