package com.github.piotrga.asyncdynamo
package functional

import annotation.tailrec
import concurrent.{ExecutionContext, Promise, Future}
import util.{Try, Failure, Success}
import scala.language.reflectiveCalls


sealed trait Iteratee[E, A]{
  def map[B](g: B => E) : Iteratee[B, A] = this match {
    case Cont(f) => Cont((x:Input[B]) => f(x map g).map(g) )
    case Done(res) => Done[B,A](res)
    case Error(cause, partiallyGatheredState) => Error[B,A](cause, partiallyGatheredState.map(g))
  }

  def withFilter(f : E => Boolean) : Iteratee[E,A] = this match{
    case Cont(g) => Cont((i:Input[E]) => (i match{
      case Elem(e) => if (f(e)) g(i) else this
      case _ => g(i)
    }).withFilter(f))
    case x => x
  }
}

case class Cont[E, A](f: Input[E] => Iteratee[E, A]) extends Iteratee[E, A]
case class Done[E, A](res: A) extends Iteratee[E, A]
case class Error[E, A](cause: Throwable, partiallyGatheredState: Iteratee[E,A]) extends Iteratee[E, A]

sealed trait Input[E]{
  def map[A](g: E=>A) : Input[A] = this match {
    case Elem(e) => Elem(g(e))
    case EOI(err) => EOI[A](err)
  }
}

case class EOI[E](error: Option[Throwable] = None) extends Input[E]
case class Elem[E](elem: E) extends Input[E]

object Iteratee{

  @tailrec
  def enumerate[E,A](list: List[E], iter: Iteratee[E, A]) : Iteratee[E, A] =
    iter match {
      case Cont(f) =>  list match {
        case h::t => enumerate(t, f(Elem(h)))
        case Nil => f(EOI[E]())
      }
      case other => other
    }

  def batch[E,A](iter: Iteratee[E, A]) : Iteratee[Seq[E],A] = {

    @tailrec
    def enumerateBatch[E,A](list: List[E], iter: Iteratee[E, A]) : Iteratee[E, A] =
      iter match {
        case it@Cont(f) =>  list match {
          case h::t => enumerateBatch(t, f(Elem(h)))
          case Nil => it
        }
        case other => other
      }

    iter match {
      case it@Cont(f) =>
        Cont(s => s match{
          case Elem(seq) => batch(enumerateBatch(seq.toList, it))
          case EOI(ex) => batch(f(EOI(ex)))
        })
      case Done(res) => Done[Seq[E], A](res)
      case Error(e, state) => Error[Seq[E], A](e, batch(state))
    }
  }

  def unbatch[E,A](iter: Iteratee[Seq[E], A]) : Iteratee[E,A] = iter match{
    case Cont(f) => Cont(i => unbatch(f(i.map(Seq(_)))))
    case Done(res)=> Done(res)
    case Error(e, s) => Error(e, unbatch(s))
  }

  def pageAsynchronously2[KEY, ELEM, RESULT](nextBatch: Option[KEY] => Future[(Seq[ELEM], Option[KEY])], iter: Iteratee[ELEM, RESULT])
                                            (implicit promise: {def apply[T](): Promise[T]}, execCtx :ExecutionContext): Future[Iteratee[ELEM, RESULT]] =
    pageAsynchronously(nextBatch, batch(iter)) map unbatch

  def pageAsynchronously[KEY, ELEM, RESULT](nextBatch: Option[KEY] => Future[(Seq[ELEM], Option[KEY])], iter: Iteratee[Seq[ELEM], RESULT])
                                           (implicit promise: {def apply[T](): Promise[T]}, execCtx :ExecutionContext): Future[Iteratee[Seq[ELEM], RESULT]] = {
    val res = promise[Iteratee[Seq[ELEM], RESULT]]()

    iter match {
      case iter@Cont(_) => nextBatch(None) onComplete doIterationStep(iter)
      case _ => res.success(iter)
    }


    def doIterationStep(iter: Cont[Seq[ELEM], RESULT])(queryResult: Try[(Seq[ELEM], Option[KEY])]) {
      queryResult match {
        case Success((batch, None)) =>
          iter.f(Elem(batch)) match {
            case Cont(f) => res.success(f(EOI[Seq[ELEM]]()))
            case other => res.success(other)
          }
        case Success((batch, lastKey@Some(_))) =>
          iter.f(Elem(batch)) match {
            case cont@Cont(_) => nextBatch(lastKey) onComplete doIterationStep(cont)
            case other => res.success(other)
          }
        case Failure(error) =>
          res.success(Error(error, iter.f(EOI(Some(error)))))
      }
    }

    res.future
  }

  def takeAll[E]() : Iteratee[E, List[E]] = {
    def step(t:List[E])(e: Input[E]) : Iteratee[E, List[E]] = e match {
      case Elem(el) => Cont(step( el :: t))
      case _ => Done(t.reverse)
    }
    Cont(step(Nil))
  }

  def takeAll[E, B](f : E => B) : Iteratee[E, List[B]] = takeAll() map f

  def takeWhile[E](cond : E => Boolean) : Iteratee[E, List[E]] = {
    def step(t:List[E])(e: Input[E]) : Iteratee[E, List[E]] = e match {
      case Elem(el) if cond(el) => Cont(step(el :: t))
      case _ => Done(t.reverse)
    }
    Cont(step(Nil))
  }

  def takeUntil[E](cond : E => Boolean) : Iteratee[E, List[E]] = takeWhile(!cond(_))

  def takeOnly[E](limit : Int) : Iteratee[E, List[E]] = {
    def step(t:List[E], limit:Int)(e: Input[E]) : Iteratee[E, List[E]] = e match {
      case Elem(el) if limit > 0 => Cont(step(el :: t, limit-1))
      case _ => Done(t.reverse)
    }
    Cont(step(Nil, limit))
  }

}