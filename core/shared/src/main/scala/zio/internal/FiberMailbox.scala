package zio.internal

import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Optimized FiberMailbox with L1 cache-line padding
 * to eliminate false sharing in high-contention scenarios.
 * Issue: #8807
 */
final class FiberMailbox {
  private[this] var p1, p2, p3, p4, p5, p6, p7: Long = 0L
  @volatile private[this] var s0, s1, s2, s3: AnyRef = _
  private[this] var p8, p9, p10, p11, p12, p13, p14: Long = 0L
  private[this] val overflow = new ConcurrentLinkedQueue[AnyRef]()

  def offer(msg: AnyRef): Unit = {
    if (s0 == null) s0 = msg
    else if (s1 == null) s1 = msg
    else if (s2 == null) s2 = msg
    else if (s3 == null) s3 = msg
    else overflow.offer(msg)
  }

  def poll(): AnyRef = {
    var m = s0; if (m != null) { s0 = null; return m }
    m = s1; if (m != null) { s1 = null; return m }
    m = s2; if (m != null) { s2 = null; return m }
    m = s3; if (m != null) { s3 = null; return m }
    overflow.poll()
  }

  def isEmpty(): Boolean =
    s0 == null && s1 == null && s2 == null && s3 == null && overflow.isEmpty
}
