package org.flexlb.balance.queue;

import org.flexlb.balance.request.Request;
import org.flexlb.balance.schedule.RequestContext;
import org.flexlb.balance.schedule.SchedulingMetadata;
import org.flexlb.balance.schedule.SchedulingTestConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks priority composition, bounded scans and stale asynchronous estimates.
 */
class PrefillCostOrderTest {
    @Test
    void priorityAndCostAreIndependentAndUnknownCostsUseStableOrder() {
        for (boolean priority : new boolean[] {false, true}) {
            var queue = new OrderedRequestQueue(priority, true);
            var high = entry(90);
            var cheap = entry(10);
            queue.add(high);
            queue.add(cheap);
            assertEquals(List.of(high), queue.scanForPlanningCandidates(1, 10, r -> true));
            queue.retry(high);
            complete(queue, high, 90, cheap, 10);
            queue.beginScan();
            assertEquals(
                    List.of(priority ? high : cheap),
                    queue.scanForPlanningCandidates(1, 10, r -> true));
        }
    }

    @Test
    void equalDensityRunsTogetherAndBetterRunningDensityBlocksWorse() {
        var queue = new OrderedRequestQueue(false, true);
        var first = entry(50);
        var equal = entry(50, 200L);
        var worse = entry(50);
        queue.add(first);
        queue.add(equal);
        queue.add(worse);
        for (var ticket : queue.estimates(10))
            ticket.complete(
                    OptionalLong.of(
                            ticket.request() == worse ? 80 : ticket.request() == equal ? 40 : 20));
        assertEquals(List.of(first, equal), queue.scanForPlanningCandidates(3, 10, r -> true));
        queue.retry(equal);
        queue.beginScan();
        assertEquals(List.of(equal), queue.scanForPlanningCandidates(3, 10, r -> r != first));
        queue.retry(first);
    }

    @Test
    void lateEstimateCannotChangeRestoredMembershipAndHugeProductsRemainExact() {
        var queue = new OrderedRequestQueue(false, true);
        var first = entry(50, Long.MAX_VALUE);
        var second = entry(50, Long.MAX_VALUE);
        queue.add(first);
        var old = queue.estimates(1).getFirst();
        queue.remove(first);
        queue.restore(first);
        queue.add(second);
        old.complete(OptionalLong.of(0));
        complete(queue, first, Long.MAX_VALUE - 1, second, 1);
        assertEquals(List.of(second), queue.scanForPlanningCandidates(1, 10, r -> true));
        assertEquals(2, queue.drain().size());
        assertTrue(queue.estimates(10).isEmpty());
    }

    @Test
    void scanBudgetDoesNotLoseCheaperRequestsBeyondFirstPage() {
        var queue = new OrderedRequestQueue(false, true);
        var first = entry(50);
        var cheap = entry(50);
        queue.add(first);
        queue.add(cheap);
        complete(queue, first, 90, cheap, 10);
        assertTrue(queue.scanForPlanningCandidates(1, 1, r -> true).isEmpty());
        var checks = new java.util.concurrent.atomic.AtomicInteger();
        assertTrue(
                queue.scanForPlanningCandidates(1, 1, r -> checks.incrementAndGet() > 0).isEmpty());
        assertEquals(1, checks.get());
        assertEquals(
                List.of(cheap),
                queue.scanForPlanningCandidates(1, 1, r -> checks.incrementAndGet() > 0));
        assertEquals(2, checks.get());
    }

    private static void complete(
            OrderedRequestQueue queue,
            RequestContext first,
            long a,
            RequestContext second,
            long b) {
        for (var ticket : queue.estimates(10))
            ticket.complete(OptionalLong.of(ticket.request() == first ? a : b));
    }

    private static RequestContext entry(int priority) {
        return entry(priority, 100);
    }

    private static RequestContext entry(int priority, long inputTokens) {
        var context = new RequestContext(SchedulingTestConfig.newConfig());
        context.setRequest(Request.builder().seqLen(inputTokens).build());
        context.setSchedulingMetadata(SchedulingMetadata.of(priority, null, Long.MAX_VALUE, 50));
        return context;
    }
}
