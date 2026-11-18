package org.flexlb.balance.queue;

import org.flexlb.balance.schedule.RequestContext;
import org.flexlb.config.service.RoleType;
import org.flexlb.worker.endpoint.CapacityKey;
import org.flexlb.worker.endpoint.RequestCapacity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Checks group retry batches, shared capacity, scan progress and notifications during a retry.
 */
class CapacityWaitQueueTest {
    private final CapacityWaitQueue queue =
            new CapacityWaitQueue(Comparator.comparingLong(RequestContext::getPlacementSequence));
    private final CapacityKey group = new CapacityKey(RoleType.PREFILL, "g", null);

    @Test
    void newArrivalsCannotKeepExtendingTheCurrentPass() {
        var first = request(1);
        var second = request(2);
        var arriving = request(3);
        queue.park(first, group);
        queue.park(second, group);
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group);
        queue.park(arriving, group);
        changed(2, 1);
        assertEquals(List.of(second), resume(10));
        queue.park(second, group);
        changed(3, 1);
        assertEquals(
                List.of(first),
                resume(10),
                "a new pass must reconsider the failed head before later arrivals");
    }

    @Test
    void oneGroupCanUseMultipleSlotsWithoutWaitingForRetryCompletion() {
        var first = request(1);
        var second = request(2);
        var third = request(3);
        queue.park(third, group);
        queue.park(first, group);
        queue.park(second, group);
        assertTrue(resume(10).isEmpty());
        changed(1, 2);
        assertEquals(List.of(first, second), resume(10));
        assertFalse(queue.isWaiting(first));
        assertFalse(queue.isWaiting(second));
        assertTrue(queue.isWaiting(third));
        assertTrue(resume(10).isEmpty());
        queue.remove(first);
        queue.remove(second);
        assertTrue(resume(10).isEmpty(), "completion does not replenish the budget");
        changed(2, 1);
        assertEquals(List.of(third), resume(10));
        queue.remove(third);
        assertFalse(queue.hasWaitingRequests());
    }

    @Test
    void plannerLimitAndCapacityApplyAcrossMultipleResumeCalls() {
        var first = request(1);
        var second = request(2);
        var third = request(3);
        queue.park(first, group);
        queue.park(second, group);
        queue.park(third, group);
        changed(1, 2);
        assertEquals(List.of(first), resume(1));
        assertEquals(List.of(second), resume(1));
        queue.park(first, group);
        queue.park(second, group);
        assertTrue(resume(10).isEmpty(), "failed retries do not replenish the budget");
        changed(2, 1);
        assertEquals(List.of(third), resume(10));
    }

    @Test
    void failedHeadDoesNotHideTheNextRequestOnAnotherCapacityObservation() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group);
        queue.park(second, group);
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group);
        changed(2, 1);
        assertEquals(List.of(second), resume(10));
        queue.remove(second);
        changed(3, 1);
        assertEquals(List.of(first), resume(10));
    }

    @Test
    void periodicChecksContinueTheScanBeforeRetryingTheHead() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group);
        queue.park(second, group);
        check(1);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group);
        check(1);
        assertEquals(List.of(second), resume(10));
        queue.park(second, group);
        check(1);
        assertEquals(List.of(first), resume(10));
    }

    @Test
    void exactAndAnyGroupWaitersShareTheSameWorkerBudget() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group);
        queue.park(second, new CapacityKey(RoleType.PREFILL, null, null));
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
        queue.remove(first);
        assertTrue(resume(10).isEmpty());
        changed(2, 1);
        assertEquals(List.of(second), resume(10));
    }

    @Test
    void unrelatedRolesAndGroupsDoNotResumeWaiters() {
        var first = request(1);
        queue.park(first, group);
        queue.capacityChanged(capacity(RoleType.DECODE, "g", 2, 1, 10));
        queue.capacityChanged(capacity(RoleType.PREFILL, "other", 3, 1, 10));
        assertTrue(resume(10).isEmpty());
        changed(1, 0);
        assertTrue(resume(10).isEmpty());
        changed(2, 1);
        assertEquals(List.of(first), resume(10));
    }

    @Test
    void lowerOrDuplicateRevisionDoesNotRestoreSpentBudget() {
        var first = request(1);
        queue.park(first, group);
        changed(3, 1);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group);
        changed(2, 100);
        changed(3, 100);
        assertTrue(resume(10).isEmpty());
    }

    @Test
    void notificationDuringRetryAccountsForEveryInFlightClaim() {
        var first = request(1);
        var second = request(2);
        var third = request(3);
        queue.park(first, group);
        queue.park(second, group);
        queue.park(third, group);
        changed(1, 2);
        assertEquals(List.of(first, second), resume(10));
        changed(2, 2);
        assertTrue(resume(10).isEmpty());
        queue.park(first, group);
        assertEquals(List.of(third), resume(10));
        queue.remove(second);
        assertTrue(
                resume(10).isEmpty(), "completed old attempts must consume their refreshed claim");
    }

    @Test
    void notificationDuringTheOnlyRetryIsRetainedAfterParking() {
        var first = request(1);
        queue.park(first, group);
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
        changed(2, 1);
        queue.park(first, group);
        assertEquals(List.of(first), resume(10));
    }

    @Test
    void fullGroupGetsOnlyOneControlCheckPerPeriod() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group);
        queue.park(second, group);
        check(0);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group);
        assertTrue(resume(10).isEmpty());
        check(0);
        assertEquals(List.of(second), resume(10));
    }

    @Test
    void removingTheNextWaiterPreservesProgress() {
        var first = request(1);
        var second = request(2);
        var third = request(3);
        queue.park(first, group);
        queue.park(second, group);
        queue.park(third, group);
        changed(1, 2);
        assertEquals(List.of(first), resume(1));
        queue.remove(second);
        assertEquals(List.of(third), resume(1));
    }

    @Test
    void aRunningHeadDoesNotBlockAnotherGroup() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group);
        queue.park(second, new CapacityKey(RoleType.DECODE, "g", null));
        changed(1, 1);
        queue.capacityChanged(capacity(RoleType.DECODE, "g", 2, 1, 1));
        var resumed = new ArrayList<RequestContext>();
        queue.resumeReady(2, request -> request != first, resumed::add);
        assertEquals(List.of(second), resumed);
        assertEquals(List.of(first), resume(2));
    }

    @Test
    void movingToAnotherGroupAndClearingReleaseMembership() {
        var first = request(1);
        queue.park(first, group);
        changed(1, 1);
        assertEquals(List.of(first), resume(1));
        var other = new CapacityKey(RoleType.DECODE, "g", null);
        queue.park(first, other);
        assertTrue(queue.isWaiting(first));
        queue.capacityChanged(capacity(RoleType.DECODE, "g", 2, 1, 1));
        assertEquals(List.of(first), resume(1));
        queue.clear();
        assertFalse(queue.isWaiting(first));
        assertFalse(queue.hasWaitingRequests());
        assertFalse(queue.hasReadyRequests());
    }

    private void changed(long revision, long slots) {
        queue.capacityChanged(capacity(RoleType.PREFILL, "g", 1, revision, slots));
    }

    private static RequestCapacity capacity(
            RoleType role, String group, long generation, long revision, long slots) {
        return new RequestCapacity(
                CapacityKey.exact(role, group, "worker-" + generation),
                generation,
                revision,
                slots);
    }

    private void check(long slots) {
        queue.checkCapacity(List.of(capacity(RoleType.PREFILL, "g", 1, 0, slots)));
    }

    private List<RequestContext> resume(int limit) {
        var resumed = new ArrayList<RequestContext>();
        queue.resumeReady(limit, request -> true, resumed::add);
        return resumed;
    }

    private static RequestContext request(long sequence) {
        var request = mock(RequestContext.class);
        when(request.getPlacementSequence()).thenReturn(sequence);
        return request;
    }
}
