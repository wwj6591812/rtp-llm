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
    void workerNotificationSelectsItsHintedWaiterWithinTheSameGroup() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group, java.util.Set.of(1L));
        queue.park(second, group, java.util.Set.of(2L));
        queue.capacityChanged(capacity(RoleType.PREFILL, "g", 2, 1, 1));
        assertEquals(List.of(second), resume(10));
        assertTrue(queue.isWaiting(first));
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
    }

    @Test
    void overlappingHintsShareOneWorkerBudgetWithoutCreatingWaitingDomains() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group, java.util.Set.of(1L));
        queue.park(second, group, java.util.Set.of(1L, 2L));
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
        queue.remove(first);
        changed(1, 1);
        assertTrue(resume(10).isEmpty());
        changed(2, 1);
        assertEquals(List.of(second), resume(10));
    }

    @Test
    void aFailedRetryReplacesItsPreviousHints() {
        var first = request(1);
        queue.park(first, group, java.util.Set.of(1L));
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group, java.util.Set.of(2L));
        changed(2, 1);
        assertTrue(resume(10).isEmpty(), "the old hint must not match a new notification");
        queue.capacityChanged(capacity(RoleType.PREFILL, "g", 2, 1, 1));
        assertEquals(List.of(first), resume(10));
    }

    @Test
    void periodicProbeCanReconsiderAStaleHintBeforeMatchedTrafficSpendsEverySlot() {
        var stale = request(1);
        var matching = request(2);
        queue.park(stale, group, java.util.Set.of(1L));
        queue.park(matching, group, java.util.Set.of(2L));
        var available = capacity(RoleType.PREFILL, "g", 2, 1, 1);
        queue.checkCapacity(List.of(available));
        assertEquals(List.of(stale), resume(10));
        queue.park(stale, group, java.util.Set.of(2L));
        assertTrue(resume(10).isEmpty());
        queue.checkCapacity(List.of(available));
        assertEquals(
                List.of(matching),
                resume(10),
                "the current finite pass continues before the failed probe");
    }

    @Test
    void unusedPeriodicCapacityCanReconsiderMultipleStaleHints() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group, java.util.Set.of(1L));
        queue.park(second, group, java.util.Set.of(2L));
        queue.checkCapacity(List.of(capacity(RoleType.PREFILL, "g", 3, 1, 2)));
        assertEquals(List.of(first, second), resume(10));
        assertTrue(resume(10).isEmpty());
    }

    @Test
    void hintMatchingPreservesOrderAcrossGroupsWhenAHeadHasNoMatchingCapacity() {
        var blocked = request(1);
        var otherGroup = request(2);
        var sameGroup = request(3);
        queue.park(blocked, group, java.util.Set.of(1L));
        queue.park(sameGroup, group, java.util.Set.of(2L));
        queue.park(
                otherGroup, new CapacityKey(RoleType.PREFILL, "other", null), java.util.Set.of(3L));
        queue.capacityChanged(capacity(RoleType.PREFILL, "g", 2, 1, 1));
        queue.capacityChanged(capacity(RoleType.PREFILL, "other", 3, 1, 1));
        assertEquals(List.of(otherGroup, sameGroup), resume(10));
        assertTrue(queue.isWaiting(blocked));
    }

    @Test
    void newArrivalsCannotKeepExtendingTheCurrentPass() {
        var first = request(1);
        var second = request(2);
        var arriving = request(3);
        queue.park(first, group, java.util.Set.of());
        queue.park(second, group, java.util.Set.of());
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group, java.util.Set.of());
        queue.park(arriving, group, java.util.Set.of());
        changed(2, 1);
        assertEquals(List.of(second), resume(10));
        queue.park(second, group, java.util.Set.of());
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
        queue.park(third, group, java.util.Set.of());
        queue.park(first, group, java.util.Set.of());
        queue.park(second, group, java.util.Set.of());
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
        queue.park(first, group, java.util.Set.of());
        queue.park(second, group, java.util.Set.of());
        queue.park(third, group, java.util.Set.of());
        changed(1, 2);
        assertEquals(List.of(first), resume(1));
        assertEquals(List.of(second), resume(1));
        queue.park(first, group, java.util.Set.of());
        queue.park(second, group, java.util.Set.of());
        assertTrue(resume(10).isEmpty(), "failed retries do not replenish the budget");
        changed(2, 1);
        assertEquals(List.of(third), resume(10));
    }

    @Test
    void failedHeadDoesNotHideTheNextRequestOnAnotherCapacityObservation() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group, java.util.Set.of());
        queue.park(second, group, java.util.Set.of());
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group, java.util.Set.of());
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
        queue.park(first, group, java.util.Set.of());
        queue.park(second, group, java.util.Set.of());
        check(1);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group, java.util.Set.of());
        check(1);
        assertEquals(List.of(second), resume(10));
        queue.park(second, group, java.util.Set.of());
        check(1);
        assertEquals(List.of(first), resume(10));
    }

    @Test
    void exactAndAnyGroupWaitersShareTheSameWorkerBudget() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group, java.util.Set.of());
        queue.park(second, new CapacityKey(RoleType.PREFILL, null, null), java.util.Set.of());
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
        queue.park(first, group, java.util.Set.of());
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
        queue.park(first, group, java.util.Set.of());
        changed(3, 1);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group, java.util.Set.of());
        changed(2, 100);
        changed(3, 100);
        assertTrue(resume(10).isEmpty());
    }

    @Test
    void notificationDuringRetryAccountsForEveryInFlightClaim() {
        var first = request(1);
        var second = request(2);
        var third = request(3);
        queue.park(first, group, java.util.Set.of());
        queue.park(second, group, java.util.Set.of());
        queue.park(third, group, java.util.Set.of());
        changed(1, 2);
        assertEquals(List.of(first, second), resume(10));
        changed(2, 2);
        assertTrue(resume(10).isEmpty());
        queue.park(first, group, java.util.Set.of());
        assertEquals(List.of(third), resume(10));
        queue.remove(second);
        assertTrue(
                resume(10).isEmpty(), "completed old attempts must consume their refreshed claim");
    }

    @Test
    void notificationDuringTheOnlyRetryIsRetainedAfterParking() {
        var first = request(1);
        queue.park(first, group, java.util.Set.of());
        changed(1, 1);
        assertEquals(List.of(first), resume(10));
        changed(2, 1);
        queue.park(first, group, java.util.Set.of());
        assertEquals(List.of(first), resume(10));
    }

    @Test
    void fullGroupGetsOnlyOneControlCheckPerPeriod() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group, java.util.Set.of());
        queue.park(second, group, java.util.Set.of());
        check(0);
        assertEquals(List.of(first), resume(10));
        queue.park(first, group, java.util.Set.of());
        assertTrue(resume(10).isEmpty());
        check(0);
        assertEquals(List.of(second), resume(10));
    }

    @Test
    void removingTheNextWaiterPreservesProgress() {
        var first = request(1);
        var second = request(2);
        var third = request(3);
        queue.park(first, group, java.util.Set.of());
        queue.park(second, group, java.util.Set.of());
        queue.park(third, group, java.util.Set.of());
        changed(1, 2);
        assertEquals(List.of(first), resume(1));
        queue.remove(second);
        assertEquals(List.of(third), resume(1));
    }

    @Test
    void aRunningHeadDoesNotBlockAnotherGroup() {
        var first = request(1);
        var second = request(2);
        queue.park(first, group, java.util.Set.of());
        queue.park(second, new CapacityKey(RoleType.DECODE, "g", null), java.util.Set.of());
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
        queue.park(first, group, java.util.Set.of());
        changed(1, 1);
        assertEquals(List.of(first), resume(1));
        var other = new CapacityKey(RoleType.DECODE, "g", null);
        queue.park(first, other, java.util.Set.of());
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
