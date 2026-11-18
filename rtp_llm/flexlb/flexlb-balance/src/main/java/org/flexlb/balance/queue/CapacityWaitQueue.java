package org.flexlb.balance.queue;

import org.flexlb.balance.schedule.RequestContext;
import org.flexlb.config.service.RoleType;
import org.flexlb.worker.endpoint.CapacityKey;
import org.flexlb.worker.endpoint.RequestCapacity;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Pauses requests by role/group and resumes ordered batches within shared worker capacity.
 * Notifications and periodic capacity checks permit fresh selection;
 * failed retries and newly parked requests wait for the next finite pass.
 * The scheduler lock protects every operation; retry budgets do not reserve Engine resources.
 */
public final class CapacityWaitQueue {
    private final Comparator<RequestContext> order;
    private final Map<CapacityKey, Domain> domains = new HashMap<>();
    private final Map<RequestContext, Waiter> membership = new IdentityHashMap<>();
    private final NavigableSet<RequestContext> ready;
    private final Map<WorkerKey, Budget> workerBudgets = new HashMap<>();
    private final Map<CapacityKey, Budget> controlChecks = new HashMap<>();

    public CapacityWaitQueue(Comparator<RequestContext> order) {
        this.order = order;
        ready = new TreeSet<>(order);
    }

    public boolean isWaiting(RequestContext request) {
        var waiter = membership.get(request);
        return waiter != null && waiter.budget == null;
    }

    public void park(RequestContext request, CapacityKey blocker) {
        var key = blocker.waitingDomain();
        var waiter = membership.get(request);
        if (waiter == null || !waiter.domain.key.equals(key)) {
            remove(request);
            waiter = new Waiter(domains.computeIfAbsent(key, Domain::new));
            membership.put(request, waiter);
        }
        var domain = waiter.domain;
        ready.remove(request);
        boolean changedDuringRetry = finishRetry(waiter, true);
        domain.entries.remove(request);
        domain.deferred.add(request);
        if (changedDuringRetry) reopen(domain);
    }

    public void remove(RequestContext request) {
        var waiter = membership.remove(request);
        if (waiter == null) return;
        var domain = waiter.domain;
        ready.remove(request);
        finishRetry(waiter, false);
        domain.entries.remove(request);
        domain.deferred.remove(request);
        removeEmptyDomain(domain);
    }

    public void capacityChanged(RequestCapacity capacity) {
        var id = new WorkerKey(capacity.key().role(), capacity.generation());
        var budget = workerBudgets.get(id);
        if (budget != null && capacity.revision() <= budget.revision) return;
        if (budget == null) {
            budget = new Budget(capacity.key());
            workerBudgets.put(id, budget);
        }
        budget.key = capacity.key();
        budget.revision = capacity.revision();
        budget.refresh(capacity.availableRequests());
        for (var domain : domains.values()) {
            if (!budget.matches(domain.key)) continue;
            domain.budgets.removeIf(candidate -> candidate.key.endpoint() == null);
            if (!domain.budgets.contains(budget)) domain.budgets.add(budget);
            reopen(domain);
        }
    }

    /**
     * Refreshes local capacity and permits one control check per full role/group for preemption.
     * The control check is not an Engine admission slot. Worker budgets remain shared across groups.
     */
    public void checkCapacity(List<RequestCapacity> capacities) {
        var currentWorkers = new java.util.HashSet<WorkerKey>();
        for (var capacity : capacities) {
            var id = new WorkerKey(capacity.key().role(), capacity.generation());
            currentWorkers.add(id);
            var budget = workerBudgets.computeIfAbsent(id, ignored -> new Budget(capacity.key()));
            if (capacity.revision() < budget.revision) continue;
            budget.key = capacity.key();
            budget.revision = capacity.revision();
            budget.refresh(capacity.availableRequests());
        }
        for (var entry : workerBudgets.entrySet())
            if (!currentWorkers.contains(entry.getKey())) entry.getValue().refresh(0L);
        workerBudgets
                .entrySet()
                .removeIf(
                        entry ->
                                !currentWorkers.contains(entry.getKey())
                                        && entry.getValue().inFlight == 0);
        controlChecks.entrySet().removeIf(entry -> entry.getValue().inFlight == 0);
        for (var budget : controlChecks.values()) budget.refresh(1L);
        for (var domain : domains.values()) {
            domain.budgets.clear();
            for (var budget : workerBudgets.values())
                if (budget.matches(domain.key)) domain.budgets.add(budget);
            if (domain.budgets.stream().noneMatch(budget -> budget.capacity > 0L)) {
                domain.budgets.add(
                        controlChecks.computeIfAbsent(
                                domain.key,
                                key -> {
                                    var budget = new Budget(key);
                                    budget.refresh(1L);
                                    return budget;
                                }));
            }
            reopen(domain);
        }
    }

    public boolean hasWaitingRequests() {
        return !domains.isEmpty();
    }

    public boolean hasReadyRequests() {
        return !ready.isEmpty();
    }

    public void resumeReady(
            int limit, Predicate<RequestContext> canRun, Consumer<RequestContext> resume) {
        var available = new HashMap<Domain, Budget>();
        var iterator = ready.iterator();
        for (int count = 0; count < limit && iterator.hasNext(); ) {
            var request = iterator.next();
            if (!canRun.test(request)) continue;
            var waiter = membership.get(request);
            var domain = waiter.domain;
            var budget = availableBudget(domain, available);
            if (budget == null) continue;
            iterator.remove();
            domain.entries.remove(request);
            budget.spent++;
            budget.inFlight++;
            domain.inFlight++;
            waiter.budget = budget;
            waiter.epoch = budget.epoch;
            resume.accept(request);
            count++;
        }
    }

    public void clear() {
        domains.clear();
        ready.clear();
        membership.clear();
        workerBudgets.clear();
        controlChecks.clear();
    }

    private Budget availableBudget(Domain domain, Map<Domain, Budget> cache) {
        var cached = cache.get(domain);
        if (cache.containsKey(domain) && (cached == null || cached.remaining() > 0L)) return cached;
        for (var budget : domain.budgets) {
            if (budget.matches(domain.key) && budget.remaining() > 0L) {
                cache.put(domain, budget);
                return budget;
            }
        }
        cache.put(domain, null);
        return null;
    }

    private boolean finishRetry(Waiter waiter, boolean capacityMiss) {
        var budget = waiter.budget;
        if (budget == null) return false;
        budget.inFlight--;
        waiter.domain.inFlight--;
        boolean refreshed = waiter.epoch != budget.epoch;
        if (refreshed) {
            budget.oldInFlight--;
            if (!capacityMiss) budget.spent++;
        }
        waiter.budget = null;
        return refreshed;
    }

    private void reopen(Domain domain) {
        if (domain.entries.isEmpty()) {
            var previous = domain.entries;
            domain.entries = domain.deferred;
            domain.deferred = previous;
            ready.addAll(domain.entries);
        }
    }

    private void removeEmptyDomain(Domain domain) {
        if (domain.entries.isEmpty() && domain.deferred.isEmpty() && domain.inFlight == 0)
            domains.remove(domain.key, domain);
    }

    /**
     * Keeps one finite ordered pass and the requests waiting for the next pass in a role/group.
     */
    private final class Domain {
        private final CapacityKey key;
        private final List<Budget> budgets = new ArrayList<>();
        private NavigableSet<RequestContext> entries = new TreeSet<>(order);
        private NavigableSet<RequestContext> deferred = new TreeSet<>(order);
        private int inFlight;

        private Domain(CapacityKey key) {
            this.key = key;
        }
    }

    /**
     * Retains a request's budget claim until its retry finishes or moves to another group.
     */
    private static final class Waiter {
        private final Domain domain;
        private Budget budget;
        private long epoch;

        private Waiter(Domain domain) {
            this.domain = domain;
        }
    }

    private record WorkerKey(RoleType role, long generation) {}

    /**
     * Shares one worker's scheduling budget across all eligible groups.
     * Completion never replenishes spent retries; refreshed snapshots account for pending attempts.
     */
    private static final class Budget {
        private CapacityKey key;
        private long revision = -1L;
        private long capacity;
        private long spent;
        private long epoch;
        private int oldInFlight;
        private int inFlight;

        private Budget(CapacityKey key) {
            this.key = key;
        }

        private void refresh(long available) {
            capacity = available;
            spent = 0L;
            oldInFlight = inFlight;
            epoch++;
        }

        private long remaining() {
            return Math.max(0L, capacity - spent - oldInFlight);
        }

        private boolean matches(CapacityKey waiter) {
            return key.role() == waiter.role()
                    && (waiter.group() == null || waiter.group().equals(key.group()));
        }
    }
}
