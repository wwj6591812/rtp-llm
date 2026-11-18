package org.flexlb.balance.queue;

import org.flexlb.balance.schedule.RequestContext;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Compares exact compute/input ratios after a budgeted scan, preserving priority arbitration.
 * Unknown estimates use stable ordering; estimates remain usable while a refresh is in flight.
 */
final class PrefillCostOrder {
    private final boolean priority;

    private final Comparator<RequestContext> stableOrder;
    private final Map<RequestContext, Entry> entries = new IdentityHashMap<>();
    private final NavigableSet<RequestContext> stable;
    private final NavigableSet<RequestContext> ranked = new TreeSet<>(this::compare);

    private boolean pendingSelection;

    private final Map<RequestContext, Long> running = new IdentityHashMap<>();
    private final java.util.LinkedHashSet<SchedulingQueue.EstimateRequest> refresh =
            new java.util.LinkedHashSet<>();

    PrefillCostOrder(boolean priority, Comparator<RequestContext> stableOrder) {
        this.priority = priority;
        this.stableOrder = stableOrder;
        stable = new TreeSet<>(stableOrder);
    }

    void add(RequestContext request) {
        var entry = new Entry();
        entries.put(request, entry);
        enqueue(request, entry);
    }

    List<RequestContext> select(
            List<RequestContext> page,
            boolean scanComplete,
            int limit,
            int remainingChecks,
            Predicate<RequestContext> eligible) {

        long now = System.nanoTime();
        for (var request : page) {
            var entry = entries.get(request);
            if (entry == null) continue;
            stable.add(request);
            if (known(request)) ranked.add(request);
            if (entry.ticket == null
                    && now - entry.estimatedAtNanos
                            >= java.util.concurrent.TimeUnit.SECONDS.toNanos(1))
                enqueue(request, entry);
        }
        if (!scanComplete) return List.of();
        var selected = new ArrayList<RequestContext>();
        pendingSelection = false;
        for (; !stable.isEmpty() && selected.size() < limit; remainingChecks--) {
            if (remainingChecks == 0) {
                pendingSelection = true;
                break;
            }
            boolean unknown = stable.size() != ranked.size() || running.containsValue(null);
            var request = (unknown ? stable : ranked).first();
            if (!eligible.test(request)) {
                discard(request);
                continue;
            }
            if (running.keySet().stream().anyMatch(active -> blocks(active, request, unknown)))
                break;
            if (!unknown && !selected.isEmpty() && differentClass(selected.getFirst(), request))
                break;
            discard(request);
            running.put(request, known(request) ? entries.get(request).compute : null);
            selected.add(request);
        }
        return selected;
    }

    List<SchedulingQueue.EstimateRequest> estimates(int limit) {
        var result = new ArrayList<SchedulingQueue.EstimateRequest>();
        while (result.size() < limit && !refresh.isEmpty()) {
            var iterator = refresh.iterator();
            var ticket = iterator.next();
            iterator.remove();
            var entry = entries.get(ticket.request());
            if (entry != null && entry.ticket == ticket) result.add(ticket);
        }
        return result;
    }

    boolean hasPendingSelection() {
        return pendingSelection;
    }

    void finished(RequestContext request) {
        running.remove(request);
        var entry = entries.get(request);
        if (entry != null
                && entry.ticket == null
                && System.nanoTime() - entry.estimatedAtNanos
                        >= java.util.concurrent.TimeUnit.SECONDS.toNanos(1))
            enqueue(request, entry);
    }

    void remove(RequestContext request) {
        discard(request);
        var entry = entries.remove(request);
        if (entry != null && entry.ticket != null) refresh.remove(entry.ticket);

        // A planner may still be completing; finished() removes its frozen input.
    }

    private void discard(RequestContext request) {
        stable.remove(request);
        if (known(request)) ranked.remove(request);
    }

    private boolean known(RequestContext request) {
        var entry = entries.get(request);
        return entry != null && entry.compute >= 0 && input(request) > 0;
    }

    private static long input(RequestContext request) {
        return request.getRequest().getSeqLen();
    }

    private boolean blocks(RequestContext active, RequestContext candidate, boolean unknown) {
        if (priority && SchedulingQueue.priority(active) != SchedulingQueue.priority(candidate))
            return SchedulingQueue.priority(active) > SchedulingQueue.priority(candidate);
        return !unknown
                && ratio(
                                running.get(active),
                                input(active),
                                entries.get(candidate).compute,
                                input(candidate))
                        < 0;
    }

    private static int ratio(long left, long leftInput, long right, long rightInput) {
        try {
            return Long.compare(
                    Math.multiplyExact(left, rightInput), Math.multiplyExact(right, leftInput));
        } catch (ArithmeticException overflow) {
            return BigInteger.valueOf(left)
                    .multiply(BigInteger.valueOf(rightInput))
                    .compareTo(BigInteger.valueOf(right).multiply(BigInteger.valueOf(leftInput)));
        }
    }

    private boolean differentClass(RequestContext first, RequestContext other) {
        return priority && SchedulingQueue.priority(first) != SchedulingQueue.priority(other)
                || ratio(
                                entries.get(first).compute,
                                input(first),
                                entries.get(other).compute,
                                input(other))
                        != 0;
    }

    private int compare(RequestContext a, RequestContext b) {
        int compared =
                priority
                        ? Integer.compare(SchedulingQueue.priority(b), SchedulingQueue.priority(a))
                        : 0;
        if (compared != 0) return compared;
        long left = entries.get(a).compute;
        long right = entries.get(b).compute;
        compared = ratio(left, input(a), right, input(b));
        if (compared == 0) compared = Long.compare(left, right);
        if (compared == 0) compared = Long.compare(input(a), input(b));
        return compared == 0 ? stableOrder.compare(a, b) : compared;
    }

    private void enqueue(RequestContext request, Entry entry) {
        entry.ticket =
                new SchedulingQueue.EstimateRequest(
                        request,
                        estimate -> {
                            if (entries.get(request) != entry || entry.ticket == null) return;
                            if (known(request)) ranked.remove(request);
                            entry.compute = estimate.orElse(-1L);
                            if (stable.contains(request) && known(request)) ranked.add(request);
                            entry.ticket = null;
                            entry.estimatedAtNanos = System.nanoTime();
                        });
        refresh.add(entry.ticket);
    }

    /**
     * Owns the cached work estimate and outstanding refresh for one queue membership.
     */
    private static final class Entry {
        private long compute = -1L;
        private SchedulingQueue.EstimateRequest ticket;
        private long estimatedAtNanos;
    }
}
