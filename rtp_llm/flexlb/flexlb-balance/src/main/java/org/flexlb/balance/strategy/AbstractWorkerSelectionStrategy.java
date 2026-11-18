package org.flexlb.balance.strategy;

import org.flexlb.balance.request.Response;
import org.flexlb.worker.endpoint.CapacityKey;
import org.flexlb.worker.endpoint.StrategyErrorType;
import org.flexlb.worker.endpoint.WorkerEndpoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Selects and registers one role, owning candidate refresh, exclusion and bounded version-conflict retries.
 * Subclasses provide ranking and role-specific registration; the loop returns only registered selections.
 */
public abstract class AbstractWorkerSelectionStrategy<I, C> {
    private static final int MAX_VERSION_CONFLICTS = 8;

    protected final SelectionResult<RoleSelection, CapacityKey> selectCandidates(
            org.flexlb.balance.schedule.RequestContext context,
            CapacityKey scope,
            List<I> initial,
            java.util.function.Function<List<I>, SelectionResult<C, CapacityKey>> choose) {
        return selectCandidates(
                context,
                scope,
                initial,
                (available, ranking) ->
                        available.isEmpty() ? SelectionResult.retry() : choose.apply(available));
    }

    /**
     * The chooser tries workers from the first list. The ranking list also includes workers
     * that refused registration for lack of capacity, so a blocked choice can identify whom to wait for.
     */
    protected final SelectionResult<RoleSelection, CapacityKey> selectCandidates(
            org.flexlb.balance.schedule.RequestContext context,
            CapacityKey scope,
            List<I> initial,
            BiFunction<List<I>, List<I>, SelectionResult<C, CapacityKey>> choose) {
        if (initial.isEmpty())
            return SelectionResult.blocked(
                    scope,
                    Response.error(
                            org.flexlb.worker.endpoint.StrategyErrorType.forRole(scope.role())),
                    null);
        List<I> candidates = initial;
        // Full workers can still determine the strategy's ranking. Unavailable workers cannot.
        List<I> rankingCandidates = initial;
        int conflicts = 0;
        SelectionResult<RoleSelection, CapacityKey> lastFull = null;
        while (true) {
            if (candidates.isEmpty() && lastFull == null) return SelectionResult.retry();
            var current = Collections.unmodifiableList(candidates);
            var choice =
                    choose.apply(
                            current,
                            candidates == rankingCandidates
                                    ? current
                                    : Collections.unmodifiableList(rankingCandidates));
            // A full candidate can fit after capacity is released, even if the remaining ones
            // cannot.
            if (lastFull != null
                    && (candidates.isEmpty() || choice.status() == SelectionResult.Status.REJECTED))
                return choice.candidateGenerations().isEmpty()
                        ? lastFull
                        : lastFull.withCandidates(choice.candidateGenerations());
            if (choice.status() != SelectionResult.Status.SUCCESS) return choice.asFailure();
            C selected = choice.value();
            I original = candidate(selected);
            int index = -1;
            for (int i = 0; i < candidates.size(); i++) {
                if (candidates.get(i) == original) {
                    index = i;
                    break;
                }
            }
            checkArgument(index >= 0, "strategy must select an original candidate");
            var accepted = register(context, scope.role(), scope.group(), selected);
            if (accepted.status() == WorkerEndpoint.RegistrationStatus.REGISTERED)
                return SelectionResult.success(accepted.value());
            if (accepted.status() == WorkerEndpoint.RegistrationStatus.CAPACITY_FULL) {
                lastFull = capacityFull(selected, scope);
            }
            if (candidates == initial) candidates = new ArrayList<>(initial);
            switch (accepted.status()) {
                case VERSION_CONFLICT, ALREADY_REGISTERED -> {
                    if (++conflicts >= MAX_VERSION_CONFLICTS)
                        return SelectionResult.retry(
                                Response.error(StrategyErrorType.SCHEDULER_PLAN_CONFLICT));
                    I refreshed = refresh(original);
                    if (refreshed == null) candidates.remove(index);
                    else candidates.set(index, refreshed);
                }
                case CAPACITY_FULL -> candidates.remove(index);
                case UNAVAILABLE -> {
                    candidates.remove(index);
                    if (rankingCandidates == initial) rankingCandidates = new ArrayList<>(initial);
                    rankingCandidates.removeIf(entry -> entry == original);
                }
                case REGISTERED ->
                        throw new IllegalStateException("registered selection already returned");
            }
        }
    }

    protected abstract I candidate(C selected);

    protected abstract WorkerEndpoint.Registration<RoleSelection> register(
            org.flexlb.balance.schedule.RequestContext context,
            org.flexlb.config.service.RoleType role,
            String group,
            C selected);

    protected SelectionResult<RoleSelection, CapacityKey> capacityFull(
            C selected, CapacityKey scope) {
        return SelectionResult.blocked(
                scope, Response.error(StrategyErrorType.RESOURCE_EXHAUSTED), null);
    }

    protected abstract I refresh(I candidate);
}
