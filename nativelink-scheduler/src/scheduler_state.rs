use nativelink_util::{action_messages::{ActionInfo, ActionState, OperationId}, platform_properties::PlatformPropertyValue};
pub trait WorkerSchedulerState {
    /// Refreshes the lifetime of the worker with the given timestamp.
    fn refresh_worker_lifetime(
        &self,
        worker_id: &WorkerId,
        timestamp: WorkerTimestamp,
    ) -> Result<(), Error>;
    /// Adds a worker to the pool.
    /// Note: This function will not do any task matching.
    fn add_worker(&self, worker: Worker) -> Result<(), Error>;
    fn remove_worker(&self, worker_id: &WorkerId) -> Option<Worker>;
    fn find_worker_for_action_mut<'a>(
        &'a self,
        operation_id: &OperationId,
    ) -> Option<&WorkerId> {
        assert!(matches!(
            awaited_action.current_state.stage,
            ActionStage::Queued
        ));
        let action_properties = &awaited_action.action_info.platform_properties;
        let mut workers_iter = self.workers.iter_mut();
        let workers_iter = match self.allocation_strategy {
            // Use rfind to get the least recently used that satisfies the properties.
            WorkerAllocationStrategy::least_recently_used => workers_iter.rfind(|(_, w)| {
                w.can_accept_work() && action_properties.is_satisfied_by(&w.platform_properties)
            }),
            // Use find to get the most recently used that satisfies the properties.
            WorkerAllocationStrategy::most_recently_used => workers_iter.find(|(_, w)| {
                w.can_accept_work() && action_properties.is_satisfied_by(&w.platform_properties)
            }),
        };
        let worker_id = workers_iter.map(|(_, w)| &w.id);
        // We need to "touch" the worker to ensure it gets re-ordered in the LRUCache, since it was selected.
        if let Some(&worker_id) = worker_id {
            self.workers.get_mut(&worker_id)
        } else {
            None
        }
    }
}


pub trait ActionSchedulerState {
    // Find action w/ add if didn't exist
    /// Adds an action to the scheduler for remote execution.
    async fn get_or_create_action(
        &self,
        action_info: ActionInfo,
    ) -> Result<OpertionId, watch::Receiver<Arc<ActionState>>, Error>;

    /// Find an existing action by its name.
    async fn find_existing_action(
        &self,
        action_id: &OperationId,
    ) -> Option<watch::Receiver<Arc<ActionState>>>;

    /// Cleans up the cache of recently completed actions.
    async fn clean_recently_completed_actions(&self);

    /// Register the metrics for the action scheduler.
    fn register_metrics(self: Arc<Self>, _registry: &mut Registry) {}
}
