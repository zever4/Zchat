//! Delivery, maintenance, FILEACK and file-status reads share one serial transport.
use super::*;
use crate::encryption::{delivery_runner::DeliveryIssue, native::RpcError};
pub(super) struct Queues {
    pub commands: mpsc::Receiver<super::commands::Command>,
    pub blobs: Option<mpsc::Receiver<super::blobs::Rpc>>,
    pub notices: Option<mpsc::Sender<crate::encryption::epoch_runner::AttachmentRetryNotice>>,
}

pub(super) async fn run<T>(
    worker: &CryptoCoordinator,
    runner: &mut EpochDeliveryRunner<T>,
    maintenance: &mut PreKeyMaintenance,
    mut stop: watch::Receiver<bool>,
    status: &watch::Sender<AccountStatus>,
    wake: &Notify,
    queues: Queues,
) -> Result<()>
where
    T: EpochDeliveryTransport + PreKeyMaintenanceTransport,
{
    let Queues {
        mut commands,
        mut blobs,
        notices,
    } = queues;
    let config = runner.config();
    let jitter = rand::random_range(0..=config.poll_interval.as_millis() as u64);
    let mut due = Instant::now() + Duration::from_millis(jitter);
    let mut failures = 0u32;
    let mut background_turn = 0u8;
    let mut file_acks = super::file_acks::Scheduler::new(config);
    let mut file_statuses = super::file_statuses::Scheduler::new(config);
    let mut foreground_turn = true;
    let mut blob_turn = true;
    loop {
        if *stop.borrow() || stop.has_changed().is_err() {
            return Ok(());
        }
        if foreground_turn && let Ok(command) = commands.try_recv() {
            super::commands::network(worker, runner.transport_mut(), command, &mut stop, wake)
                .await?;
            foreground_turn = false;
            continue;
        }
        if blob_turn
            && let Some(queue) = &mut blobs
            && let Ok(request) = queue.try_recv()
        {
            super::blobs::rpc(worker, runner.transport_mut(), request, &mut stop, wake).await?;
            blob_turn = false;
            foreground_turn = true;
            continue;
        }
        let now = Instant::now();
        let delivery_due = now >= due;
        let maintenance_due = now >= maintenance.next_due();
        let file_ack_due = now >= file_acks.next_due();
        let file_status_due = now >= file_statuses.next_due();
        let selected = (0..4)
            .map(|i| (background_turn + i) % 4)
            .find(|lane| match lane {
                0 => delivery_due,
                1 => maintenance_due,
                2 => file_ack_due,
                _ => file_status_due,
            });
        if selected == Some(0) {
            blob_turn = true;
            foreground_turn = true;
            background_turn = 1;
            if matches!(
                runner.poll_bounded(worker, &mut stop).await?,
                EpochPoll::Stopped
            ) {
                return Ok(());
            }
            for notice in runner.take_attachment_retries() {
                if let Some(queue) = &notices {
                    // A full queue loses only a hint. Next exact Send rediscovers it.
                    let _ = queue.try_send(notice);
                }
            }
            let report = runner.last_report().expect("completed account poll");
            failures = if report.delivery.issues.is_empty() {
                0
            } else {
                failures.saturating_add(1)
            };
            let health = if report.delivery.issues.is_empty() {
                DeliveryHealth::Healthy
            } else if report
                .delivery
                .issues
                .iter()
                .any(|i| matches!(i.error, DeliveryIssue::Timeout))
            {
                DeliveryHealth::RoundTimeout
            } else if report
                .delivery
                .issues
                .iter()
                .any(|i| matches!(i.error, DeliveryIssue::Rpc(RpcError::LocalDeadline)))
            {
                DeliveryHealth::LocalDeadline
            } else {
                DeliveryHealth::Retry
            };
            status.send_modify(|s| {
                s.delivery_rounds = s.delivery_rounds.saturating_add(1);
                s.incoming_message_revision = runner.incoming_message_revision();
                s.bootstrap_completions = s
                    .bootstrap_completions
                    .saturating_add(report.bootstrapped as u64);
                s.delivery_health = Some(health);
            });
            let max = config.delay(failures).as_millis() as u64;
            due = Instant::now() + Duration::from_millis(rand::random_range(max.div_ceil(2)..=max));
        } else if selected == Some(1) {
            blob_turn = true;
            foreground_turn = true;
            background_turn = 2;
            let step = maintenance
                .tick(worker, runner.transport_mut(), &mut stop)
                .await?;
            let health = match step {
                MaintenanceStep::Stopped => return Ok(()),
                MaintenanceStep::Halted => return Err(AccountError::Runtime),
                MaintenanceStep::Waiting => continue,
                MaintenanceStep::Healthy => MaintenanceHealth::Healthy,
                MaintenanceStep::CapacityExhausted => MaintenanceHealth::CapacityExhausted,
                MaintenanceStep::Planned(_) | MaintenanceStep::Uploaded { .. } => {
                    MaintenanceHealth::Pending
                }
                MaintenanceStep::Retry(MaintenanceError::Rpc(RpcError::LocalDeadline)) => {
                    MaintenanceHealth::LocalDeadline
                }
                MaintenanceStep::Retry(_) => MaintenanceHealth::Retry,
                MaintenanceStep::TimedOut => MaintenanceHealth::TimedOut,
            };
            status.send_modify(|s| {
                s.maintenance_steps = s.maintenance_steps.saturating_add(1);
                s.maintenance_health = Some(health);
            });
        } else if selected == Some(2) {
            blob_turn = true;
            foreground_turn = true;
            background_turn = 3;
            match file_acks
                .tick(worker, runner.transport_mut(), &mut stop)
                .await?
            {
                super::file_acks::Step::Stopped => return Ok(()),
                super::file_acks::Step::Complete { saved, health } => status.send_modify(|s| {
                    s.file_ack_steps = s.file_ack_steps.saturating_add(1);
                    s.file_acks = s.file_acks.saturating_add(u64::from(saved));
                    s.file_ack_health = Some(health);
                }),
            }
            tokio::task::yield_now().await;
        } else if selected == Some(3) {
            blob_turn = true;
            foreground_turn = true;
            background_turn = 0;
            match file_statuses
                .tick(worker, runner.transport_mut(), &mut stop)
                .await?
            {
                super::file_statuses::Step::Stopped => return Ok(()),
                super::file_statuses::Step::Complete { observed, health } => {
                    status.send_modify(|s| {
                        s.file_status_steps = s.file_status_steps.saturating_add(1);
                        s.file_status_reads =
                            s.file_status_reads.saturating_add(u64::from(observed));
                        s.file_status_health = Some(health);
                    })
                }
            }
            tokio::task::yield_now().await;
        } else {
            tokio::select! {
                biased;
                _ = super::super::delivery_runner::cancelled(&mut stop) => return Ok(()),
                _ = tokio::time::sleep_until(due.min(maintenance.next_due()).min(file_acks.next_due()).min(file_statuses.next_due())) => {},
                _ = wake.notified(), if failures == 0 => { due = Instant::now(); },
                request = super::blobs::receive(&mut blobs) => {
                    let Some(request) = request else { blobs = None; continue; };
                    super::blobs::rpc(worker, runner.transport_mut(), request, &mut stop, wake).await?;
                    blob_turn = false;
                    foreground_turn = true;
                },
                command = commands.recv() => {
                    let Some(command) = command else {return Ok(());};
                    super::commands::network(worker,runner.transport_mut(),command,&mut stop,wake).await?;
                    foreground_turn = false;
                },
            }
        }
    }
}
