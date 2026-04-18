package syncer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type Scheduler struct {
	control  *ControlPlane
	logger   *slog.Logger
	interval time.Duration
}

func NewScheduler(control *ControlPlane, logger *slog.Logger, interval time.Duration) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}

	return &Scheduler{
		control:  control,
		logger:   logger,
		interval: interval,
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	if s.interval <= 0 {
		return
	}

	ticker := time.NewTicker(s.interval)
	s.logger.Info("sync scheduler started", "interval", s.interval.String())

	go func() {
		defer ticker.Stop()
		defer s.logger.Info("sync scheduler stopped")

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				triggerCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				run, err := s.control.TriggerIncremental(triggerCtx, "scheduler")
				cancel()
				if err != nil {
					if errors.Is(err, ErrSyncAlreadyRunning) {
						s.logger.Warn("scheduler skipped sync trigger", "reason", "already_running")
						continue
					}
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						s.logger.Warn("scheduler trigger canceled", "error", err)
						continue
					}
					s.logger.Error("scheduler trigger failed", "error", err)
					continue
				}

				s.logger.Info("scheduler triggered sync run", "run_id", run.ID, "status", run.Status, "triggered_by", run.TriggeredBy)
			}
		}
	}()
}

func ParseScheduleInterval(schedule string) (enabled bool, interval time.Duration, err error) {
	raw := strings.TrimSpace(strings.ToLower(schedule))
	if raw == "" {
		return true, 24 * time.Hour, nil
	}

	switch raw {
	case "disabled", "off", "none", "false", "0":
		return false, 0, nil
	case "@daily":
		return true, 24 * time.Hour, nil
	case "@hourly":
		return true, time.Hour, nil
	}

	if strings.HasPrefix(raw, "@every") {
		value := strings.TrimSpace(strings.TrimPrefix(raw, "@every"))
		d, parseErr := time.ParseDuration(value)
		if parseErr != nil {
			return false, 0, fmt.Errorf("parse sync schedule %q: %w", schedule, parseErr)
		}
		if d < time.Minute {
			return false, 0, fmt.Errorf("sync schedule interval must be at least 1m")
		}
		return true, d, nil
	}

	d, parseErr := time.ParseDuration(raw)
	if parseErr != nil {
		return false, 0, fmt.Errorf("parse sync schedule %q: %w", schedule, parseErr)
	}
	if d < time.Minute {
		return false, 0, fmt.Errorf("sync schedule interval must be at least 1m")
	}

	return true, d, nil
}
