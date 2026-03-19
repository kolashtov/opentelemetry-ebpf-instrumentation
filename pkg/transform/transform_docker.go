// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package transform // import "go.opentelemetry.io/obi/pkg/transform"

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/obi/pkg/appolly/app"
	"go.opentelemetry.io/obi/pkg/appolly/app/request"
	"go.opentelemetry.io/obi/pkg/appolly/app/svc"
	"go.opentelemetry.io/obi/pkg/appolly/discover/exec"
	"go.opentelemetry.io/obi/pkg/docker"
	attr "go.opentelemetry.io/obi/pkg/export/attributes/names"
	"go.opentelemetry.io/obi/pkg/pipe/global"
	"go.opentelemetry.io/obi/pkg/pipe/msg"
	"go.opentelemetry.io/obi/pkg/pipe/swarm"
	"go.opentelemetry.io/obi/pkg/pipe/swarm/swarms"
)

func delog() *slog.Logger {
	return slog.With("component", "transform.DockerEnricher")
}

func DockerDecoratorProvider(
	ctxInfo *global.ContextInfo,
	input, output *msg.Queue[[]request.Span],
) swarm.InstanceFunc {
	return func(ctx context.Context) (swarm.RunFunc, error) {
		// only enable this node if Docker is available, but also
		// if we aren't running on Kubernetes
		if ctxInfo.K8sInformer.IsKubeEnabled() ||
			!ctxInfo.DockerMetadata.IsEnabled(ctx) {
			return swarm.Bypass(input, output)
		}

		dd := dockerEnricher{
			in:             input.Subscribe(msg.SubscriberName("DockerEnricher")),
			out:            output,
			containerByPID: map[app.PID]docker.ContainerMeta{},
			log:            delog(),
			docker:         ctxInfo.DockerMetadata,
		}
		return dd.decorate, nil
	}
}

type dockerEnricher struct {
	in             <-chan []request.Span
	out            *msg.Queue[[]request.Span]
	containerByPID map[app.PID]docker.ContainerMeta
	log            *slog.Logger
	docker         *docker.ContainerStore
}

func (dd *dockerEnricher) decorate(ctx context.Context) {
	defer dd.out.Close()
	swarms.ForEachInput(ctx, dd.in, dd.log.Debug, func(spans []request.Span) {
		for i := range spans {
			svc := &spans[i].Service
			if _, hasContainer := svc.Metadata[attr.ContainerName]; hasContainer {
				continue
			}
			if ci, ok := dd.containerInfo(ctx, svc.ProcPID); ok {
				ci.DecorateService(svc)
			}
		}
		dd.out.SendCtx(ctx, spans)
	})
}

func (dd *dockerEnricher) containerInfo(ctx context.Context, pid app.PID) (docker.ContainerMeta, bool) {
	if ci, ok := dd.containerByPID[pid]; ok {
		return ci, true
	}
	ci, ok := dd.docker.ContainerInfo(ctx, pid)
	if ok {
		dd.containerByPID[pid] = ci
	} else {
		dd.log.Debug("can't find container metadata", "pid", pid)
	}
	return ci, ok
}

func dpelog() *slog.Logger {
	return slog.With("component", "transform.DockerProcessEventDecorator")
}

func DockerProcessEventDecoratorProvider(
	ctxInfo *global.ContextInfo,
	input, output *msg.Queue[exec.ProcessEvent],
) swarm.InstanceFunc {
	return func(ctx context.Context) (swarm.RunFunc, error) {
		// only enable this node if Docker is available, but also
		// if we aren't running on Kubernetes
		if ctxInfo.K8sInformer.IsKubeEnabled() ||
			!ctxInfo.DockerMetadata.IsEnabled(ctx) {
			return swarm.Bypass(input, output)
		}

		in := input.Subscribe()
		containers := ctxInfo.DockerMetadata
		containerByPID := map[app.PID]docker.ContainerMeta{}

		return func(ctx context.Context) {
			defer output.Close()
			swarms.ForEachInput(ctx, in, dpelog().Debug, func(ev exec.ProcessEvent) {
				if ev.File == nil {
					return
				}
				switch ev.Type {
			case exec.ProcessEventCreated:
				ci, ok := containerByPID[ev.File.Pid]
				source := "cache"
				if !ok {
					if ci, ok = containers.ContainerInfo(ctx, ev.File.Pid); ok {
						containerByPID[ev.File.Pid] = ci
						source = "docker-api"
					}
				}
				if !ok {
					if ci, ok = containerMetaFromAttrs(&ev.File.Service); ok {
						source = "svc-metadata"
					}
				}
				if ok {
					ci.DecorateService(&ev.File.Service)
				}
				dpelog().Info("process event docker", "pid", ev.File.Pid, "found", ok, "source", source,
					"svcName", ev.File.Service.UID.Name, "svcMetadata", ev.File.Service.Metadata)
				case exec.ProcessEventTerminated:
					delete(containerByPID, ev.File.Pid)
				}
				output.SendCtx(ctx, ev)
			})
		}, nil
	}
}

func containerMetaFromAttrs(s *svc.Attrs) (docker.ContainerMeta, bool) {
	if s.Metadata == nil {
		return docker.ContainerMeta{}, false
	}
	name, hasName := s.Metadata[attr.ContainerName]
	if !hasName || name == "" {
		return docker.ContainerMeta{}, false
	}
	id, _ := s.Metadata[attr.ContainerID]
	return docker.ContainerMeta{Name: name, ID: id}, true
}
