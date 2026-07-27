package link

import (
	"context"
	"fmt"
)

func buildHomeControlRouter(
	a *Acceptor,
	reqCtx context.Context,
	record *sessionRecord,
	sessionGate func() bool,
) (*controlRouter, error) {
	sendPlanReply := func(in controlDispatchInput, requestID string, reply PlanReply) {
		raw, _ := encodeControl(controlFrame{
			RequestID: requestID, Kind: ctrlPlanReply, PlanReply: &reply,
		})
		_ = in.link.sendControl(raw)
	}
	rows := map[controlKind]controlRoute{
		ctrlAttach: {
			parse: parseAttach, execution: controlExecutionWorker,
			gate: controlGateNone, state: controlStateCandidate,
			handle: func(in controlDispatchInput, msg controlMessage) {
				request := msg.payload.(*AttachRequest)
				reply := AttachReply{
					ChannelID: a.channelID, DaemonID: in.peerID,
					Generation: in.session.generation,
				}
				if request.Proto != 2 {
					reply.Reason = fmt.Sprintf("link: unsupported attach proto %d", request.Proto)
					raw, _ := encodeControl(controlFrame{
						RequestID: msg.requestID, Kind: ctrlAttachReply, AttachReply: &reply,
					})
					_ = in.link.sendControl(raw)
					in.session.report(SessionAdmissionRejected, "attach_proto_unsupported", nil)
					return
				}
				if err := a.canAttach(reqCtx, in.peerID); err != nil {
					reply.Reason = err.Error()
					raw, _ := encodeControl(controlFrame{
						RequestID: msg.requestID, Kind: ctrlAttachReply, AttachReply: &reply,
					})
					_ = in.link.sendControl(raw)
					in.session.report(SessionAdmissionRejected, "attach_policy_rejected", err)
					return
				}
				if _, err := a.sessions.activate(in.session); err != nil {
					in.session.report(SessionProtocolViolation, "duplicate_or_late_attach", err)
					return
				}
				reply.Accepted = true
				raw, _ := encodeControl(controlFrame{
					RequestID: msg.requestID, Kind: ctrlAttachReply, AttachReply: &reply,
				})
				if err := in.link.sendControl(raw); err != nil {
					in.session.report(SessionSpineLost, "accepted_reply_write_failed", err)
					return
				}
				go a.probeSession(in.session, in.link)
			},
			busy: func(in controlDispatchInput, msg controlMessage) {
				reply := AttachReply{
					ChannelID: a.channelID, DaemonID: in.peerID,
					Generation: in.session.generation,
					Reason:     "link: control task pool busy",
				}
				raw, _ := encodeControl(controlFrame{
					RequestID: msg.requestID, Kind: ctrlAttachReply, AttachReply: &reply,
				})
				_ = in.link.sendControl(raw)
				in.session.report(SessionAdmissionRejected, "attach_task_pool_busy", nil)
			},
		},
		ctrlPlanPull: {
			parse: parsePlanPull, execution: controlExecutionWorker,
			gate: controlGateSession, state: controlStateSessionGate,
			handle: func(in controlDispatchInput, msg controlMessage) {
				reply := PlanReply{}
				var err error
				reply.Actors, err = a.plan(reqCtx, in.peerID)
				if err != nil {
					reply.Error = err.Error()
				}
				sendPlanReply(in, msg.requestID, reply)
			},
			gateReject: func(in controlDispatchInput, msg controlMessage) {
				sendPlanReply(in, msg.requestID, PlanReply{Error: "link: plan pull outside active session"})
			},
			busy: func(in controlDispatchInput, msg controlMessage) {
				sendPlanReply(in, msg.requestID, PlanReply{Error: "link: control task pool busy"})
			},
		},
		ctrlAllocReply: {
			parse: parseAllocReply, execution: controlExecutionInline,
			gate: controlGateNone, state: controlStateNone,
			handle: func(_ controlDispatchInput, msg controlMessage) {
				reply := msg.payload.(*AllocReply)
				a.pendingAlloc.deliver(reply.RequestID, *reply)
			},
		},
		ctrlReclaimReply: {
			parse: parseReclaimReply, execution: controlExecutionInline,
			gate: controlGateNone, state: controlStateNone,
			handle: func(_ controlDispatchInput, msg controlMessage) {
				reply := msg.payload.(*ReclaimReply)
				a.pendingReclaim.deliver(reply.RequestID, *reply)
			},
		},
		ctrlCommitted: {
			parse: parseCommitted, execution: controlExecutionWorker,
			gate: controlGateNone, state: controlStateNone,
			handle: func(in controlDispatchInput, msg controlMessage) {
				a.handleCommitted(reqCtx, in.link, in.peerID, msg.payload.(*Committed))
			},
			busy: func(in controlDispatchInput, msg controlMessage) {
				a.sendCommittedBusy(in.link, msg.payload.(*Committed).RequestID)
			},
		},
		ctrlReclaimAck: {
			parse: parseReclaimAck, execution: controlExecutionWorker,
			gate: controlGateNone, state: controlStateNone,
			handle: func(in controlDispatchInput, msg controlMessage) {
				a.handleReclaimAck(reqCtx, in.link, in.peerID, msg.payload.(*ReclaimAck))
			},
			busy: func(in controlDispatchInput, msg controlMessage) {
				a.sendReclaimAckBusy(in.link, msg.payload.(*ReclaimAck).RequestID)
			},
		},
		ctrlReconcilePull: {
			parse: parseReconcilePull, execution: controlExecutionWorker,
			gate: controlGateNone, state: controlStateNone,
			handle: func(in controlDispatchInput, msg controlMessage) {
				a.handleReconcilePull(reqCtx, in.link, in.peerID, msg.payload.(*ReconcilePull))
			},
			busy: func(in controlDispatchInput, msg controlMessage) {
				a.sendReconcileBusy(in.link, msg.payload.(*ReconcilePull).RequestID)
			},
		},
		ctrlResolveCoord: {
			parse: parseResolveCoord, execution: controlExecutionInline,
			gate: controlGateNone, state: controlStateNone,
			handle: func(in controlDispatchInput, msg controlMessage) {
				reply := a.handleResolveCoord(in.peerID, msg.payload.(*ResolveCoordRequest))
				raw, _ := encodeLaneControl(laneControlFrame{
					Kind: ctrlResolveCoordReply, ResolveCoordReply: &reply,
				})
				_ = in.link.sendControl(raw)
			},
		},
		ctrlProbe:      probeRoute(parseProbe, false),
		ctrlProbeReply: probeRoute(parseProbeReply, true),
	}
	return newControlRouter(
		homeKnownControlKinds, rows, sessionGate,
		func(kind controlKind, err error) {
			detail := "malformed_" + string(kind)
			if kind == "" {
				detail = "control_frame_missing_kind"
			}
			record.report(SessionProtocolViolation, detail, err)
		},
		func(kind controlKind) {
			a.logger.Warn("link.unknown_control_kind",
				"generation", record.generation, "key", record.key, "kind", string(kind))
		},
	)
}

func buildDaemonControlRouter(d *Dialer) (*controlRouter, error) {
	rows := map[controlKind]controlRoute{
		ctrlAttachReply: {
			parse: parseAttachReply, execution: controlExecutionInline,
			gate: controlGateNone, state: controlStateNone,
			handle: func(_ controlDispatchInput, msg controlMessage) {
				reply := msg.payload.(*AttachReply)
				if !d.pendingAttach.deliver(msg.requestID, *reply) {
					d.onSessionEvidence(SessionProtocolViolation, "uncorrelated_attach_reply", nil)
				}
			},
		},
		ctrlPlanReply: {
			parse: parsePlanReply, execution: controlExecutionInline,
			gate: controlGateNone, state: controlStateNone,
			handle: func(_ controlDispatchInput, msg controlMessage) {
				reply := msg.payload.(*PlanReply)
				d.pendingPlan.deliver(msg.requestID, *reply)
			},
		},
		ctrlPlanPoke: {
			parse: parsePlanPoke, execution: controlExecutionInline,
			gate: controlGateNone, state: controlStateNone,
			handle: func(controlDispatchInput, controlMessage) { d.signalPlanChanged() },
		},
		ctrlAllocRequest: {
			parse: parseAllocRequest, execution: controlExecutionWorker,
			gate: controlGateNone, state: controlStateNone,
			handle: func(_ controlDispatchInput, msg controlMessage) {
				d.handleAllocRequest(*msg.payload.(*AllocRequest))
			},
			busy: func(_ controlDispatchInput, msg controlMessage) {
				d.sendAllocBusy(msg.payload.(*AllocRequest).RequestID)
			},
		},
		ctrlReclaimRequest: {
			parse: parseReclaimRequest, execution: controlExecutionWorker,
			gate: controlGateNone, state: controlStateNone,
			handle: func(_ controlDispatchInput, msg controlMessage) {
				d.handleReclaimRequest(*msg.payload.(*ReclaimRequest))
			},
			busy: func(_ controlDispatchInput, msg controlMessage) {
				d.sendReclaimBusy(msg.payload.(*ReclaimRequest).RequestID)
			},
		},
		ctrlCommittedReply: inlineReplyRoute(parseCommittedReply, func(msg controlMessage) {
			reply := msg.payload.(*CommittedReply)
			d.pendingCommitted.deliver(reply.RequestID, *reply)
		}),
		ctrlReclaimAckReply: inlineReplyRoute(parseReclaimAckReply, func(msg controlMessage) {
			reply := msg.payload.(*ReclaimAckReply)
			d.pendingReclaim.deliver(reply.RequestID, *reply)
		}),
		ctrlReconcilePullReply: inlineReplyRoute(parseReconcilePullReply, func(msg controlMessage) {
			reply := msg.payload.(*ReconcilePullReply)
			d.pendingReconcile.deliver(reply.RequestID, *reply)
		}),
		ctrlResolveCoordReply: inlineReplyRoute(parseResolveCoordReply, func(msg controlMessage) {
			reply := msg.payload.(*ResolveCoordReply)
			d.pendingResolveCoord.deliver(reply.RequestID, *reply)
		}),
		ctrlProbe:      probeRoute(parseProbe, false),
		ctrlProbeReply: probeRoute(parseProbeReply, true),
	}
	return newControlRouter(
		daemonKnownControlKinds, rows, nil,
		func(kind controlKind, err error) {
			detail := "malformed_" + string(kind)
			if kind == "" {
				detail = "control_frame_missing_kind"
			}
			d.onSessionEvidence(SessionProtocolViolation, detail, err)
		},
		func(kind controlKind) {
			d.logger.Warn("link.unknown_control_kind", "kind", string(kind))
		},
	)
}

func inlineReplyRoute(
	parse func([]byte) (controlMessage, error),
	deliver func(controlMessage),
) controlRoute {
	return controlRoute{
		parse: parse, execution: controlExecutionInline,
		gate: controlGateNone, state: controlStateNone,
		handle: func(_ controlDispatchInput, msg controlMessage) { deliver(msg) },
	}
}

func probeRoute(
	parse func([]byte) (controlMessage, error),
	reply bool,
) controlRoute {
	return controlRoute{
		parse: parse, execution: controlExecutionInline,
		gate: controlGateNone, state: controlStateProbe,
		handle: func(in controlDispatchInput, msg controlMessage) {
			if reply {
				in.link.handleProbeReply(msg.payload.(*ProbeReply))
				return
			}
			in.link.handleProbe(msg.payload.(*Probe))
		},
	}
}
