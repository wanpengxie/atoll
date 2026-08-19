package actorctl

import "github.com/wanpengxie/atoll/protocol/actor"

type AdmitResult struct {
	ActorID actor.ActorID `json:"actor_id"`
	Created bool          `json:"created"`
}

type IntroduceResult = AdmitResult

type RemoveResult struct {
	Removed []actor.ActorID `json:"removed"`
}
