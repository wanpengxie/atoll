package driverproto

import "fmt"

type Certainty uint8

const (
	Definitive Certainty = iota
	Ambiguous
)

type WorkerDisposition uint8

const (
	KeepWorker WorkerDisposition = iota
	RetireWorker
)

type FailureClass uint8

const (
	FailureInvalidInput FailureClass = iota
	FailureOverloaded
	FailureProvider
	FailureTransport
)

type OpenVerdict uint8

const (
	OpenReady OpenVerdict = iota
	OpenResumeInvalid
	OpenRejected
	OpenAmbiguous
)

type StartVerdict uint8

const (
	StartAccepted StartVerdict = iota
	StartRejected
	StartResumeInvalid
)

type ControlVerdict uint8

const (
	ControlAccepted ControlVerdict = iota
	ControlRejected
	ControlNotSteerable
	ControlTargetGone
)

type resultCore struct {
	valid       bool
	certainty   Certainty
	disposition WorkerDisposition
	failure     FailureClass
	detail      string
}

type OpenResult struct {
	core    resultCore
	verdict OpenVerdict
}

type StartResult struct {
	core    resultCore
	verdict StartVerdict
}

type ControlResult struct {
	core    resultCore
	verdict ControlVerdict
}

func Ready() OpenResult { return openResult(OpenReady, Definitive, KeepWorker, FailureProvider, "") }
func ResumeInvalid(detail string) OpenResult {
	return openResult(OpenResumeInvalid, Definitive, RetireWorker, FailureProvider, detail)
}
func OpenReject(class FailureClass, detail string, disposition WorkerDisposition) OpenResult {
	return openResult(OpenRejected, Definitive, disposition, class, detail)
}
func OpenUncertain(class FailureClass, detail string) OpenResult {
	return openResult(OpenAmbiguous, Ambiguous, RetireWorker, class, detail)
}

func StartAccept(disposition WorkerDisposition) StartResult {
	return startResult(StartAccepted, Definitive, disposition, FailureProvider, "")
}
func StartReject(class FailureClass, detail string, disposition WorkerDisposition) StartResult {
	return startResult(StartRejected, Definitive, disposition, class, detail)
}
func StartInvalidResume(detail string) StartResult {
	return startResult(StartResumeInvalid, Definitive, RetireWorker, FailureProvider, detail)
}
func StartUncertain(class FailureClass, detail string) StartResult {
	return startResult(StartRejected, Ambiguous, RetireWorker, class, detail)
}

func ControlAccept(disposition WorkerDisposition) ControlResult {
	return controlResult(ControlAccepted, Definitive, disposition, FailureProvider, "")
}
func ControlReject(class FailureClass, detail string, disposition WorkerDisposition) ControlResult {
	return controlResult(ControlRejected, Definitive, disposition, class, detail)
}
func NotSteerable(detail string, disposition WorkerDisposition) ControlResult {
	return controlResult(ControlNotSteerable, Definitive, disposition, FailureProvider, detail)
}
func TargetGone(detail string, disposition WorkerDisposition) ControlResult {
	return controlResult(ControlTargetGone, Definitive, disposition, FailureProvider, detail)
}
func ControlUncertain(class FailureClass, detail string) ControlResult {
	return controlResult(ControlRejected, Ambiguous, RetireWorker, class, detail)
}

func openResult(v OpenVerdict, c Certainty, d WorkerDisposition, f FailureClass, detail string) OpenResult {
	return OpenResult{core: resultCore{valid: true, certainty: c, disposition: d, failure: f, detail: detail}, verdict: v}
}
func startResult(v StartVerdict, c Certainty, d WorkerDisposition, f FailureClass, detail string) StartResult {
	return StartResult{core: resultCore{valid: true, certainty: c, disposition: d, failure: f, detail: detail}, verdict: v}
}
func controlResult(v ControlVerdict, c Certainty, d WorkerDisposition, f FailureClass, detail string) ControlResult {
	return ControlResult{core: resultCore{valid: true, certainty: c, disposition: d, failure: f, detail: detail}, verdict: v}
}

func (r OpenResult) Verdict() OpenVerdict              { return r.verdict }
func (r StartResult) Verdict() StartVerdict            { return r.verdict }
func (r ControlResult) Verdict() ControlVerdict        { return r.verdict }
func (r OpenResult) Certainty() Certainty              { return r.core.certainty }
func (r StartResult) Certainty() Certainty             { return r.core.certainty }
func (r ControlResult) Certainty() Certainty           { return r.core.certainty }
func (r OpenResult) Disposition() WorkerDisposition    { return r.core.disposition }
func (r StartResult) Disposition() WorkerDisposition   { return r.core.disposition }
func (r ControlResult) Disposition() WorkerDisposition { return r.core.disposition }
func (r OpenResult) Failure() FailureClass             { return r.core.failure }
func (r StartResult) Failure() FailureClass            { return r.core.failure }
func (r ControlResult) Failure() FailureClass          { return r.core.failure }
func (r OpenResult) Detail() string                    { return r.core.detail }
func (r StartResult) Detail() string                   { return r.core.detail }
func (r ControlResult) Detail() string                 { return r.core.detail }

func (r OpenResult) Validate() error {
	if !r.core.valid || r.verdict > OpenAmbiguous {
		return fmt.Errorf("driverproto: invalid open result")
	}
	if (r.core.certainty == Ambiguous || r.verdict == OpenResumeInvalid) && r.core.disposition != RetireWorker {
		return fmt.Errorf("driverproto: unsafe open result")
	}
	if r.verdict == OpenAmbiguous && r.core.certainty != Ambiguous {
		return fmt.Errorf("driverproto: ambiguous open verdict without ambiguous certainty")
	}
	return nil
}

func (r StartResult) Validate() error {
	if !r.core.valid || r.verdict > StartResumeInvalid {
		return fmt.Errorf("driverproto: invalid start result")
	}
	if (r.core.certainty == Ambiguous || r.verdict == StartResumeInvalid) && r.core.disposition != RetireWorker {
		return fmt.Errorf("driverproto: unsafe start result")
	}
	return nil
}

func (r ControlResult) Validate() error {
	if !r.core.valid || r.verdict > ControlTargetGone {
		return fmt.Errorf("driverproto: invalid control result")
	}
	if r.core.certainty == Ambiguous && r.core.disposition != RetireWorker {
		return fmt.Errorf("driverproto: unsafe control result")
	}
	return nil
}
