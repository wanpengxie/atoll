package link

import "errors"

type codedSentinel struct{ code, message string }

func (e codedSentinel) Error() string     { return e.message }
func (e codedSentinel) ErrorCode() string { return e.code }

func decodeAckError(code, message string) error {
	if code == "" && message == "" {
		return nil
	}
	switch code {
	case "writer_not_live":
		return ErrWriterNotLive
	case "access_not_live":
		return ErrAccessNotLive
	case "schedule_not_live":
		return ErrScheduleNotLive
	default:
		if message == "" {
			message = code
		}
		return errors.New(message)
	}
}
