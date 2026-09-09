package api

const inputProperties = `"input_id":{"type":"string"},"seq":{"type":"integer","minimum":1},"text":{"type":"string"},"attachments":{"type":"array"},"caller_channel":{"type":"string"},"caller_actor":{"type":"string"},"origin":{"type":"object","required":["session"],"properties":{"session":{"type":"string"},"label":{"type":"string"}},"additionalProperties":false}`

const StartInputSchema = `{"type":"object","required":["session_id","turn_id","inputs","selection"],"properties":{"session_id":{"type":"string","minLength":1},"turn_id":{"type":"string","minLength":1},"open":{"type":"object"},"inputs":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},"selection":{"type":"object","properties":{"model":{"type":"string"}},"additionalProperties":false},"prompt":{"type":"string"},"tools":{"type":"array"},"limits":{"type":"object"}},"additionalProperties":false}`

const InputInputSchema = `{"type":"object","required":["session_id","turn_id","inputs"],"properties":{"session_id":{"type":"string","minLength":1},"turn_id":{"type":"string","minLength":1},"control_id":{"type":"string"},"inputs":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}},"additionalProperties":false}`

const StopInputSchema = `{"type":"object","required":["session_id"],"properties":{"session_id":{"type":"string","minLength":1},"turn_id":{"type":"string"},"archive":{"type":"boolean"},"reason":{"type":"string"}},"additionalProperties":false}`

const InspectInputSchema = `{"type":"object","required":["work_id"],"properties":{"session_id":{"type":"string"},"context_version":{"type":"integer"},"work_id":{"type":"string","minLength":1},"assignment_id":{"type":"string"}},"additionalProperties":false}`

const AckOutputSchema = `{"type":"object","required":["disposition"],"properties":{"disposition":{"type":"string"},"work_id":{"type":"string"},"assignment_id":{"type":"string"},"input_id":{"type":"string"},"included":{"type":"boolean"}},"additionalProperties":true}`

const InspectOutputSchema = `{"type":"object","required":["work_id","assignment_id","phase","input_count"],"properties":{"session_id":{"type":"string"},"context_version":{"type":"integer"},"work_id":{"type":"string"},"assignment_id":{"type":"string"},"phase":{"type":"string"},"input_count":{"type":"integer"}},"additionalProperties":true}`
