package api

const inputProperties = `"input_id":{"type":"string"},"seq":{"type":"integer","minimum":1},"text":{"type":"string"},"attachments":{"type":"array"},"caller_channel":{"type":"string"},"caller_actor":{"type":"string"},"origin":{"type":"object","required":["session"],"properties":{"session":{"type":"string"},"label":{"type":"string"}},"additionalProperties":false}`

const StartInputSchema = `{"type":"object","required":["work_id","assignment_id","controller_actor","inputs","context_actor","llm_actor"],"properties":{"work_id":{"type":"string","minLength":1},"assignment_id":{"type":"string","minLength":1},"controller_actor":{"type":"string","minLength":1},"inputs":{"type":"array","minItems":1,"items":{"type":"object","required":["input_id","seq","text"],"properties":{` + inputProperties + `},"additionalProperties":false}},"prior":{"type":"array"},"context_actor":{"type":"string","minLength":1},"llm_actor":{"type":"string","minLength":1},"workspace_actor":{"type":"string"},"host_actor":{"type":"string"},"model":{"type":"string"},"max_turns":{"type":"integer","minimum":1},"tools":{"type":"array","items":{"type":"object","required":["name","actor","word"],"properties":{"name":{"type":"string","minLength":1},"actor":{"type":"string","minLength":1},"word":{"type":"string","minLength":1}},"additionalProperties":false}},"tool_result_max_lines":{"type":"integer","minimum":1},"tool_result_max_bytes":{"type":"integer","minimum":1},"tool_image_max_bytes":{"type":"integer","minimum":1}},"additionalProperties":false}`

const InputInputSchema = `{"type":"object","required":["work_id","assignment_id","input"],"properties":{"work_id":{"type":"string","minLength":1},"assignment_id":{"type":"string","minLength":1},"input":{"type":"object","required":["input_id","seq","text"],"properties":{` + inputProperties + `},"additionalProperties":false}},"additionalProperties":false}`

const StopInputSchema = `{"type":"object","required":["work_id","assignment_id"],"properties":{"work_id":{"type":"string","minLength":1},"assignment_id":{"type":"string","minLength":1},"reason":{"type":"string"}},"additionalProperties":false}`

const InspectInputSchema = `{"type":"object","required":["work_id"],"properties":{"work_id":{"type":"string","minLength":1},"assignment_id":{"type":"string"}},"additionalProperties":false}`

const AckOutputSchema = `{"type":"object","required":["disposition"],"properties":{"disposition":{"type":"string"},"work_id":{"type":"string"},"assignment_id":{"type":"string"},"input_id":{"type":"string"},"included":{"type":"boolean"}},"additionalProperties":true}`

const InspectOutputSchema = `{"type":"object","required":["work_id","assignment_id","phase","input_count"],"properties":{"work_id":{"type":"string"},"assignment_id":{"type":"string"},"phase":{"type":"string"},"input_count":{"type":"integer"}},"additionalProperties":false}`
