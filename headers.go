package blue_green_kafka

func ExtractHeaders(headers []Header) map[string]interface{} {
	result := map[string]interface{}{}
	for _, h := range headers {
		result[h.Key] = string(h.Value)
	}
	return result
}

func BuildHeaders(ctxData map[string]string) []Header {
	var result []Header
	for k, v := range ctxData {
		result = append(result, Header{Key: k, Value: []byte(v)})
	}
	return result
}
