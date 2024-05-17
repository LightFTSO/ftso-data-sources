package helpers

// Function to check if all elements in a slice of structs have the same value for a specific field.
func AreAllFieldsEqual[T any, F comparable](slice []T, fieldExtractor func(T) F) bool {
	if len(slice) == 0 {
		return true // An empty slice is considered as having all elements equal.
	}

	firstValue := fieldExtractor(slice[0])
	for _, element := range slice[1:] {
		if fieldExtractor(element) != firstValue {
			return false
		}
	}
	return true
}
