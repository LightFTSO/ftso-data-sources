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

func ItemInSlice[K comparable](k K, list []K) bool {
	for _, v := range list {
		if v == k {
			return true
		}
	}
	return false
}

func RemoveFromSlice[T comparable](l []T, item T) []T {
	for i, other := range l {
		if other == item {
			return append(l[:i], l[i+1:]...)
		}
	}
	return l
}
