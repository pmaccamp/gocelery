func GetUuidString() string {
	uuid, error := uuid.NewV4()

	return uuid.String();
}