package internal

import "github.com/google/uuid"

type Task struct {
	ID uuid.UUID `json:"id"`
}
