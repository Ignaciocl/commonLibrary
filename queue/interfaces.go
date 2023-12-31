package queue

type CheckableZero interface {
	IsZero() bool
}
type Queue[T CheckableZero] interface {
	SendMessage(data T) error
	ReceiveMessage() (T, error)
	CreateReceiver() <-chan T
	AddReceiver() <-chan T
}
