package orders

type OrderStatus string

const (
	NewOrderStatus       OrderStatus = "new"
	PaidOrderStatus      OrderStatus = "paid"
	CompletedOrderStatus OrderStatus = "completed"
	CanceledOrderStatus  OrderStatus = "canceled"
)

type Order struct {
	ID     int         `json:"id"`
	Status OrderStatus `json:"status"`
}
