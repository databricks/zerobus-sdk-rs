// Package exampleutil contains shared example data helpers.
package exampleutil

import (
	"fmt"
	"time"
)

// NowMicros returns the current Unix timestamp in microseconds.
func NowMicros() int64 {
	return time.Now().UnixMicro()
}

// MakeOrderJSON builds a JSON record for the example orders table.
func MakeOrderJSON(id int, customer, product string, quantity int, price float64, status string, ts int64) string {
	return fmt.Sprintf(
		`{"id": %d, "customer_name": %q, "product_name": %q, "quantity": %d, `+
			`"price": %g, "status": %q, "created_at": %d, "updated_at": %d}`,
		id, customer, product, quantity, price, status, ts, ts)
}
