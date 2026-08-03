package config

import (
	"fmt"
	"time"
)

// NowMicros returns the current time as microseconds since the Unix epoch (UTC),
// the encoding Delta TIMESTAMP columns expect.
func NowMicros() int64 {
	return time.Now().UnixMicro()
}

// MakeOrderJSON builds one order record as a JSON string matching the example
// `orders` table columns: id INT, customer_name STRING, product_name STRING,
// quantity INT, price DOUBLE, status STRING, created_at TIMESTAMP,
// updated_at TIMESTAMP.
func MakeOrderJSON(id int, customer, product string, quantity int, price float64, status string, ts int64) string {
	return fmt.Sprintf(
		`{"id": %d, "customer_name": %q, "product_name": %q, "quantity": %d, `+
			`"price": %g, "status": %q, "created_at": %d, "updated_at": %d}`,
		id, customer, product, quantity, price, status, ts, ts)
}
