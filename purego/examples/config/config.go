// Package config contains shared example configuration helpers.
//
// Required environment variables:
//
//	ZEROBUS_SERVER_ENDPOINT   Zerobus gRPC endpoint
//	DATABRICKS_WORKSPACE_URL  Unity Catalog / workspace URL (OAuth token exchange)
//	ZEROBUS_TABLE_NAME        target table, "catalog.schema.table"
//	DATABRICKS_CLIENT_ID      OAuth 2.0 client id
//	DATABRICKS_CLIENT_SECRET  OAuth 2.0 client secret
//
// Examples target an `orders` table; see the examples README.
package config

import (
	"fmt"
	"os"
)

// Settings is the resolved connection configuration for an example.
type Settings struct {
	ServerEndpoint string
	WorkspaceURL   string
	TableName      string
	ClientID       string
	ClientSecret   string
}

// RequireEnv returns a required environment variable or exits with status 2.
func RequireEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		fmt.Fprintf(os.Stderr,
			"error: environment variable %s is not set.\n"+
				"See the examples README for the required variables.\n", name)
		os.Exit(2)
	}
	return v
}

// Load reads the standard connection variables from the environment.
func Load() Settings {
	return Settings{
		ServerEndpoint: RequireEnv("ZEROBUS_SERVER_ENDPOINT"),
		WorkspaceURL:   RequireEnv("DATABRICKS_WORKSPACE_URL"),
		TableName:      RequireEnv("ZEROBUS_TABLE_NAME"),
		ClientID:       RequireEnv("DATABRICKS_CLIENT_ID"),
		ClientSecret:   RequireEnv("DATABRICKS_CLIENT_SECRET"),
	}
}
